"""执行引擎：任务 = 根表的一段行区间 = 一个事务 = 一个并行单位 = 一个续跑单位。"""
from __future__ import annotations

import hashlib
import random
import sqlite3
import threading
import time
from concurrent.futures import FIRST_COMPLETED, CancelledError, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from .dsn import Dsn, parse_dsn
from .gen import DataError, TaskGenerator
from .progress import Progress
from .sink import Check, PacketTooLarge, Sink, SinkFactory, SinkPool
from .spec import Plan, Spec, SpecError, Table

__all__ = ["RunConfig", "RunResult", "run", "build_signature", "is_retryable"]

_RETRY_CODES = {1040, 1053, 1205, 1213, 2003, 2006, 2013, 2014, 4031}


class Stopped(Exception):
    """收到停止信号（别的任务失败）。"""


def is_retryable(exc: BaseException) -> bool:
    """连接类 / 锁类错误才重试；数据错误立刻失败。"""
    if isinstance(exc, (DataError, SpecError, Stopped)):
        return False
    if isinstance(exc, sqlite3.IntegrityError):
        return False
    if isinstance(exc, sqlite3.OperationalError):
        text = str(exc).lower()
        return "locked" in text or "busy" in text
    if isinstance(exc, ValueError):
        return False  # 规则/数据问题，重试没有意义
    name = type(exc).__name__
    if name in ("OperationalError", "InterfaceError", "InternalError"):
        code = exc.args[0] if getattr(exc, "args", None) else None
        if name == "InterfaceError" and (code is None or code == 0):
            return True  # pymysql 在连接已断时抛 InterfaceError(0, '')
        return code in _RETRY_CODES
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


@dataclass
class RunConfig:
    rows: Optional[int] = None
    task_rows: int = 10_000
    workers: int = 0  # 0 = min(4, CPU)：SQLite 写并发收益有限，MySQL 建议 4~8
    seed: Optional[str] = None
    retries: int = 3
    stop_on_error: bool = True
    check: str = "first"  # first / all / none
    create_tables: bool = True
    wipe: bool = False
    resume: bool = False
    verify: bool = False
    progress: bool = True
    packet_bytes: int = 4 * 1024 * 1024
    max_batch_rows: int = 5_000


@dataclass
class RunResult:
    ok: bool
    backend: str
    run_id: str
    rows_expected_root: int
    rows_written: int
    rows_by_table: Dict[str, int]
    batches_by_table: Dict[str, int]
    tasks_total: int
    tasks_done: int
    tasks_skipped: int
    elapsed: float
    retries: int
    splits: int
    failures: List[Tuple[int, str]] = field(default_factory=list)
    checks: List[Check] = field(default_factory=list)


def build_signature(spec: Spec, plan: Plan, target: Dsn, seed: str) -> str:
    parts = [
        f"version={spec.version}",
        f"rows={plan.root_rows}",
        f"task_rows={plan.task_rows}",
        f"seed={seed}",
        f"target={target.identity}",
    ]
    for table in plan.order:
        digest = hashlib.sha1(table.ddl.encode("utf-8")).hexdigest()[:10]
        parent = table.parent.name if table.parent is not None else "-"
        pp = table.per_parent
        if isinstance(pp, int) and not isinstance(pp, bool):
            pp_text = str(pp)
        elif isinstance(pp, (tuple, list)):
            pp_text = f"{pp[0]}-{pp[1]}"
        else:
            pp_text = "fn:" + getattr(pp, "__name__", "callable")
        parts.append(
            f"{table.name}<{parent}:{table.fk or '-'}>:{digest}:{pp_text}:"
            + ",".join(table.column_names)
        )
    return "|".join(parts)


def run_id_for(signature: str) -> str:
    return hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]


class _Metrics:
    def __init__(self, tables: Sequence[Table]) -> None:
        self.rows_by_table: Dict[str, int] = {t.name: 0 for t in tables}
        self.batches_by_table: Dict[str, int] = {t.name: 0 for t in tables}
        self.tasks_done = 0
        self.retries = 0
        self.splits = 0
        self.failures: List[Tuple[int, str]] = []
        self.task_seconds: List[float] = []
        self.elapsed = 0.0
        self._lock = threading.Lock()

    @property
    def rows(self) -> int:
        with self._lock:
            return sum(self.rows_by_table.values())

    def add_task(self, counts: Dict[str, int], batches: Dict[str, int], seconds: float) -> None:
        with self._lock:
            for name, value in counts.items():
                self.rows_by_table[name] += value
            for name, value in batches.items():
                self.batches_by_table[name] += value
            self.tasks_done += 1
            self.task_seconds.append(seconds)

    def add_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def add_split(self) -> None:
        with self._lock:
            self.splits += 1

    def add_failure(self, task: int, exc: BaseException) -> None:
        with self._lock:
            self.failures.append((task, f"{type(exc).__name__}: {exc}"))


class _Runner:
    def __init__(
        self,
        spec: Spec,
        plan: Plan,
        target: Dsn,
        cfg: RunConfig,
        seed: str,
        signature: str,
        run_id: str,
        progress_enabled: bool,
    ) -> None:
        self.spec = spec
        self.plan = plan
        self.target = target
        self.cfg = cfg
        self.seed = seed
        self.signature = signature
        self.run_id = run_id
        self.progress_enabled = progress_enabled
        self.factory = SinkFactory(target, cfg.packet_bytes)
        self.pool = SinkPool(self.factory)
        self.metrics = _Metrics(plan.order)
        self.stop = threading.Event()
        # 1153（包过大）时逐步收紧每批行数；None = 用计划里的值
        self.batch_cap: Optional[int] = None

    # -- 单任务 -----------------------------------------------------------
    def _write(self, sink: Sink, table: Table, rows: List[Tuple]) -> None:
        sink.write(table, rows)

    def _execute(self, sink: Sink, task: int, start: int, end: int) -> Tuple[Dict, Dict]:
        sink.begin()
        generator = TaskGenerator(
            self.spec,
            self.plan,
            task,
            start,
            end,
            seed=self.seed,
            check=self.cfg.check,
            max_batch_rows=self.batch_cap,
        )
        counts: Dict[str, int] = {}
        batches: Dict[str, int] = {}
        for table, rows in generator.iter_batches():
            if self.stop.is_set():
                raise Stopped()
            self._write(sink, table, rows)
            counts[table.name] = counts.get(table.name, 0) + len(rows)
            batches[table.name] = batches.get(table.name, 0) + 1
        for table in self.plan.order:
            expected = self.plan.expected_in_task(table, end - start)
            actual = counts.get(table.name, 0)
            if expected is not None and actual != expected:
                raise DataError(
                    f"任务 {task} 的 {table.name} 行数不对：期望 {expected}，实际 {actual}"
                )
        if self.progress_enabled:
            sink.mark_task(self.run_id, task, sum(counts.values()))
        sink.commit()
        return counts, batches

    def _work(self, task: int, start: int, end: int) -> Tuple[Dict, Dict, float]:
        attempt = 0
        split_attempts = 0
        while True:
            sink = self.pool.get()
            began = time.perf_counter()
            try:
                counts, batches = self._execute(sink, task, start, end)
                return counts, batches, time.perf_counter() - began
            except Stopped:
                raise
            except PacketTooLarge:
                # 1153 之后连接通常已经被服务端断开：丢弃连接，缩小批次，整任务重来
                try:
                    sink.rollback()
                except Exception:  # noqa: BLE001 - 连接已断
                    pass
                self.pool.discard(sink)
                current = self.batch_cap or max(self.plan.batch_rows.values(), default=1)
                self.batch_cap = max(1, current // 2)
                self.metrics.add_split()
                split_attempts += 1
                if split_attempts > 8 or self.stop.is_set():
                    raise
                time.sleep(0.05)
                continue
            except Exception as exc:  # noqa: BLE001 - 统一按任务重试
                rolled_back = True
                try:
                    sink.rollback()
                except Exception:  # noqa: BLE001 - 连接可能已断
                    rolled_back = False
                    self.pool.discard(sink)
                if (
                    attempt < self.cfg.retries
                    and not self.stop.is_set()
                    and is_retryable(exc)
                ):
                    attempt += 1
                    self.metrics.add_retry()
                    delay = min(10.0, 0.5 * (2 ** (attempt - 1)))
                    time.sleep(delay * (0.5 + random.random() / 2))
                    if not rolled_back:
                        self.pool.discard(sink)
                    continue
                raise

    # -- 主循环 -----------------------------------------------------------
    def execute(self, completed: set) -> RunResult:
        tasks = [
            (index, index * self.plan.task_rows, min((index + 1) * self.plan.task_rows, self.plan.root_rows))
            for index in range(self.plan.tasks)
        ]
        pending = [t for t in tasks if t[0] not in completed]
        if self.cfg.workers <= 0:
            import os

            workers = min(4, max(2, os.cpu_count() or 4))
        else:
            workers = self.cfg.workers
        workers = max(1, min(workers, max(1, len(pending))))

        progress = Progress(
            len(pending),
            self.plan.total_rows,
            enabled=self.cfg.progress and bool(pending),
        )
        started = time.perf_counter()
        progress.start()
        try:
            if pending:
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="df") as pool:
                    futures: Dict[Future, Tuple[int, int, int]] = {}
                    iterator = iter(pending)
                    exhausted = False
                    while futures or not exhausted:
                        while (
                            not exhausted
                            and not self.stop.is_set()
                            and len(futures) < workers * 2
                        ):
                            try:
                                item = next(iterator)
                            except StopIteration:
                                exhausted = True
                                break
                            futures[pool.submit(self._work, *item)] = item
                        if not futures:
                            break
                        done, _ = wait(
                            futures, timeout=0.2, return_when=FIRST_COMPLETED
                        )
                        for future in done:
                            item = futures.pop(future)
                            try:
                                counts, batches, seconds = future.result()
                            except CancelledError:
                                continue
                            except Stopped:
                                continue
                            except Exception as exc:  # noqa: BLE001
                                self.metrics.add_failure(item[0], exc)
                                if self.cfg.stop_on_error:
                                    self.stop.set()
                                    for other in futures:
                                        other.cancel()
                            else:
                                self.metrics.add_task(counts, batches, seconds)
                        progress.update(
                            self.metrics.tasks_done,
                            self.metrics.rows,
                            len(self.metrics.failures),
                        )
        finally:
            progress.finish()
            self.metrics.elapsed = time.perf_counter() - started
            self.pool.close_all()

        checks: List[Check] = []
        if self.cfg.verify and self.target.kind != "dry":
            sink = self.factory.open()
            try:
                checks = sink.verify(self.plan)
            finally:
                sink.close()

        failures = list(self.metrics.failures)
        ok = not failures and all(check.ok for check in checks)
        return RunResult(
            ok=ok,
            backend=self.target.kind,
            run_id=self.run_id,
            rows_expected_root=self.plan.root_rows,
            rows_written=self.metrics.rows,
            rows_by_table=dict(self.metrics.rows_by_table),
            batches_by_table=dict(self.metrics.batches_by_table),
            tasks_total=len(tasks),
            tasks_done=self.metrics.tasks_done,
            tasks_skipped=len(completed),
            elapsed=self.metrics.elapsed,
            retries=self.metrics.retries,
            splits=self.metrics.splits,
            failures=failures,
            checks=checks,
        )


def run(
    spec: Spec,
    dsn: Optional[str] = None,
    config: Optional[RunConfig] = None,
) -> RunResult:
    cfg = config or RunConfig()
    seed = cfg.seed or spec.seed
    plan = spec.plan(
        rows=cfg.rows,
        task_rows=cfg.task_rows,
        packet_bytes=cfg.packet_bytes,
        max_batch_rows=cfg.max_batch_rows,
    )
    if plan.errors:
        raise SpecError(plan.problems)
    if cfg.wipe and cfg.resume:
        raise ValueError("--wipe 和 --resume 不能同时使用（会清空数据又跳过任务）")
    if cfg.check not in ("first", "all", "none"):
        raise ValueError("check 只能是 first / all / none")

    target = parse_dsn(dsn)
    if cfg.resume and target.kind == "dry":
        raise ValueError("dry-run 没有数据库，无法续跑")

    signature = build_signature(spec, plan, target, seed)
    run_id = run_id_for(signature)
    progress_enabled = target.kind != "dry"

    bootstrap = SinkFactory(target, cfg.packet_bytes).open()
    completed: set = set()
    try:
        if cfg.create_tables:
            bootstrap.create_tables(plan.order)
        bootstrap.check_schema(plan.order)  # 在 wipe 之前：结构不对就别清数据
        if cfg.wipe:
            bootstrap.wipe(plan.order)
        if progress_enabled:
            try:
                bootstrap.ensure_progress()
            except Exception as exc:  # noqa: BLE001
                if cfg.resume:
                    raise RuntimeError(
                        f"无法创建进度表 _dataforge_runs/_dataforge_tasks，续跑不可用：{exc}"
                    ) from exc
                print(
                    f"[warn] 无法创建进度表，本次运行不支持续跑：{type(exc).__name__}: {exc}"
                )
                progress_enabled = False
        if progress_enabled:
            existing = bootstrap.get_run(run_id)
            if cfg.resume:
                if existing is None:
                    raise RuntimeError(
                        f"没有找到可续跑的记录（run_id={run_id}）。"
                        "表结构/行数/任务大小/种子/数据库 任一变化都会产生新的 run_id；"
                        "如果要重跑请加 --wipe 或去掉 --resume"
                    )
                completed = bootstrap.completed(run_id)
            else:
                if existing is not None and not cfg.wipe:
                    raise RuntimeError(
                        f"这个库已经有相同签名的运行记录（run_id={run_id}）。\n"
                        "  继续跑用 --resume；想重新生成用 --wipe；只是想验证用 verify"
                    )
                bootstrap.reset_run(run_id)
                bootstrap.insert_run(run_id, signature)
    finally:
        bootstrap.close()

    runner = _Runner(spec, plan, target, cfg, seed, signature, run_id, progress_enabled)
    return runner.execute(completed)
