"""调度侧（框架内部）：任务 = 根表的一段行区间。

一个任务内按“父表 → 子表”的顺序流式生成并写入，最后一次提交；
任务同时是并行单元和断点续跑单元。用户不需要了解这些细节。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import tracemalloc
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from .sink import PacketTooLargeError, Sink
from .tables import GenContext, Table, TableGraph, batched

log = logging.getLogger("MySQLScriptV3")


@dataclass
class EngineConfig:
    task_rows: int = 20_000
    batch_size: int = 5_000
    workers: int = 4
    seed: str = "v3"
    retries: int = 2
    fail_fast: bool = False
    checkpoint_path: Optional[str] = None
    resume: bool = False
    log_every: int = 0
    memory_profile: bool = True


def graph_signature(graph: TableGraph, rows: int, config: EngineConfig) -> str:
    parts = [f"rows={rows}", f"task_rows={config.task_rows}", f"seed={config.seed}"]
    for table in graph.tables:
        digest = hashlib.sha256(table.ddl.encode("utf-8")).hexdigest()[:12]
        parts.append(f"{table.name}<{table.depends_on}:{table.fk_column}>:{digest}")
    return "|".join(parts)


class Metrics:
    def __init__(self, graph: TableGraph) -> None:
        self.rows_by_table: Dict[str, int] = {t.name: 0 for t in graph.tables}
        self.batches_by_table: Dict[str, int] = {t.name: 0 for t in graph.tables}
        self.tasks_done = 0
        self.retries = 0
        self.splits = 0
        self.failed_tasks: List[Tuple[int, str]] = []
        self.task_seconds: List[float] = []
        self.elapsed = 0.0
        self.peak_bytes = 0
        self._lock = threading.Lock()

    @property
    def total_rows(self) -> int:
        return sum(self.rows_by_table.values())

    def add_table(self, name: str, rows: int, batches: int) -> None:
        with self._lock:
            self.rows_by_table[name] += rows
            self.batches_by_table[name] += batches

    def add_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def add_split(self) -> None:
        with self._lock:
            self.splits += 1

    def add_failure(self, task: int, error: str) -> None:
        with self._lock:
            self.failed_tasks.append((task, error))

    def add_task_done(self, seconds: float) -> int:
        with self._lock:
            self.tasks_done += 1
            self.task_seconds.append(seconds)
            return self.tasks_done


class Checkpoint:
    """任务级断点：每个任务提交成功后原子落盘。"""

    def __init__(self, path: Optional[str], signature: str) -> None:
        self.path = path
        self.signature = signature
        self.completed: set[int] = set()
        self._lock = threading.Lock()

    def load(self) -> set[int]:
        if not self.path or not os.path.exists(self.path):
            return set()
        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("signature") != self.signature:
            raise RuntimeError(
                "checkpoint 与本次运行参数不匹配（表结构、行数、任务大小、种子之一变了）："
                "请换一个 --checkpoint 路径，或去掉 --resume / 使用 --no-checkpoint。"
            )
        self.completed = set(data.get("completed", []))
        return set(self.completed)

    def mark(self, task: int) -> None:
        with self._lock:
            if task in self.completed:
                return
            self.completed.add(task)
            if not self.path:
                return
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {"signature": self.signature, "completed": sorted(self.completed)}, f
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)


class Engine:
    """任务 = 根表行区间 [start, end)：区间内的父表与所有子表在同一个事务里写完。"""

    def __init__(self, graph: TableGraph, rows: int, config: EngineConfig, factory) -> None:
        self.graph = graph
        self.rows = rows
        self.config = config
        self.factory = factory
        self.metrics = Metrics(graph)
        self.checkpoint = Checkpoint(
            config.checkpoint_path, graph_signature(graph, rows, config)
        )
        self._started = 0.0
        self._task_total = 0

    def tasks(self) -> Iterator[Tuple[int, int, int]]:
        for index, start in enumerate(range(0, self.rows, self.config.task_rows)):
            yield index, start, min(start + self.config.task_rows, self.rows)

    def run(self) -> Metrics:
        tasks = list(self.tasks())
        self._task_total = len(tasks)
        completed = self.checkpoint.load() if self.config.resume else set()
        pending = [task for task in tasks if task[0] not in completed]
        if completed:
            log.info(
                "断点续跑：已完成 %s 个任务，本次待跑 %s 个", len(completed), len(pending)
            )

        self._started = time.perf_counter()
        if self.config.memory_profile:
            tracemalloc.start()

        with ThreadPoolExecutor(
            max_workers=self.config.workers, thread_name_prefix="task"
        ) as executor:
            futures: Dict[Future, int] = {
                executor.submit(self._run_task, index, start, end): index
                for index, start, end in pending
            }
            for future in as_completed(futures):
                task = futures[future]
                error = future.exception()
                if error is not None:
                    self.metrics.add_failure(task, repr(error))
                    log.error("任务 %s 失败: %s", task, error)
                    if self.config.fail_fast:
                        for other in futures:
                            other.cancel()

        self.metrics.elapsed = time.perf_counter() - self._started
        if self.config.memory_profile:
            self.metrics.peak_bytes = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        return self.metrics

    # ---------- 单个任务 ----------

    def _run_task(self, task: int, start: int, end: int) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(self.config.retries + 1):
            sink: Optional[Sink] = None
            began = time.perf_counter()
            try:
                sink = self.factory.open()
                rows_by_table, batches_by_table = self._stream_task(sink, task, start, end)
                sink.commit()
                for name in rows_by_table:
                    self.metrics.add_table(name, rows_by_table[name], batches_by_table[name])
                done = self.metrics.add_task_done(time.perf_counter() - began)
                self.checkpoint.mark(task)
                if self.config.log_every and done % self.config.log_every == 0:
                    elapsed = time.perf_counter() - self._started
                    rate = self.metrics.total_rows / elapsed if elapsed > 0 else 0
                    log.info(
                        "进度 %s/%s 任务，累计 %s 行，%.1fs，%.0f 行/s",
                        done,
                        self._task_total,
                        self.metrics.total_rows,
                        elapsed,
                        rate,
                    )
                return
            except Exception as exc:  # noqa: BLE001 - 统一按任务重试
                last_error = exc
                if sink is not None:
                    try:
                        sink.rollback()
                    except Exception:  # noqa: BLE001 - 连接可能已断
                        pass
                if attempt < self.config.retries and not self.config.fail_fast:
                    self.metrics.add_retry()
                    delay = 0.5 * (2 ** attempt)
                    log.warning(
                        "任务 %s 第 %s 次失败，%.1fs 后重试: %s", task, attempt + 1, delay, exc
                    )
                    time.sleep(delay)
                else:
                    break
            finally:
                if sink is not None:
                    try:
                        sink.close()
                    except Exception:  # noqa: BLE001
                        pass

        raise RuntimeError(
            f"任务 {task}（根表行 {start}~{end}）在 {self.config.retries + 1} 次尝试后仍失败: "
            f"{last_error}"
        )

    def _stream_task(
        self, sink: Sink, task: int, start: int, end: int
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        rows_by_table: Dict[str, int] = {t.name: 0 for t in self.graph.tables}
        batches_by_table: Dict[str, int] = {t.name: 0 for t in self.graph.tables}

        root = self.graph.root
        ctx = GenContext(root.name, start, end, self.config.seed, task)
        produced = 0
        for batch in batched(self._rows(root, ctx), self._batch_of(root)):
            self._check_width(root, batch)
            self._write_resilient(sink, root, batch)
            produced += len(batch)
            rows_by_table[root.name] += len(batch)
            batches_by_table[root.name] += 1
            for child in self.graph.child_tables(root.name):
                self._stream_child(sink, child, batch, task, start, end, rows_by_table, batches_by_table)

        if produced != end - start:
            raise RuntimeError(
                f"{root.name}.generate_data 产出了 {produced} 行，但本任务要求 "
                f"ctx.start..ctx.end 正好 {end - start} 行（{start}..{end}）。"
                "根表生成器必须严格按 ctx.start / ctx.end 产出。"
            )
        return rows_by_table, batches_by_table

    def _stream_child(
        self,
        sink: Sink,
        table: Table,
        parent_rows: List[tuple],
        task: int,
        start: int,
        end: int,
        rows_by_table: Dict[str, int],
        batches_by_table: Dict[str, int],
    ) -> None:
        """子表拿到父表**刚写入的这一批行**作为入参，因此外键天然落在同一任务内。"""
        ctx = GenContext(table.name, start, end, self.config.seed, task)
        for batch in batched(self._rows(table, ctx, parent_rows), self._batch_of(table)):
            self._check_width(table, batch)
            self._write_resilient(sink, table, batch)
            rows_by_table[table.name] += len(batch)
            batches_by_table[table.name] += 1
            for grandchild in self.graph.child_tables(table.name):
                self._stream_child(
                    sink, grandchild, batch, task, start, end, rows_by_table, batches_by_table
                )

    def _rows(self, table: Table, ctx: GenContext, parent_rows: Optional[List[tuple]] = None):
        rows = table.generate_data(ctx) if table.depends_on is None else table.generate_data(ctx, parent_rows)
        if not hasattr(rows, "__iter__"):
            raise TypeError(
                f"{table.name}.generate_data 必须用 yield 返回可迭代的行，实际返回了 "
                f"{type(rows).__name__}。"
            )
        return rows

    def _batch_of(self, table: Table) -> int:
        return table.batch_size or self.config.batch_size

    @staticmethod
    def _check_width(table: Table, batch: Sequence[tuple]) -> None:
        width = len(table.columns)
        for row in batch:
            if not isinstance(row, (tuple, list)) or len(row) != width:
                raise ValueError(
                    f"{table.name} 产出的行不是 {width} 列：{row!r}\n"
                    f"请按建表语句的列顺序产出：{table.columns}"
                )

    def _write_resilient(self, sink: Sink, table: Table, rows: List[tuple], depth: int = 0) -> None:
        try:
            sink.write_batch(table, rows)
        except PacketTooLargeError:
            if depth >= 8 or len(rows) <= 1:
                raise
            self.metrics.add_split()
            mid = len(rows) // 2
            log.warning(
                "单条 INSERT 超过 max_allowed_packet，拆批重试：%s %s 行 -> %s + %s",
                table.name,
                len(rows),
                mid,
                len(rows) - mid,
            )
            self._write_resilient(sink, table, rows[:mid], depth + 1)
            self._write_resilient(sink, table, rows[mid:], depth + 1)
