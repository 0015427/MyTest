"""对外入口：`run(tables, rows, db, ...)`。

用户只写：建表语句 + 每张表的 generate_data + main 里的连接配置。
批量 INSERT、任务事务、线程池、断点续跑都在这里和 engine 内部完成。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from .engine import Engine, EngineConfig, Metrics
from .sink import MySQLSink, NullSink, SQLiteSink, VerifyResult
from .tables import Table, TableGraph, build_graph

log = logging.getLogger("MySQLScriptV3")


@dataclass(frozen=True)
class MySQLConfig:
    """MySQL 连接配置（`db=MySQLConfig(...)` 时启用 MySQL 后端）。"""

    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "123456"
    database: str = "test"
    charset: str = "utf8mb4"

    def as_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
        }


@dataclass(frozen=True)
class SQLiteConfig:
    """SQLite 配置：没有 MySQL 时用它验证生成与事务逻辑。"""

    path: str = "v3_demo.db"
    foreign_keys: bool = True


@dataclass
class VerifyItem:
    name: str
    ok: bool
    detail: str


@dataclass
class RunResult:
    ok: bool
    backend: str
    rows_expected: int
    rows_written: int
    rows_by_table: Dict[str, int]
    batches_by_table: Dict[str, int]
    elapsed: float
    peak_bytes: int
    tasks_total: int
    tasks_done: int
    retries: int
    splits: int
    failed_tasks: List[Tuple[int, str]] = field(default_factory=list)
    verifications: List[VerifyItem] = field(default_factory=list)


# ---------- 后端（框架内部） ----------


class _NullFactory:
    label = "dry-run"

    def bootstrap(self, graph: TableGraph, create_tables: bool, wipe: bool) -> None:
        pass

    def open(self) -> NullSink:
        return NullSink()

    def verify(self, graph: TableGraph, root_rows: int) -> List[VerifyResult]:
        return []


class _SQLiteFactory:
    label = "sqlite"

    def __init__(self, config: SQLiteConfig) -> None:
        self.config = config

    def bootstrap(self, graph: TableGraph, create_tables: bool, wipe: bool) -> None:
        sink = SQLiteSink(self.config.path, self.config.foreign_keys)
        try:
            if create_tables:
                sink.create_tables(graph.tables)
            if wipe:
                sink.wipe(graph.tables)
        finally:
            sink.close()

    def open(self) -> SQLiteSink:
        return SQLiteSink(self.config.path, self.config.foreign_keys)

    def verify(self, graph: TableGraph, root_rows: int) -> List[VerifyResult]:
        sink = SQLiteSink(self.config.path, self.config.foreign_keys)
        try:
            return sink.verify(graph.tables, root_rows)
        finally:
            sink.close()


class _MySQLFactory:
    label = "mysql"

    def __init__(self, config: MySQLConfig) -> None:
        try:
            import pymysql  # noqa: F401
        except ImportError as exc:  # pragma: no cover - 环境相关
            raise RuntimeError(
                "未安装 pymysql：请先 `pip install pymysql`，"
                "或改用 SQLiteConfig(...) / db=None 验证。"
            ) from exc
        self.config = config

    def bootstrap(self, graph: TableGraph, create_tables: bool, wipe: bool) -> None:
        sink = MySQLSink(self.config.as_dict())
        try:
            if create_tables:
                sink.create_tables(graph.tables)
            if wipe:
                sink.wipe(graph.tables)
        finally:
            sink.close()

    def open(self) -> MySQLSink:
        return MySQLSink(self.config.as_dict())

    def verify(self, graph: TableGraph, root_rows: int) -> List[VerifyResult]:
        sink = MySQLSink(self.config.as_dict())
        try:
            return sink.verify(graph.tables, root_rows)
        finally:
            sink.close()


def _make_factory(db) -> object:
    if db is None:
        return _NullFactory()
    if isinstance(db, SQLiteConfig):
        return _SQLiteFactory(db)
    if isinstance(db, MySQLConfig):
        return _MySQLFactory(db)
    raise TypeError(
        "db 只接受 None（dry-run）、SQLiteConfig(...) 或 MySQLConfig(...)，"
        f"收到 {type(db).__name__}"
    )


# ---------- 主流程 ----------


def run(
    tables: Sequence[Table],
    rows: int,
    db=None,
    *,
    task_rows: int = 20_000,
    batch_size: int = 5_000,
    workers: int = 4,
    seed: str = "v3",
    retries: int = 2,
    resume: bool = False,
    checkpoint: Optional[str] = ".v3_checkpoint.json",
    create_tables: bool = True,
    wipe: bool = False,
    verify: bool = False,
    memory_profile: bool = True,
    log_every: Optional[int] = None,
    quiet: bool = False,
):
    """按表生成数据并批量入库，返回 RunResult。

    参数：
      tables       Table 列表（顺序无所谓，框架按 depends_on 排拓扑序）
      rows         根表总行数
      db           None=dry-run（只跑生成与分批，不连库）；SQLiteConfig；MySQLConfig
      task_rows    一个任务的根表行数（= 一个事务 / 并行 / 续跑单位）
      batch_size   每批行数（表上可单独覆盖）
      workers      并发任务数（每 worker 一条连接）
      resume       读取 checkpoint，跳过已完成任务
      wipe         运行前清空目标表
      verify       运行后校验：根表行数 + 孤儿外键
    """
    if rows < 1:
        raise ValueError("rows 必须 >= 1")
    if task_rows < 1 or batch_size < 1 or workers < 1:
        raise ValueError("task_rows / batch_size / workers 必须 >= 1")

    graph = build_graph(tables)

    try:
        factory = _make_factory(db)
    except RuntimeError as exc:
        print(f"[错误] {exc}")
        return RunResult(
            ok=False,
            backend="mysql",
            rows_expected=rows,
            rows_written=0,
            rows_by_table={t.name: 0 for t in graph.tables},
            batches_by_table={t.name: 0 for t in graph.tables},
            elapsed=0.0,
            peak_bytes=0,
            tasks_total=0,
            tasks_done=0,
            retries=0,
            splits=0,
        )

    backend = factory.label
    total_tasks = (rows + task_rows - 1) // task_rows
    if log_every is None:
        log_every = max(1, total_tasks // 10) if total_tasks >= 5 else 0

    config = EngineConfig(
        task_rows=task_rows,
        batch_size=batch_size,
        workers=workers,
        seed=seed,
        retries=max(0, retries),
        checkpoint_path=None if backend == "dry-run" else checkpoint,
        resume=resume,
        log_every=log_every,
        memory_profile=memory_profile,
    )

    if not quiet:
        _print_intro(graph, rows, total_tasks, task_rows, workers, seed, backend)

    factory.bootstrap(graph, create_tables, wipe)

    started = time.perf_counter()
    metrics = Engine(graph, rows, config, factory).run()
    wall = time.perf_counter() - started
    if not quiet:
        _print_report(metrics, graph, rows, wall, backend)

    verifications: List[VerifyItem] = []
    if verify:
        if backend == "dry-run":
            if not quiet:
                print("\n[dry-run] 未连库，跳过一致性校验（改用 SQLiteConfig / MySQLConfig 才能校验）。")
        else:
            verifications = [
                VerifyItem(name, passed, detail)
                for name, passed, detail in factory.verify(graph, rows)
            ]
            if not quiet:
                _print_verifications(verifications)

    ok = not metrics.failed_tasks and all(item.ok for item in verifications)
    if not quiet:
        print(f"\n总耗时（含建表/校验）{wall:.2f}s，结论: {'通过' if ok else '失败'}")

    return RunResult(
        ok=ok,
        backend=backend,
        rows_expected=rows,
        rows_written=metrics.total_rows,
        rows_by_table=dict(metrics.rows_by_table),
        batches_by_table=dict(metrics.batches_by_table),
        elapsed=metrics.elapsed,
        peak_bytes=metrics.peak_bytes,
        tasks_total=total_tasks,
        tasks_done=metrics.tasks_done,
        retries=metrics.retries,
        splits=metrics.splits,
        failed_tasks=list(metrics.failed_tasks),
        verifications=verifications,
    )


# ---------- 输出 ----------


def _print_intro(graph, rows, total_tasks, task_rows, workers, seed, backend) -> None:
    print(f"后端 {backend}；根表 {graph.root.name} {rows:,} 行；"
          f"任务 {total_tasks} × {task_rows:,}；并发 {workers}；seed={seed}")
    print(f"{'表':<16}{'依赖':<16}{'外键列':<14}")
    for table in graph.tables:
        print(f"{table.name:<16}{table.depends_on or '-':<16}{table.fk_column or '-':<14}")


def _percentile(values: List[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * ratio))
    return ordered[index]


def _print_report(metrics: Metrics, graph: TableGraph, rows: int, wall: float, backend: str) -> None:
    elapsed = metrics.elapsed
    rate = metrics.total_rows / elapsed if elapsed > 0 else 0
    print("\n==== 运行报告 ====")
    print(f"{'后端':<16}: {backend}")
    print(f"{'根表行数':<16}: {rows:,}")
    print(f"{'本次写入行数':<16}: {metrics.total_rows:,}")
    print(f"{'端到端耗时':<16}: {elapsed:.2f}s（含建表/校验 {wall:.2f}s）")
    print(f"{'端到端吞吐':<16}: {rate:,.0f} 行/s")
    print(f"{'峰值 Python 堆':<16}: {metrics.peak_bytes / 1024 / 1024:.1f} MB")
    print(f"{'任务完成':<16}: {metrics.tasks_done}")
    print(f"{'任务耗时 p50':<16}: {_percentile(metrics.task_seconds, 0.50):.3f}s")
    print(f"{'任务耗时 p95':<16}: {_percentile(metrics.task_seconds, 0.95):.3f}s")
    print(f"{'重试 / 拆批':<16}: {metrics.retries} / {metrics.splits}")
    print(f"{'失败任务':<16}: {len(metrics.failed_tasks)}")

    print(f"\n{'表':<16}{'行数':>14}{'批次':>10}")
    for table in graph.tables:
        print(f"{table.name:<16}{metrics.rows_by_table[table.name]:>14,}"
              f"{metrics.batches_by_table[table.name]:>10,}")


def _print_verifications(items: List[VerifyItem]) -> None:
    print("\n[一致性校验]")
    for item in items:
        print(f"  {'PASS' if item.ok else 'FAIL'}  {item.name:<28}{item.detail}")
    print(f"[一致性校验] {'全部通过' if all(i.ok for i in items) else '存在失败项'}")
