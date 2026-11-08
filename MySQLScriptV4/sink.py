"""写入侧：SQLite / MySQL / dry-run，三种后端共用一套协议。

每个 worker 线程持有一个 Sink（一条连接），任务内所有批次同一事务。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .ddl import ensure_if_not_exists, sqlite_type
from .dsn import Dsn
from .spec import Plan, Table

__all__ = [
    "Check",
    "Sink",
    "SinkFactory",
    "SinkPool",
    "PacketTooLarge",
    "insert_sql",
    "RUNS_TABLE",
    "TASKS_TABLE",
]

RUNS_TABLE = "_dataforge_runs"
TASKS_TABLE = "_dataforge_tasks"

# 固定默认适配器，避免 Python 3.12+ 的 DeprecationWarning，也保证写入格式稳定
sqlite3.register_adapter(datetime, lambda value: value.isoformat(sep=" "))
sqlite3.register_adapter(date, lambda value: value.isoformat())
sqlite3.register_adapter(Decimal, lambda value: str(value))


class PacketTooLarge(Exception):
    """单条多行 INSERT 超过 max_allowed_packet。"""


class SchemaError(ValueError):
    """目标库里的表结构与 DDL 对不上（通常在 wipe 之前就抛，避免先清数据再失败）。"""


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def insert_sql(table: Table, n_rows: int, placeholder: str) -> str:
    columns = ", ".join(f"`{c}`" for c in table.column_names)
    row_sql = "(" + ", ".join([placeholder] * len(table.column_names)) + ")"
    values = ", ".join([row_sql] * n_rows)
    return f"INSERT INTO `{table.name}` ({columns}) VALUES {values}"


def _params_bytes(params: Sequence[Any]) -> int:
    """估算一条多值 INSERT 的报文大小（保守：非 ASCII 按 4 字节/字符、二进制按 2 倍）。"""
    total = 0
    for value in params:
        if isinstance(value, str):
            length = len(value)
            total += length if value.isascii() else length * 4 + 2
        elif isinstance(value, (bytes, bytearray)):
            total += len(value) * 2 + 4
        else:
            total += 16
    return total


_MYSQL_KIND = {
    "tinyint": "int", "smallint": "int", "mediumint": "int", "int": "int",
    "integer": "int", "bigint": "int", "bit": "int", "year": "int",
    "decimal": "decimal", "numeric": "decimal",
    "float": "float", "double": "float", "real": "float",
    "char": "text", "varchar": "text", "tinytext": "text", "text": "text",
    "mediumtext": "text", "longtext": "text",
    "date": "temporal", "datetime": "temporal", "timestamp": "temporal", "time": "temporal",
    "binary": "blob", "varbinary": "blob", "tinyblob": "blob", "blob": "blob",
    "mediumblob": "blob", "longblob": "blob",
    "json": "json", "enum": "enum", "set": "enum",
}


def _mysql_kind(data_type: str) -> str:
    return _MYSQL_KIND.get(data_type.lower(), "other")


class Sink:
    label = "sink"

    # -- DDL / 维护 -------------------------------------------------------
    def create_tables(self, tables: Sequence[Table]) -> None:
        raise NotImplementedError

    def wipe(self, tables: Sequence[Table]) -> None:
        raise NotImplementedError

    def check_schema(self, tables: Sequence[Table]) -> None:
        """对照 DDL 检查目标表；不一致抛 SchemaError。dry-run 不检查。"""
        pass

    # -- 事务 -------------------------------------------------------------
    def begin(self) -> None:
        raise NotImplementedError

    def write(self, table: Table, rows: List[Tuple[Any, ...]]) -> None:
        raise NotImplementedError

    def mark_task(self, run_id: str, task: int, rows: int) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    # -- 进度 -------------------------------------------------------------
    def ensure_progress(self) -> None:
        raise NotImplementedError

    def get_run(self, run_id: str) -> Optional[str]:
        raise NotImplementedError

    def insert_run(self, run_id: str, signature: str) -> None:
        raise NotImplementedError

    def reset_run(self, run_id: str) -> None:
        raise NotImplementedError

    def completed(self, run_id: str) -> set:
        raise NotImplementedError

    # -- 校验 -------------------------------------------------------------
    def verify(self, plan: Plan) -> List[Check]:
        raise NotImplementedError


class NullSink(Sink):
    label = "dry-run"

    def create_tables(self, tables: Sequence[Table]) -> None:
        pass

    def wipe(self, tables: Sequence[Table]) -> None:
        pass

    def begin(self) -> None:
        pass

    def write(self, table: Table, rows: List[Tuple[Any, ...]]) -> None:
        pass

    def mark_task(self, run_id: str, task: int, rows: int) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass

    def ensure_progress(self) -> None:
        pass

    def get_run(self, run_id: str) -> Optional[str]:
        return None

    def insert_run(self, run_id: str, signature: str) -> None:
        pass

    def reset_run(self, run_id: str) -> None:
        pass

    def completed(self, run_id: str) -> set:
        return set()

    def verify(self, plan: Plan) -> List[Check]:
        return []


def _sqlite_create(table: Table, by_name: Dict[str, Table]) -> str:
    parts: List[str] = []
    for column in table.info.insert_columns:  # 生成列不建（值由 MySQL 算，SQLite 代理不需要）
        piece = f"`{column.name}` {sqlite_type(column)}"
        if column.not_null:
            piece += " NOT NULL"
        if column.has_default and column.default_sql:
            piece += f" DEFAULT {column.default_sql}"
        parts.append(piece)
    if table.primary_key:
        parts.append(f"PRIMARY KEY (`{table.primary_key}`)")
    if table.parent is not None and table.fk:
        parent = by_name[table.parent.name]
        if parent.primary_key:
            parts.append(
                f"FOREIGN KEY (`{table.fk}`) REFERENCES `{parent.name}` (`{parent.primary_key}`)"
            )
    return f"CREATE TABLE IF NOT EXISTS `{table.name}` ({', '.join(parts)})"


class SQLiteSink(Sink):
    label = "sqlite"

    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(
            path, timeout=60.0, isolation_level=None, check_same_thread=False
        )
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass  # 别的连接正在写时可能拿不到锁；WAL 已经持久化即可
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self._sql: Dict[str, str] = {}

    def create_tables(self, tables: Sequence[Table]) -> None:
        by_name = {t.name: t for t in tables}
        for table in tables:
            self.conn.execute(_sqlite_create(table, by_name))
        self.conn.commit()

    def wipe(self, tables: Sequence[Table]) -> None:
        for table in reversed(tables):
            self.conn.execute(f"DELETE FROM `{table.name}`")
        self.conn.commit()

    def check_schema(self, tables: Sequence[Table]) -> None:
        problems: List[str] = []
        for table in tables:
            rows = self.conn.execute(f"PRAGMA table_info(`{table.name}`)").fetchall()
            if not rows:
                problems.append(f"表 `{table.name}` 不存在")
                continue
            names = {str(row[1]).lower() for row in rows}
            for column in table.info.insert_columns:
                if column.name.lower() not in names:
                    problems.append(f"`{table.name}`.`{column.name}` 在数据库里不存在")
        if problems:
            raise SchemaError(
                "目标表结构与 DDL 不一致（为避免清错数据，这里还没有执行 wipe）：\n  - "
                + "\n  - ".join(problems)
            )

    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def write(self, table: Table, rows: List[Tuple[Any, ...]]) -> None:
        sql = self._sql.get(table.name)
        if sql is None:
            sql = insert_sql(table, 1, "?")
            self._sql[table.name] = sql
        self.conn.executemany(sql, rows)

    def mark_task(self, run_id: str, task: int, rows: int) -> None:
        self.conn.execute(
            f"INSERT INTO {TASKS_TABLE} (run_id, task_id, row_count, finished_at) "
            "VALUES (?, ?, ?, ?)",
            (run_id, task, rows, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    def ensure_progress(self) -> None:
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {RUNS_TABLE} ("
            "run_id TEXT PRIMARY KEY, signature TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {TASKS_TABLE} ("
            "run_id TEXT NOT NULL, task_id INTEGER NOT NULL, row_count INTEGER NOT NULL, "
            "finished_at TEXT NOT NULL, PRIMARY KEY (run_id, task_id))"
        )

    def get_run(self, run_id: str) -> Optional[str]:
        row = self.conn.execute(
            f"SELECT signature FROM {RUNS_TABLE} WHERE run_id = ?", (run_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    def insert_run(self, run_id: str, signature: str) -> None:
        self.conn.execute(
            f"INSERT INTO {RUNS_TABLE} (run_id, signature, created_at) VALUES (?, ?, ?)",
            (run_id, signature, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

    def reset_run(self, run_id: str) -> None:
        self.conn.execute(f"DELETE FROM {TASKS_TABLE} WHERE run_id = ?", (run_id,))
        self.conn.execute(f"DELETE FROM {RUNS_TABLE} WHERE run_id = ?", (run_id,))

    def completed(self, run_id: str) -> set:
        rows = self.conn.execute(
            f"SELECT task_id FROM {TASKS_TABLE} WHERE run_id = ?", (run_id,)
        ).fetchall()
        return {int(r[0]) for r in rows}

    def verify(self, plan: Plan) -> List[Check]:
        checks: List[Check] = []
        for table in plan.order:
            expected = plan.rows_by_table.get(table.name)
            actual = self._scalar(f"SELECT COUNT(*) FROM `{table.name}`")
            if expected is None:
                checks.append(
                    Check(f"rows {table.name}", True, f"{actual:,} 行（动态 per_parent，无预估）")
                )
            else:
                checks.append(
                    Check(
                        f"rows {table.name}",
                        actual == expected,
                        f"期望 {expected:,}，实际 {actual:,}",
                    )
                )
            if table.parent is not None and table.fk and table.parent.primary_key:
                orphans = self._scalar(
                    f"SELECT COUNT(*) FROM `{table.name}` c LEFT JOIN `{table.parent.name}` p "
                    f"ON c.`{table.fk}` = p.`{table.parent.primary_key}` "
                    f"WHERE p.`{table.parent.primary_key}` IS NULL"
                )
                checks.append(
                    Check(f"fk {table.name}.{table.fk}", orphans == 0, f"孤儿 {orphans:,} 行")
                )
            if table.primary_key:
                duplicates = self._scalar(
                    f"SELECT COUNT(*) - COUNT(DISTINCT `{table.primary_key}`) FROM `{table.name}`"
                )
                checks.append(
                    Check(f"pk {table.name}.{table.primary_key}", duplicates == 0, f"重复 {duplicates:,} 行")
                )
        return checks

    def _scalar(self, sql: str):
        row = self.conn.execute(sql).fetchone()
        return row[0] if row else None


class MySQLSink(Sink):
    label = "mysql"

    def __init__(self, dsn: Dsn, max_statement_bytes: int = 4 * 1024 * 1024) -> None:
        try:
            import pymysql
        except ImportError as exc:  # pragma: no cover - 环境相关
            raise RuntimeError(
                "未安装 pymysql：pip install pymysql；"
                "或者先用 sqlite:demo.db / dry-run 验证脚本"
            ) from exc
        self._pymysql = pymysql
        self.conn = pymysql.connect(
            host=dsn.host,
            port=dsn.port,
            user=dsn.user,
            password=dsn.password,
            database=dsn.database,
            charset=dsn.charset,
            autocommit=False,
            connect_timeout=10,
            read_timeout=600,
            write_timeout=600,
        )
        self._sql_cache: Dict[Tuple[str, int], str] = {}
        self.max_statement_bytes = max_statement_bytes
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SET SESSION unique_checks=1, foreign_key_checks=1, "
                "innodb_lock_wait_timeout=600, net_read_timeout=600, net_write_timeout=600"
            )
        finally:
            cursor.close()

    # -- DDL --------------------------------------------------------------
    def create_tables(self, tables: Sequence[Table]) -> None:
        cursor = self.conn.cursor()
        try:
            for table in tables:  # 拓扑序：父表先建
                cursor.execute(ensure_if_not_exists(table.ddl))
        finally:
            cursor.close()
        self.conn.commit()

    def wipe(self, tables: Sequence[Table]) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SET SESSION foreign_key_checks=0")
            for table in reversed(tables):
                cursor.execute(f"TRUNCATE TABLE `{table.name}`")
            cursor.execute("SET SESSION foreign_key_checks=1")
        finally:
            cursor.close()
        self.conn.commit()

    def check_schema(self, tables: Sequence[Table]) -> None:
        problems: List[str] = []
        cursor = self.conn.cursor()
        try:
            for table in tables:
                cursor.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
                    "NUMERIC_PRECISION, NUMERIC_SCALE, EXTRA, IS_NULLABLE "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND LOWER(TABLE_NAME) = LOWER(%s) "
                    "ORDER BY ORDINAL_POSITION",
                    (table.name,),
                )
                rows = cursor.fetchall()
                if not rows:
                    problems.append(f"表 `{table.name}` 不存在")
                    continue
                db = {str(row[0]).lower(): row for row in rows}
                for column in table.info.columns:
                    row = db.get(column.name.lower())
                    if row is None:
                        problems.append(f"`{table.name}`.`{column.name}` 在数据库里不存在")
                        continue
                    db_kind = _mysql_kind(str(row[1]))
                    if (
                        db_kind != "other"
                        and db_kind != column.kind
                        and not (column.kind == "bool" and db_kind == "int")
                    ):
                        problems.append(
                            f"`{table.name}`.`{column.name}` 类型不一致："
                            f"DDL {column.sql_type}，数据库 {row[1]}"
                        )
                    if column.length and row[2] is not None and int(row[2]) < column.length:
                        problems.append(
                            f"`{table.name}`.`{column.name}` 数据库长度 {row[2]} < DDL {column.length}"
                        )
                    if (
                        column.kind == "decimal"
                        and column.precision is not None
                        and row[3] is not None
                    ):
                        if int(row[3]) < column.precision or int(row[4] or 0) < int(column.scale or 0):
                            problems.append(
                                f"`{table.name}`.`{column.name}` 精度不一致：DDL {column.sql_type}"
                            )
                    extra = str(row[5] or "").upper()
                    db_generated = "STORED GENERATED" in extra or "VIRTUAL GENERATED" in extra
                    if db_generated != bool(column.generated):
                        problems.append(
                            f"`{table.name}`.`{column.name}` 生成列状态不一致"
                            f"（DDL generated={column.generated}, 数据库 generated={db_generated}）"
                        )
        finally:
            cursor.close()
        if problems:
            raise SchemaError(
                "目标表结构与 DDL 不一致（为避免清错数据，这里还没有执行 wipe）：\n  - "
                + "\n  - ".join(problems)
            )

    # -- 事务 -------------------------------------------------------------
    def begin(self) -> None:
        self.conn.begin()

    def write(self, table: Table, rows: List[Tuple[Any, ...]]) -> None:
        key = (table.name, len(rows))
        sql = self._sql_cache.get(key)
        if sql is None:
            sql = insert_sql(table, len(rows), "%s")
            self._sql_cache[key] = sql
        params = [value for row in rows for value in row]
        # 发送前先估算包大小：等服务端回 1153 时连接通常已经被断开，客户端再拆批就晚了
        estimated = _params_bytes(params)
        if estimated > self.max_statement_bytes:
            raise PacketTooLarge(
                f"{table.name} 这一批预计 {estimated:,} 字节，超过上限 {self.max_statement_bytes:,}"
            )
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
        except self._pymysql.err.OperationalError as exc:
            code = exc.args[0] if exc.args else None
            if code == 1153:
                raise PacketTooLarge(str(exc)) from exc
            raise
        finally:
            cursor.close()

    def mark_task(self, run_id: str, task: int, rows: int) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {TASKS_TABLE} (run_id, task_id, row_count, finished_at) "
                "VALUES (%s, %s, %s, %s)",
                (run_id, task, rows, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
        finally:
            cursor.close()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        try:
            self.conn.rollback()
        except Exception:  # noqa: BLE001 - 连接可能已断
            pass

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass

    # -- 进度 -------------------------------------------------------------
    def ensure_progress(self) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {RUNS_TABLE} ("
                "run_id VARCHAR(32) NOT NULL, signature TEXT NOT NULL, "
                "created_at DATETIME NOT NULL, PRIMARY KEY (run_id)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {TASKS_TABLE} ("
                "run_id VARCHAR(32) NOT NULL, task_id INT NOT NULL, row_count BIGINT NOT NULL, "
                "finished_at DATETIME NOT NULL, PRIMARY KEY (run_id, task_id)) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
        finally:
            cursor.close()
        self.conn.commit()

    def get_run(self, run_id: str) -> Optional[str]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT signature FROM {RUNS_TABLE} WHERE run_id = %s", (run_id,))
            row = cursor.fetchone()
            return None if row is None else str(row[0])
        finally:
            cursor.close()

    def insert_run(self, run_id: str, signature: str) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {RUNS_TABLE} (run_id, signature, created_at) VALUES (%s, %s, %s)",
                (run_id, signature, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
        finally:
            cursor.close()
        self.conn.commit()

    def reset_run(self, run_id: str) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DELETE FROM {TASKS_TABLE} WHERE run_id = %s", (run_id,))
            cursor.execute(f"DELETE FROM {RUNS_TABLE} WHERE run_id = %s", (run_id,))
        finally:
            cursor.close()
        self.conn.commit()

    def completed(self, run_id: str) -> set:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"SELECT task_id FROM {TASKS_TABLE} WHERE run_id = %s", (run_id,)
            )
            return {int(r[0]) for r in cursor.fetchall()}
        finally:
            cursor.close()

    # -- 校验 -------------------------------------------------------------
    def verify(self, plan: Plan) -> List[Check]:
        checks: List[Check] = []
        for table in plan.order:
            expected = plan.rows_by_table.get(table.name)
            actual = self._scalar(f"SELECT COUNT(*) FROM `{table.name}`")
            if expected is None:
                checks.append(
                    Check(f"rows {table.name}", True, f"{actual:,} 行（动态 per_parent，无预估）")
                )
            else:
                checks.append(
                    Check(
                        f"rows {table.name}",
                        actual == expected,
                        f"期望 {expected:,}，实际 {actual:,}",
                    )
                )
            if table.parent is not None and table.fk and table.parent.primary_key:
                orphans = self._scalar(
                    f"SELECT COUNT(*) FROM `{table.name}` c LEFT JOIN `{table.parent.name}` p "
                    f"ON c.`{table.fk}` = p.`{table.parent.primary_key}` "
                    f"WHERE p.`{table.parent.primary_key}` IS NULL"
                )
                checks.append(
                    Check(f"fk {table.name}.{table.fk}", orphans == 0, f"孤儿 {orphans:,} 行")
                )
            if table.primary_key:
                duplicates = self._scalar(
                    f"SELECT COUNT(*) - COUNT(DISTINCT `{table.primary_key}`) FROM `{table.name}`"
                )
                checks.append(
                    Check(f"pk {table.name}.{table.primary_key}", duplicates == 0, f"重复 {duplicates:,} 行")
                )
        return checks

    def _scalar(self, sql: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


class SinkFactory:
    def __init__(self, dsn: Dsn, max_statement_bytes: int = 4 * 1024 * 1024) -> None:
        self.dsn = dsn
        self.max_statement_bytes = max_statement_bytes

    @property
    def label(self) -> str:
        return self.dsn.label

    def open(self) -> Sink:
        if self.dsn.kind == "sqlite":
            return SQLiteSink(self.dsn.path)
        if self.dsn.kind == "mysql":
            return MySQLSink(self.dsn, self.max_statement_bytes)
        return NullSink()


class SinkPool:
    """每个线程一条连接，任务内复用；出错时丢弃重建。"""

    def __init__(self, factory: SinkFactory) -> None:
        import threading

        self.factory = factory
        self._local = threading.local()
        self._all: List[Sink] = []
        self._lock = threading.Lock()

    def get(self) -> Sink:
        sink = getattr(self._local, "sink", None)
        if sink is None:
            sink = self.factory.open()
            self._local.sink = sink
            with self._lock:
                self._all.append(sink)
        return sink

    def discard(self, sink: Sink) -> None:
        if getattr(self._local, "sink", None) is sink:
            self._local.sink = None
        with self._lock:
            if sink in self._all:
                self._all.remove(sink)
        try:
            sink.close()
        except Exception:  # noqa: BLE001
            pass

    def close_all(self) -> None:
        with self._lock:
            sinks = list(self._all)
            self._all = []
        for sink in sinks:
            try:
                sink.close()
            except Exception:  # noqa: BLE001
                pass
