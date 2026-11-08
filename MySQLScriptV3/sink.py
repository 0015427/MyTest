"""写入侧：NullSink（dry-run）/ SQLiteSink / MySQLSink（批量 INSERT）。"""
from __future__ import annotations

import sqlite3
from typing import Callable, List, Sequence, Tuple

from .ddl import ensure_if_not_exists
from .tables import Table

VerifyResult = Tuple[str, bool, str]


class PacketTooLargeError(Exception):
    """单条多行 INSERT 超过 max_allowed_packet，需要拆批重试。"""


def insert_sql(table: Table, n_rows: int, placeholder: str) -> str:
    columns = ", ".join(f"`{c}`" for c in table.columns)
    row_sql = "(" + ", ".join([placeholder] * len(table.columns)) + ")"
    values = ", ".join([row_sql] * n_rows)
    return f"INSERT INTO `{table.name}` ({columns}) VALUES {values}"


def _sqlite_type(sql_type: str) -> str:
    upper = sql_type.upper()
    if "INT" in upper:
        return "INTEGER"
    if any(k in upper for k in ("DECIMAL", "NUMERIC", "REAL", "DOUBLE", "FLOAT")):
        return "REAL"
    if any(k in upper for k in ("DATETIME", "TIMESTAMP", "DATE", "TIME")):
        return "TEXT"
    if "BLOB" in upper or "BINARY" in upper:
        return "BLOB"
    return "TEXT"


def sqlite_ddl(table: Table, by_name: dict[str, Table]) -> str:
    cols = []
    for column in table.info.columns:
        cols.append(f"`{column.name}` {_sqlite_type(column.sql_type)}")
    parts = ", ".join(cols)
    if table.primary_key:
        parts += f", PRIMARY KEY (`{table.primary_key}`)"
    if table.depends_on and table.fk_column:
        parent = by_name[table.depends_on]
        if parent.primary_key:
            parts += (
                f", FOREIGN KEY (`{table.fk_column}`) "
                f"REFERENCES `{parent.name}` (`{parent.primary_key}`)"
            )
    return f"CREATE TABLE IF NOT EXISTS `{table.name}` ({parts})"


def run_verifications(
    tables: Sequence[Table], root_rows: int, scalar: Callable[[str], object]
) -> List[VerifyResult]:
    results: List[VerifyResult] = []
    by_name = {t.name: t for t in tables}
    root = next(t for t in tables if t.depends_on is None)

    actual = scalar(f"SELECT COUNT(*) FROM `{root.name}`")
    results.append(
        (f"根表行数 {root.name}", actual == root_rows, f"期望 {root_rows}，实际 {actual}")
    )

    for table in tables:
        if table.depends_on is None or not table.fk_column:
            continue
        parent = by_name[table.depends_on]
        if not parent.primary_key:
            continue
        orphans = scalar(
            f"SELECT COUNT(*) FROM `{table.name}` c LEFT JOIN `{parent.name}` p "
            f"ON c.`{table.fk_column}` = p.`{parent.primary_key}` "
            f"WHERE p.`{parent.primary_key}` IS NULL"
        )
        results.append(
            (
                f"孤儿外键 {table.name}.{table.fk_column}",
                orphans == 0,
                f"孤儿行 {orphans}",
            )
        )
    return results


class Sink:
    """每个 worker 独占一个实例（独占一条连接）。"""

    def create_tables(self, tables: Sequence[Table]) -> None:
        raise NotImplementedError

    def wipe(self, tables: Sequence[Table]) -> None:
        raise NotImplementedError

    def write_batch(self, table: Table, rows: List[tuple]) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def verify(self, tables: Sequence[Table], root_rows: int) -> List[VerifyResult]:
        raise NotImplementedError


class NullSink(Sink):
    """dry-run：只走生成 + 批次切分，不落库。"""

    def write_batch(self, table: Table, rows: List[tuple]) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


class SQLiteSink(Sink):
    """本地验证用：真实外键约束 + 真实事务。"""

    def __init__(self, path: str, foreign_keys: bool = True, timeout: float = 120.0) -> None:
        self.conn = sqlite3.connect(path, timeout=timeout)
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'}")
        cur.close()
        self._sql_cache: dict[str, str] = {}

    def create_tables(self, tables: Sequence[Table]) -> None:
        by_name = {t.name: t for t in tables}
        cur = self.conn.cursor()
        for table in tables:  # 拓扑序：父表先建
            cur.execute(sqlite_ddl(table, by_name))
        self.conn.commit()
        cur.close()

    def wipe(self, tables: Sequence[Table]) -> None:
        cur = self.conn.cursor()
        for table in reversed(tables):
            cur.execute(f"DELETE FROM `{table.name}`")
        self.conn.commit()
        cur.close()

    def write_batch(self, table: Table, rows: List[tuple]) -> None:
        sql = self._sql_cache.get(table.name)
        if sql is None:
            sql = insert_sql(table, 1, "?")
            self._sql_cache[table.name] = sql
        self.conn.cursor().executemany(sql, rows)

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    def verify(self, tables: Sequence[Table], root_rows: int) -> List[VerifyResult]:
        def scalar(sql: str) -> object:
            row = self.conn.execute(sql).fetchone()
            return row[0] if row else None

        return run_verifications(tables, root_rows, scalar)


class MySQLSink(Sink):
    """多行 VALUES 批量写入；会话级参数按需调整。"""

    def __init__(self, config: dict, foreign_key_checks: bool = True) -> None:
        try:
            import pymysql
        except ImportError as exc:  # pragma: no cover - 环境相关
            raise RuntimeError(
                "未安装 pymysql，无法使用 MySQL 后端；请 `pip install pymysql`，"
                "或改用 SQLiteConfig / db=None（dry-run）验证。"
            ) from exc
        self._pymysql = pymysql
        self._config = dict(config)
        self.conn = pymysql.connect(
            host=config["host"],
            port=int(config["port"]),
            user=config["user"],
            password=config["password"],
            database=config["database"],
            charset=config.get("charset", "utf8mb4"),
            autocommit=False,
        )
        self._sql_cache: dict[Tuple[str, int], str] = {}
        cur = self.conn.cursor()
        cur.execute("SET SESSION unique_checks=1")
        cur.execute(f"SET SESSION foreign_key_checks={1 if foreign_key_checks else 0}")
        cur.execute("SET SESSION innodb_lock_wait_timeout=600")
        cur.execute("SET SESSION net_write_timeout=600")
        cur.execute("SET SESSION net_read_timeout=600")
        cur.close()

    def create_tables(self, tables: Sequence[Table]) -> None:
        cur = self.conn.cursor()
        for table in tables:  # 拓扑序：父表先建
            cur.execute(ensure_if_not_exists(table.ddl))
        self.conn.commit()
        cur.close()

    def wipe(self, tables: Sequence[Table]) -> None:
        cur = self.conn.cursor()
        cur.execute("SET SESSION foreign_key_checks=0")
        for table in reversed(tables):
            cur.execute(f"TRUNCATE TABLE `{table.name}`")
        cur.execute("SET SESSION foreign_key_checks=1")
        self.conn.commit()
        cur.close()

    def write_batch(self, table: Table, rows: List[tuple]) -> None:
        key = (table.name, len(rows))
        sql = self._sql_cache.get(key)
        if sql is None:
            sql = insert_sql(table, len(rows), "%s")
            self._sql_cache[key] = sql
        params = [value for row in rows for value in row]
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
        except self._pymysql.err.OperationalError as exc:
            code = exc.args[0] if exc.args else None
            if code == 1153:
                raise PacketTooLargeError(str(exc)) from exc
            raise
        finally:
            cur.close()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    def verify(self, tables: Sequence[Table], root_rows: int) -> List[VerifyResult]:
        check = MySQLSink(self._config)
        try:
            def scalar(sql: str) -> object:
                cur = check.conn.cursor()
                try:
                    cur.execute(sql)
                    row = cur.fetchone()
                    return row[0] if row else None
                finally:
                    cur.close()

            return run_verifications(tables, root_rows, scalar)
        finally:
            check.close()
