import contextlib
import hashlib
import io
import os
import shutil
import sqlite3
import unittest
import uuid

from MySQLScriptV4 import Table, between, money
from MySQLScriptV4.engine import RunConfig, run
from MySQLScriptV4.sink import SchemaError
from MySQLScriptV4.spec import Spec

USERS_DDL = (
    "CREATE TABLE users (id VARCHAR(32) NOT NULL, name VARCHAR(32) NOT NULL, "
    "PRIMARY KEY (id))"
)
ORDERS_DDL = (
    "CREATE TABLE orders (id VARCHAR(48) NOT NULL, user_id VARCHAR(32) NOT NULL, "
    "amount DECIMAL(10,2) NOT NULL, PRIMARY KEY (id))"
)
ITEMS_DDL = (
    "CREATE TABLE order_items (id VARCHAR(64) NOT NULL, order_id VARCHAR(48) NOT NULL, "
    "qty INT NOT NULL, PRIMARY KEY (id))"
)
QUERIES = {
    "users": "select id, name from users order by id",
    "orders": "select id, user_id, amount from orders order by id",
    "order_items": "select id, order_id, qty from order_items order by id",
}


def make_spec(seed="test"):
    users = Table(
        "users",
        USERS_DDL,
        rows=300,
        fields={"id": lambda r: f"u{r.i}", "name": lambda r: f"n{r.i}"},
    )
    orders = Table(
        "orders",
        ORDERS_DDL,
        parent=users,
        fk="user_id",
        per_parent=2,
        fields={
            "id": lambda r: f"{r.parent['id']}-o{r.k}",
            "amount": money(1, 10),
        },
    )
    items = Table(
        "order_items",
        ITEMS_DDL,
        parent=orders,
        fk="order_id",
        per_parent=3,
        fields={
            "id": lambda r: f"{r.parent['id']}-i{r.k}",
            "qty": between(1, 5),
        },
    )
    return Spec([users, orders, items], seed=seed, title="test")


def digest(path):
    conn = sqlite3.connect(path)
    try:
        data = [conn.execute(q).fetchall() for q in QUERIES.values()]
    finally:
        conn.close()
    return hashlib.sha256(repr(data).encode()).hexdigest()


class EngineTest(unittest.TestCase):
    def setUp(self):
        # 用 os.makedirs（默认 0777，继承 ACL）而不是 mkdtemp（0700）：
        # 某些 Windows 沙箱下 0700 目录里的 SQLite 文件会打不开
        self.dir = os.path.join(os.getcwd(), ".df_test_" + uuid.uuid4().hex[:8])
        os.makedirs(self.dir)
        self.spec = make_spec()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def path(self, name):
        return os.path.join(self.dir, name + ".db")

    def dsn(self, name):
        return "sqlite:" + self.path(name)

    def config(self, **kwargs):
        base = dict(rows=300, task_rows=100, progress=False, wipe=True)
        base.update(kwargs)
        return RunConfig(**base)

    def test_run_and_verify(self):
        result = run(self.spec, self.dsn("a"), self.config(verify=True))
        self.assertTrue(result.ok, result.failures)
        self.assertEqual(result.rows_by_table, {"users": 300, "orders": 600, "order_items": 1800})
        self.assertEqual(result.tasks_done, 3)
        self.assertTrue(all(c.ok for c in result.checks), result.checks)

    def test_deterministic_across_workers_and_batches(self):
        a, b, c = self.path("a"), self.path("b"), self.path("c")
        run(self.spec, "sqlite:" + a, self.config(workers=1))
        run(self.spec, "sqlite:" + b, self.config(workers=4))
        run(
            self.spec,
            "sqlite:" + c,
            self.config(workers=2, max_batch_rows=7, packet_bytes=3000),
        )
        self.assertEqual(digest(a), digest(b))
        self.assertEqual(digest(a), digest(c))

    def test_resume_skips_completed(self):
        run(self.spec, self.dsn("a"), self.config())
        result = run(self.spec, self.dsn("a"), self.config(resume=True, wipe=False, verify=True))
        self.assertTrue(result.ok)
        self.assertEqual(result.tasks_done, 0)
        self.assertEqual(result.tasks_skipped, 3)
        self.assertTrue(all(c.ok for c in result.checks))

    def test_wipe_and_resume_rejected(self):
        with self.assertRaises(ValueError):
            run(self.spec, self.dsn("a"), self.config(resume=True, wipe=True))

    def test_rerun_without_resume_rejected(self):
        run(self.spec, self.dsn("a"), self.config())
        with self.assertRaises(RuntimeError):
            run(self.spec, self.dsn("a"), self.config(wipe=False))

    def test_resume_into_other_database_rejected(self):
        run(self.spec, self.dsn("a"), self.config())
        with self.assertRaises(RuntimeError):
            run(self.spec, self.dsn("b"), self.config(resume=True, wipe=False))

    def test_partial_resume(self):
        path = self.path("a")
        run(self.spec, "sqlite:" + path, self.config())
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "delete from order_items where order_id in "
            "(select id from orders where cast(substr(user_id, 2) as integer) >= 200)"
        )
        conn.execute("delete from orders where cast(substr(user_id, 2) as integer) >= 200")
        conn.execute("delete from users where cast(substr(id, 2) as integer) >= 200")
        conn.execute("delete from _dataforge_tasks where task_id = 2")
        conn.commit()
        conn.close()

        result = run(self.spec, "sqlite:" + path, self.config(resume=True, wipe=False, verify=True))
        self.assertTrue(result.ok, result.failures)
        self.assertEqual(result.tasks_done, 1)
        self.assertEqual(result.tasks_skipped, 2)
        self.assertTrue(all(c.ok for c in result.checks), result.checks)

    def test_orphan_detected(self):
        path = self.path("a")
        run(self.spec, "sqlite:" + path, self.config())
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("insert into orders values ('bogus', 'u99999', 1.0)")
        conn.commit()
        conn.close()
        result = run(
            self.spec,
            "sqlite:" + path,
            self.config(resume=True, wipe=False, verify=True),
        )
        self.assertFalse(result.ok)
        failed = [c.name for c in result.checks if not c.ok]
        self.assertTrue(any(name.startswith("fk orders") for name in failed), failed)

    def test_dry_run(self):
        result = run(self.spec, None, self.config(rows=100))
        self.assertTrue(result.ok)
        self.assertEqual(result.backend, "dry")
        self.assertEqual(result.rows_written, 900)
        self.assertEqual(result.checks, [])

    def test_check_all_mode(self):
        result = run(self.spec, self.dsn("a"), self.config(check="all"))
        self.assertTrue(result.ok)

    def test_progress_table_failure_still_runs(self):
        """没有建进度表权限时：降级为不可续跑，但数据照常写入。"""
        from MySQLScriptV4 import sink as sink_module

        original = sink_module.SQLiteSink.ensure_progress

        def boom(self):
            raise sqlite3.OperationalError("progress table denied")

        sink_module.SQLiteSink.ensure_progress = boom
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                result = run(self.spec, self.dsn("a"), self.config(verify=True))
        finally:
            sink_module.SQLiteSink.ensure_progress = original
        self.assertTrue(result.ok, result.failures)
        self.assertEqual(result.rows_written, 2700)
        conn = sqlite3.connect(self.path("a"))
        try:
            names = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type='table'"
                )
            }
        finally:
            conn.close()
        self.assertNotIn("_dataforge_tasks", names)

    def test_progress_table_failure_blocks_resume(self):
        from MySQLScriptV4 import sink as sink_module

        original = sink_module.SQLiteSink.ensure_progress

        def boom(self):
            raise sqlite3.OperationalError("progress table denied")

        sink_module.SQLiteSink.ensure_progress = boom
        try:
            with self.assertRaises(RuntimeError):
                run(
                    self.spec,
                    self.dsn("a"),
                    self.config(resume=True, wipe=False),
                )
        finally:
            sink_module.SQLiteSink.ensure_progress = original

    def test_schema_mismatch_blocks_wipe(self):
        path = self.path("a")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO users VALUES ('keep-me')")
        conn.commit()
        conn.close()

        with self.assertRaises(SchemaError):
            run(self.spec, "sqlite:" + path, self.config(wipe=True))

        conn = sqlite3.connect(path)
        try:
            rows = conn.execute("select id from users").fetchall()
        finally:
            conn.close()
        self.assertEqual(rows, [("keep-me",)])


if __name__ == "__main__":
    unittest.main()
