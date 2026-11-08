import unittest

from MySQLScriptV4 import Table
from MySQLScriptV4.dsn import DsnError, parse_dsn
from MySQLScriptV4.sink import insert_sql
from MySQLScriptV4.spec import estimate_row_bytes

DDL = (
    "CREATE TABLE t (id VARCHAR(16) NOT NULL, n INT NOT NULL, "
    "amount DECIMAL(10,2) NOT NULL, PRIMARY KEY (id))"
)


class SinkTest(unittest.TestCase):
    def test_multi_row_insert(self):
        table = Table("t", DDL, rows=10, fields={
            "id": lambda r: f"x{r.i}", "n": lambda r: 1, "amount": lambda r: 1.5})
        sql = insert_sql(table, 2, "%s")
        self.assertTrue(sql.startswith("INSERT INTO `t` (`id`, `n`, `amount`) VALUES "))
        self.assertEqual(sql.count("(%s, %s, %s)"), 2)

    def test_single_row_insert(self):
        table = Table("t", DDL, rows=10, fields={
            "id": lambda r: f"x{r.i}", "n": lambda r: 1, "amount": lambda r: 1.5})
        self.assertEqual(
            insert_sql(table, 1, "?"),
            "INSERT INTO `t` (`id`, `n`, `amount`) VALUES (?, ?, ?)",
        )

    def test_estimate_row_bytes(self):
        table = Table("t", DDL, rows=10, fields={
            "id": lambda r: f"x{r.i}", "n": lambda r: 1, "amount": lambda r: 1.5})
        self.assertGreater(estimate_row_bytes(table), 8)


class DsnTest(unittest.TestCase):
    def test_dry(self):
        self.assertEqual(parse_dsn(None).kind, "dry")
        self.assertEqual(parse_dsn("dry-run").kind, "dry")

    def test_sqlite_forms(self):
        self.assertEqual(parse_dsn("sqlite:demo.db").path, "demo.db")
        self.assertEqual(parse_dsn("plain.db").kind, "sqlite")
        self.assertEqual(parse_dsn("sqlite:///C:/tmp/x.db").path, "C:/tmp/x.db")

    def test_memory_rejected(self):
        with self.assertRaises(DsnError):
            parse_dsn("sqlite::memory:")

    def test_mysql(self):
        dsn = parse_dsn("mysql://root:p%40ss@db.local:3307/demo?charset=utf8mb4")
        self.assertEqual(
            (dsn.host, dsn.port, dsn.user, dsn.password, dsn.database, dsn.charset),
            ("db.local", 3307, "root", "p@ss", "demo", "utf8mb4"),
        )
        self.assertEqual(dsn.identity, "mysql:db.local:3307/demo")

    def test_mysql_missing_db(self):
        with self.assertRaises(DsnError):
            parse_dsn("mysql://root:pw@localhost:3306")


if __name__ == "__main__":
    unittest.main()
