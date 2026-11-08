import unittest

from MySQLScriptV4 import SKIP, Table, between, money, pick
from MySQLScriptV4.spec import Spec, SpecError

USERS = (
    "CREATE TABLE users (id VARCHAR(32) NOT NULL, name VARCHAR(10) NOT NULL, "
    "country VARCHAR(2) NOT NULL, PRIMARY KEY (id))"
)
ORDERS = (
    "CREATE TABLE orders (id VARCHAR(40) NOT NULL, user_id VARCHAR(32) NOT NULL, "
    "amount DECIMAL(10,2) NOT NULL, PRIMARY KEY (id))"
)
ORDERS_SHORT_FK = (
    "CREATE TABLE orders (id VARCHAR(40) NOT NULL, user_id VARCHAR(4) NOT NULL, "
    "amount DECIMAL(10,2) NOT NULL, PRIMARY KEY (id))"
)
NO_PK = "CREATE TABLE logs (id VARCHAR(32) NOT NULL, msg VARCHAR(10) NOT NULL)"


def make_users(rows=300, **overrides):
    fields = {
        "id": lambda r: f"u{r.i}",
        "name": lambda r: f"n{r.i}",
        "country": pick(("CN", "US")),
    }
    fields.update(overrides)
    return Table("users", USERS, rows=rows, fields=fields)


def make_orders(parent, per_parent=2, ddl=ORDERS, **overrides):
    fields = {
        "id": lambda r: f"{r.parent['id']}-o{r.k}",
        "amount": money(1, 10),
    }
    fields.update(overrides)
    return Table(
        "orders", ddl, parent=parent, fk="user_id", per_parent=per_parent, fields=fields
    )


class SpecTest(unittest.TestCase):
    def test_ok(self):
        u = make_users()
        spec = Spec([u, make_orders(u)])
        plan = spec.plan(rows=300, task_rows=100)
        self.assertTrue(plan.ok, [str(p) for p in plan.problems])
        self.assertEqual(plan.rows_by_table["users"], 300)
        self.assertEqual(plan.rows_by_table["orders"], 600)
        self.assertEqual(plan.tasks, 3)
        self.assertTrue(plan.samples["orders"])

    def test_missing_column(self):
        spec = Spec(
            [
                Table(
                    "users",
                    USERS,
                    rows=10,
                    fields={"id": lambda r: f"u{r.i}", "name": lambda r: "x"},
                )
            ]
        )
        plan = spec.plan(rows=10)
        self.assertFalse(plan.ok)
        text = "\n".join(str(p) for p in plan.problems)
        self.assertIn("缺少列 country", text)

    def test_unknown_column_with_suggestion(self):
        spec = Spec([make_users(contry=pick(("CN",)))])
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("未知列", text)
        self.assertIn("country", text)

    def test_fk_in_fields_rejected(self):
        u = make_users()
        child = make_orders(u, user_id=lambda r: "x")
        spec = Spec([u, child])
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("外键列", text)

    def test_fk_too_short(self):
        u = make_users()
        spec = Spec([u, make_orders(u, ddl=ORDERS_SHORT_FK)])
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("截断", text)

    def test_child_pk_using_task_local_index_is_detected(self):
        u = make_users()
        child = make_orders(u, id=lambda r: f"o{r.i}")
        spec = Spec([u, child])
        plan = spec.plan(rows=300, task_rows=100)
        text = "\n".join(str(p) for p in plan.problems)
        self.assertIn("主键 id 在不同任务里重复", text)

    def test_varchar_length(self):
        spec = Spec([make_users(name=lambda r: "n" * 11)])
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("VARCHAR(10)", text)
        self.assertIn("上限 10", text)

    def test_not_null(self):
        spec = Spec([make_users(name=lambda r: None)])
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("NOT NULL", text)

    def test_enum(self):
        ddl = USERS.replace("VARCHAR(2)", "ENUM('CN','US')")
        spec = Spec(
            [
                Table(
                    "users",
                    ddl,
                    rows=10,
                    fields={
                        "id": lambda r: f"u{r.i}",
                        "name": lambda r: "x",
                        "country": lambda r: "CHN",
                    },
                )
            ]
        )
        text = "\n".join(str(p) for p in spec.plan(rows=10).problems)
        self.assertIn("枚举", text)

    def test_two_roots(self):
        spec = Spec(
            [
                make_users(),
                Table(
                    "logs",
                    NO_PK,
                    rows=5,
                    fields={"id": lambda r: f"l{r.i}", "msg": lambda r: "m"},
                ),
            ]
        )
        with self.assertRaises(SpecError):
            spec.plan(rows=10)

    def test_no_pk_warns(self):
        spec = Spec(
            [
                Table(
                    "logs",
                    NO_PK,
                    rows=10,
                    fields={"id": lambda r: f"l{r.i}", "msg": lambda r: "m"},
                )
            ]
        )
        plan = spec.plan(rows=10)
        self.assertTrue(plan.ok)
        self.assertTrue(any("主键" in p.message for p in plan.warnings))

    def test_dynamic_per_parent_has_unknown_counts(self):
        u = make_users()
        spec = Spec([u, make_orders(u, per_parent=(0, 3))])
        plan = spec.plan(rows=300, task_rows=100)
        self.assertTrue(plan.ok)
        self.assertIsNone(plan.rows_by_table["orders"])
        self.assertIsNone(plan.total_rows)

    def test_skip_column(self):
        ddl = (
            "CREATE TABLE t (id VARCHAR(8) NOT NULL, created_at DATETIME NOT NULL "
            "DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))"
        )
        spec = Spec(
            [
                Table(
                    "t",
                    ddl,
                    rows=10,
                    fields={"id": lambda r: f"x{r.i}", "created_at": SKIP},
                )
            ]
        )
        plan = spec.plan(rows=10)
        self.assertTrue(plan.ok, [str(p) for p in plan.problems])
        self.assertEqual(plan.order[0].column_names, ("id",))

    def test_rows_override(self):
        spec = Spec([make_users(rows=1_000_000)])
        plan = spec.plan(rows=500, task_rows=200)
        self.assertEqual(plan.root_rows, 500)
        self.assertEqual(plan.tasks, 3)


if __name__ == "__main__":
    unittest.main()
