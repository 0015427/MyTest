"""MySQLScriptV3：表定义 + 每表一个 yield 生成器 + 批量入库。

用户写法：
    from MySQLScriptV3 import Table, MySQLConfig, run

    TABLES = [
        Table(name="users", ddl=USERS_DDL, generate_data=generate_users),
        Table(name="orders", ddl=ORDERS_DDL, generate_data=generate_orders,
              depends_on="users", fk_column="user_id"),
    ]
    run(TABLES, rows=1_000_000, db=MySQLConfig(host="...", user="...", password="...",
                                              database="..."))
"""
from .api import MySQLConfig, RunResult, SQLiteConfig, VerifyItem, run
from .tables import Table, GenContext

__all__ = [
    "Table",
    "GenContext",
    "MySQLConfig",
    "SQLiteConfig",
    "RunResult",
    "VerifyItem",
    "run",
]
