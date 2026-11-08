"""V3 示例：users → orders → order_items（一父一子一孙）。

你只需要写三样东西：
  1. 建表语句（下面的 *_DDL 常量，直接贴你库里的建表语句即可）
  2. 每张表一个 generate_data 生成器，用 yield 返回行元组
  3. main()：配置数据库连接，调用 run(TABLES, rows, db, ...)

运行（在仓库根目录 d:\\ChatWithAI\\MyTest\\MyTest 下）：
  py -m MySQLScriptV3.example --backend dry-run --rows 30000
  py -m MySQLScriptV3.example --backend sqlite --rows 30000 --db %TEMP%\\v3_demo.db --wipe --verify
  py -m MySQLScriptV3.example --backend mysql --rows 30000 --database performance_db --wipe --verify
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
from datetime import datetime, timedelta

from . import MySQLConfig, SQLiteConfig, Table, run

ORDERS_PER_USER = 3
ITEMS_PER_ORDER = 2
COUNTRIES = ("CN", "US", "JP", "DE")
STATUSES = ("paid", "pending", "shipped")
BASE_TIME = datetime(2026, 1, 1)

USERS_DDL = """
CREATE TABLE users (
    id          VARCHAR(32)  NOT NULL,
    name        VARCHAR(32)  NOT NULL,
    country     VARCHAR(8)   NOT NULL,
    created_at  DATETIME     NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ORDERS_DDL = """
CREATE TABLE orders (
    id          VARCHAR(40)   NOT NULL,
    user_id     VARCHAR(32)   NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(16)   NOT NULL,
    created_at  DATETIME      NOT NULL,
    PRIMARY KEY (id),
    KEY idx_orders_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ORDER_ITEMS_DDL = """
CREATE TABLE order_items (
    id        VARCHAR(48)   NOT NULL,
    order_id  VARCHAR(40)   NOT NULL,
    sku       VARCHAR(16)   NOT NULL,
    qty       INT           NOT NULL,
    price     DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_items_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


# ---------------------------------------------------------------------------
# 每张表一个 generate_data；函数体里 yield 行元组，列顺序 = 建表语句的列顺序
# ---------------------------------------------------------------------------


def generate_users(ctx):
    """根表：必须按 ctx.start..ctx.end 产出，且正好产出 ctx.size 行。"""
    for i in range(ctx.start, ctx.end):
        yield (
            f"u{i}",                                    # id
            f"user_{i}",                                # name
            random.Random(f"{ctx.seed}|country|{i}").choice(COUNTRIES),  # country
            BASE_TIME + timedelta(minutes=i),           # created_at
        )


def generate_orders(ctx, users_batch):
    """子表：users_batch 就是父表刚写入的这一批用户行，按列顺序取用。"""
    for user in users_batch:
        user_id = user[0]  # users.id
        for k in range(ORDERS_PER_USER):
            yield (
                f"{user_id}-o{k}",                       # id
                user_id,                                 # user_id -> users.id
                round(ctx.rng.uniform(10, 500), 2),      # amount
                STATUSES[k % len(STATUSES)],             # status
                BASE_TIME + timedelta(minutes=int(user_id[1:])),  # created_at
            )


def generate_order_items(ctx, orders_batch):
    """孙表：orders_batch 是父表刚写入的这一批订单行。"""
    for order in orders_batch:
        order_id = order[0]  # orders.id
        for line in range(ITEMS_PER_ORDER):
            yield (
                f"{order_id}-i{line}",                   # id
                order_id,                                # order_id -> orders.id
                f"sku{line}",                            # sku
                ctx.rng.randint(1, 5),                   # qty
                round(ctx.rng.uniform(5, 100), 2),       # price
            )


# depends_on / fk_column 只用于排序和校验，写法和普通函数没有区别
TABLES = [
    Table(name="users", ddl=USERS_DDL, generate_data=generate_users),
    Table(
        name="orders",
        ddl=ORDERS_DDL,
        generate_data=generate_orders,
        depends_on="users",
        fk_column="user_id",
    ),
    Table(
        name="order_items",
        ddl=ORDER_ITEMS_DDL,
        generate_data=generate_order_items,
        depends_on="orders",
        fk_column="order_id",
    ),
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="MySQLScriptV3.example", description="V3 示例：users / orders / order_items"
    )
    parser.add_argument("--backend", choices=("dry-run", "sqlite", "mysql"), default="dry-run")
    parser.add_argument("--rows", type=int, default=100_000, help="根表 users 行数")
    parser.add_argument("--task-rows", type=int, default=20_000, help="每任务根表行数")
    parser.add_argument("--batch-size", type=int, default=5_000, help="每批行数")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seed", default="v3")
    parser.add_argument("--wipe", action="store_true", help="运行前清空目标表")
    parser.add_argument("--verify", action="store_true", help="运行后校验行数与孤儿外键")
    parser.add_argument("--resume", action="store_true", help="从 checkpoint 续跑")
    parser.add_argument("--checkpoint", default=".v3_checkpoint.json")
    parser.add_argument("--no-checkpoint", action="store_true")
    parser.add_argument("--no-create-tables", action="store_true")
    parser.add_argument("--db", default="v3_demo.db", help="SQLite 文件路径")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="123456")
    parser.add_argument("--database", default="performance_db")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # 1) 选数据库连接
    if args.backend == "dry-run":
        db = None
    elif args.backend == "sqlite":
        db = SQLiteConfig(path=args.db, foreign_keys=True)
    else:
        db = MySQLConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

    # 2) 调用主流程：传表定义 + 根表行数 + 连接配置
    result = run(
        TABLES,
        rows=args.rows,
        db=db,
        task_rows=args.task_rows,
        batch_size=args.batch_size,
        workers=args.workers,
        seed=args.seed,
        resume=args.resume,
        checkpoint=None if args.no_checkpoint else args.checkpoint,
        create_tables=not args.no_create_tables,
        wipe=args.wipe,
        verify=args.verify,
    )
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
