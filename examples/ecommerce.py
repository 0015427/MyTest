"""示例 / 模板：电商三张表 users -> orders -> order_items。

用法（在仓库根目录执行）：
    python -m MySQLScriptV4 plan   examples/ecommerce.py --rows 3000 --explain
    python -m MySQLScriptV4 run    examples/ecommerce.py --rows 3000 --dsn sqlite:demo.db --wipe --verify
    python -m MySQLScriptV4 run    examples/ecommerce.py --dsn "mysql://root:pass@localhost:3306/demo" --wipe --verify

给 AI 的输入只有：三条建表语句 + "orders.user_id -> users.id"、
"order_items.order_id -> orders.id" 这两条关系。
"""
from datetime import datetime, timedelta

from MySQLScriptV4 import SKIP, Table, between, maybe, money, pick, ts

TITLE = "ecommerce: users -> orders -> order_items"
SPEC_VERSION = "1"  # 改了字段规则就 +1，避免拿旧进度续新数据
SEED = "v4-demo"

BASE = datetime(2026, 1, 1)
COUNTRIES = ("CN", "US", "JP", "DE")
STATUSES = ("paid", "pending", "shipped")

USERS_DDL = """
CREATE TABLE `users` (
    `id`         VARCHAR(32)  NOT NULL,
    `name`       VARCHAR(40)  NOT NULL,
    `country`    VARCHAR(8)   NOT NULL,
    `created_at` DATETIME     NOT NULL,
    `updated_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_users_country` (`country`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

ORDERS_DDL = """
CREATE TABLE `orders` (
    `id`         VARCHAR(40)   NOT NULL,
    `user_id`    VARCHAR(32)   NOT NULL,
    `amount`     DECIMAL(10,2) NOT NULL,
    `status`     VARCHAR(16)   NOT NULL,
    `note`       VARCHAR(64)   NULL,
    `created_at` DATETIME      NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_orders_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

ORDER_ITEMS_DDL = """
CREATE TABLE `order_items` (
    `id`       VARCHAR(48)   NOT NULL,
    `order_id` VARCHAR(40)   NOT NULL,
    `sku`      VARCHAR(16)   NOT NULL,
    `qty`      INT           NOT NULL,
    `price`    DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_items_order` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# 根表：行号 i 是全局的（0..rows-1），可以直接用来组键
USERS = Table(
    name="users",
    ddl=USERS_DDL,
    rows=200_000,
    fields={
        "id": lambda r: f"u{r.i}",
        "name": lambda r: f"user_{r.i}",
        "country": pick(COUNTRIES),
        "created_at": ts(BASE, step_seconds=60),   # 2026-01-01 起每分钟一行
        "updated_at": SKIP,                        # 用数据库默认值
    },
)

# 子表：外键 user_id 不需要写，框架自动取父行的 users.id
ORDERS = Table(
    name="orders",
    ddl=ORDERS_DDL,
    parent=USERS,
    fk="user_id",
    per_parent=3,
    fields={
        "id": lambda r: f"{r.parent['id']}-o{r.k}",  # 父键 + 兄弟序号 = 全局唯一
        "amount": money(10, 500),
        "status": pick(STATUSES, weights=(6, 3, 1)),
        "note": maybe(pick(("rush", "gift", "fragile")), p=0.1),
        "created_at": lambda r: r.parent["created_at"] + timedelta(hours=r.rng.randint(0, 720)),
    },
)

# 孙表：per_parent 也可以写成 (1, 4) 或函数，让每个父行产出数量不等
ORDER_ITEMS = Table(
    name="order_items",
    ddl=ORDER_ITEMS_DDL,
    parent=ORDERS,
    fk="order_id",
    per_parent=2,
    fields={
        "id": lambda r: f"{r.parent['id']}-i{r.k}",
        "sku": lambda r: f"SKU-{r.rng.randrange(2000):05d}",
        "qty": between(1, 3),
        "price": money(5, 100),
    },
)

TABLES = [USERS, ORDERS, ORDER_ITEMS]
