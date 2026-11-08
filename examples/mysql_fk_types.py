"""MySQL 示例：真实外键约束 + utf8mb4（中文/emoji）+ JSON / VARBINARY / DATE。

和 ecommerce.py 的区别：
  * DDL 里真的写了 FOREIGN KEY，用来验证“父先子后”的插入顺序和 foreign_key_checks=1；
  * 字符串包含中文和 emoji，用来验证连接 charset 和库的 utf8mb4；
  * 覆盖 JSON、VARBINARY、DATE、TINYINT(1)、DECIMAL(6,2) 几种类型。

用法：
    python -m MySQLScriptV4 plan examples/mysql_fk_types.py --rows 1000 --sample 3
    python -m MySQLScriptV4 run  examples/mysql_fk_types.py --rows 1000 `
      --dsn "mysql://root:123456@127.0.0.1:3306/demo" --wipe --verify
"""
from datetime import date, datetime, timedelta

from MySQLScriptV4 import Table, between, money, ts

TITLE = "mysql: fk + utf8mb4 + json/blob/date"
SPEC_VERSION = "1"
SEED = "mysql-demo"
BASE = datetime(2026, 1, 1)

USERS_DDL = """
CREATE TABLE `users` (
    `id`         VARCHAR(32)  NOT NULL,
    `name`       VARCHAR(64)  NOT NULL,
    `bio`        TEXT         NULL,
    `meta`       JSON         NULL,
    `avatar`     VARBINARY(8) NULL,
    `born`       DATE         NULL,
    `score`      DECIMAL(6,2) NOT NULL,
    `active`     TINYINT(1)   NOT NULL,
    `created_at` DATETIME     NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

ORDERS_DDL = """
CREATE TABLE `orders` (
    `id`         VARCHAR(40)   NOT NULL,
    `user_id`    VARCHAR(32)   NOT NULL,
    `amount`     DECIMAL(10,2) NOT NULL,
    `created_at` DATETIME      NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_orders_user` (`user_id`),
    CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CN_NAMES = ("张三", "李四", "王五", "赵六")

USERS = Table(
    name="users",
    ddl=USERS_DDL,
    rows=1000,
    fields={
        "id": lambda r: f"u{r.i}",
        "name": lambda r: f"{CN_NAMES[r.i % 4]}_{r.i}" + ("😀" if r.i % 2 == 0 else ""),
        "bio": lambda r: f"这是第 {r.i} 个用户，备注：中文、标点、换行\n第二行",
        "meta": lambda r: '{"idx": %d, "tag": "中文", "ok": true}' % r.i,
        "avatar": lambda r: bytes([r.i % 256, (r.i // 256) % 256]),
        "born": lambda r: date(1990, 1, 1) + timedelta(days=r.i % 10000),
        "score": money(1, 9999),
        "active": between(0, 1),
        "created_at": ts(BASE, step_seconds=60),
    },
)

ORDERS = Table(
    name="orders",
    ddl=ORDERS_DDL,
    parent=USERS,
    fk="user_id",
    per_parent=2,
    fields={
        "id": lambda r: f"{r.parent['id']}-o{r.k}",
        "amount": money(1, 5000),
        "created_at": lambda r: r.parent["created_at"] + timedelta(hours=r.k),
    },
)

TABLES = [USERS, ORDERS]
