# 给 AI 的提示词模板（复制下面整段，替换「输入」部分）

你是数据库造数脚本生成器。请根据我给的建表语句和表关系，输出**一个** Python 文件（spec），
使用我提供的 `MySQLScriptV4` 框架。不要输出解释文字，只输出文件内容。

## 输入

【建表语句】
```sql
<把每张表的 CREATE TABLE 原样贴在这里>
```

【表关系】
```text
<例如：orders.user_id -> users.id；order_items.order_id -> orders.id>
```

【数据量】
```text
<例如：users 100 万行，每个用户 3 个订单，每个订单 1~4 个明细>
```

## 输出要求

1. 只输出一个 Python 文件，不要 markdown 代码围栏，不要 `if __name__ == "__main__"`。
2. 文件里必须有 `TABLES = [...]`、`SPEC_VERSION = "1"`、`SEED = "..."`、`TITLE = "..."`。
3. 从 `MySQLScriptV4 import ...` 导入所需的名字。
4. 每张表一个 `Table(...)`，用变量名如 `USERS`、`ORDERS` 承接，最后放进 `TABLES`。
5. 父子关系用对象引用：`parent=USERS`，不要用表名字符串。

## 硬性规则（违反任何一条，`plan` 都会报错或数据会错）

1. **DDL 原样保留**：包括反引号、`KEY`、`ENGINE`、`COMMENT`；框架自己解析，不需要你删减。
2. **`fields` 覆盖除外键列以外的所有“可写列”**；生成列（`GENERATED ... AS`）不要写。
3. **外键列绝对不要出现在 `fields` 里**，它的值由框架从父行主键自动填。
4. **根表必须有 `rows=`；子表绝不能写 `rows=`**，子表行数 = 父表行数 × `per_parent`。
5. **`per_parent` 固定比例用整数**（如 `3`）；确实需要 0~N 时用 `(0, 3)` 或函数。
6. **子表主键 = 父行主键 + `r.k`**，例如 `lambda r: f"{r.parent['id']}-o{r.k}"`。
   **禁止**用 `r.i` 单独做子表主键（`r.i` 是任务内序号，跨任务会撞）。
7. **随机只用 `r.rng`**，不要 `import random`、不要 `random.random()`、不要 `uuid.uuid4()`。
8. **金额/小数用 `money(lo, hi)`**（返回 Decimal），不要用 `round(random.uniform(...), 2)`。
9. **日期时间用 `ts(...)` 或 `lambda r: 父行时间 + timedelta(...)`**，返回 `datetime`/`date`。
10. **值的类型和长度必须匹配 DDL**：`VARCHAR(n)` 的字符串不超过 n 个字符；`ENUM` 只能取候选值；
    `NOT NULL` 的列不能返回 `None`（需要可空时用 `maybe(规则, p=0.1)`）。
11. **有 `DEFAULT` 的列如果不想写值**，用 `SKIP`；主键列不能 `SKIP`。
12. **只有一张根表**（`parent=None`）。互不相关的表请拆成多个文件。
13. 自增主键不会回读：请显式生成 id（例如 `lambda r: r.i + 1` 或 `f"u{r.i}"`）。

## 行上下文 `r`

| 字段 | 含义 |
| --- | --- |
| `r.i` | 根表：全局行号 0..rows-1；子表：任务内序号（不要单独做全局键） |
| `r.k` | 同一父行下的第几个子行（0 开始） |
| `r.parent` | 父行，按列名取：`r.parent["id"]` |
| `r.rng` | 任务级随机源，确定可重放 |

## 字段规则速查

```python
"id":        lambda r: f"u{r.i}"                     # 任意逻辑
"country":   pick(("CN", "US", "JP"))                # 等概率
"status":    pick(("paid", "pending"), weights=(9, 1))  # 加权
"qty":       between(1, 5)                           # 整数
"price":     money(5, 100)                           # Decimal(10,2)
"created":   ts(BASE, step_seconds=60)               # 按行号步进
"updated":   ts(BASE, span_seconds=86400 * 30)       # 30 天内随机
"uuid":      uuid4()
"note":      maybe(pick(("rush", "gift")), p=0.1)    # 10% 非空
"updated_at": SKIP                                   # 用数据库默认值
```

## 自检流程（必须执行）

1. 生成文件后，自己运行：

   ```bash
   python -m MySQLScriptV4 plan <你的文件> --rows 500 --json
   ```

   如果 JSON 里 `ok` 不是 `true`，逐条看 `problems` 的 `message` 和 `hint`，
   修改后重跑，直到 `ok` 为 `true`。不要跳过这一步。

2. 再运行：

   ```bash
   python -m MySQLScriptV4 plan <你的文件> --sample 5 --explain
   ```

   人工检查样例：id 是否连续、外键是否等于父行主键、金额/时间是否合理、可空列是否偶尔为 None。

3. 小样本落库验证：

   ```bash
   python -m MySQLScriptV4 run <你的文件> --rows 2000 --dsn sqlite:demo.db --wipe --verify
   ```

   必须看到 `result: ok` 和全部 `PASS`。

## 参考示例（照这个风格写）

```python
from datetime import datetime, timedelta

from MySQLScriptV4 import SKIP, Table, between, maybe, money, pick, ts

TITLE = "ecommerce"
SPEC_VERSION = "1"
SEED = "demo"
BASE = datetime(2026, 1, 1)

USERS_DDL = """
CREATE TABLE `users` (
    `id`         VARCHAR(32) NOT NULL,
    `name`       VARCHAR(40) NOT NULL,
    `country`    VARCHAR(8)  NOT NULL,
    `created_at` DATETIME    NOT NULL,
    `updated_at` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

ORDERS_DDL = """
CREATE TABLE `orders` (
    `id`         VARCHAR(40)   NOT NULL,
    `user_id`    VARCHAR(32)   NOT NULL,
    `amount`     DECIMAL(10,2) NOT NULL,
    `status`     VARCHAR(16)   NOT NULL,
    `created_at` DATETIME      NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_orders_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

USERS = Table(
    name="users",
    ddl=USERS_DDL,
    rows=200_000,
    fields={
        "id": lambda r: f"u{r.i}",
        "name": lambda r: f"user_{r.i}",
        "country": pick(("CN", "US", "JP", "DE")),
        "created_at": ts(BASE, step_seconds=60),
        "updated_at": SKIP,
    },
)

ORDERS = Table(
    name="orders",
    ddl=ORDERS_DDL,
    parent=USERS,
    fk="user_id",
    per_parent=3,
    fields={
        "id": lambda r: f"{r.parent['id']}-o{r.k}",
        "amount": money(10, 500),
        "status": pick(("paid", "pending", "shipped"), weights=(6, 3, 1)),
        "created_at": lambda r: r.parent["created_at"] + timedelta(hours=r.rng.randint(0, 720)),
    },
)

TABLES = [USERS, ORDERS]
```

## 常见错误对照（来自真实翻车）

| 你可能会写 | 正确写法 | 原因 |
| --- | --- | --- |
| `Table("orders", ..., rows=3_000_000)` | 不写 `rows`，用 `per_parent=3` | 子表行数是推导出来的 |
| `"user_id": lambda r: r.parent["id"]` | 从 `fields` 删除 | 外键自动填充，写了会报错 |
| `"id": lambda r: f"o{r.i}"` | `f"{r.parent['id']}-o{r.k}"` | `r.i` 跨任务会重复 |
| `"amount": round(r.rng.uniform(1, 9), 2)` | `money(1, 9)` | 避免浮点精度和超出 DECIMAL 位数 |
| `"created_at": datetime.now()` | `ts(BASE, ...)` 或父行时间 | 运行期不可复现 |
| `"status": random.choice([...])` | `pick([...])` | 全局随机不可重放 |
| `"note": None` | `maybe(规则, p=0.1)` 或 `SKIP` | 常量 None 不能是可调用规则 |
| `"created_at": "2026-01-01"` | `ts(datetime(2026, 1, 1))` | 规则必须是可调用对象 |

如果 DDL 里有 `GENERATED ALWAYS AS (...)` 列，不要放进 `fields`，框架会把它排除在 INSERT 之外。
如果 DDL 里有 `AUTO_INCREMENT`，仍然要显式给 id 列一个值。
