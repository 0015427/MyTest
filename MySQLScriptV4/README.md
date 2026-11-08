# MySQLScriptV4

给 MySQL / SQLite 造数据：**贴建表语句 + 说明表关系 → 生成一个 spec 文件 → 校验 → 流式写入 → 事后核对**。

V4 和 V3 是两套并存的实现，V3 目录保持不动。区别只有一句话：

> V3 让用户/AI 写“每张表怎么 yield 一行”的 Python 生成器；
> V4 让用户/AI 写“每列用什么规则”的**声明式表定义**，列顺序、外键取值、行数、事务、并发、续跑、校验都由框架负责。

---

## 1. 5 分钟上手

```powershell
# 1) 只校验 + 打印执行计划和样例（不连库，AI 自检也用它）
python -m MySQLScriptV4 plan examples/ecommerce.py --rows 3000

# 2) 先写 SQLite，验证数据长什么样
python -m MySQLScriptV4 run examples/ecommerce.py --rows 3000 `
  --dsn sqlite:demo.db --wipe --verify

# 3) 写 MySQL
python -m MySQLScriptV4 run examples/ecommerce.py `
  --dsn "mysql://root:pass@localhost:3306/demo" --wipe --verify

# 4) 只做一致性校验
python -m MySQLScriptV4 verify examples/ecommerce.py --dsn sqlite:demo.db
```

`plan` 是默认命令，`python -m MySQLScriptV4 examples/ecommerce.py` 等价于 `plan`。
MySQL 后端需要 `pip install pymysql`；SQLite / dry-run 不依赖第三方包。
终端输出全部是 ASCII 标签（`rows / tasks / PASS / FAIL`），避免 Windows 代码页问题；
中文只出现在错误提示和文档里。

## 2. 推荐工作流（AI 参与的那条路）

1. 从库里复制每张表的 `CREATE TABLE`，再写清楚关系（例：`orders.user_id -> users.id`）。
2. 把这两样东西 + [AI_PROMPT.md](AI_PROMPT.md) 一起丢给 AI，让它输出一个 spec 文件。
3. 让 AI 自己跑：`python -m MySQLScriptV4 plan spec.py --rows 500 --json`，
   有 `error` 就改，直到 `"ok": true`。JSON 里有精确到“表.列”的问题和提示。
4. 人工过目：`plan spec.py --sample 5 --explain`，逐行看样例值，`--explain` 会标出每列规则定义在文件的第几行。
5. 小样本落库：`run spec.py --rows 2000 --dsn sqlite:demo.db --wipe --verify`。
6. 全量：`run spec.py --dsn "mysql://..." --wipe --verify`，看进度条和最终的 PASS/FAIL。

> 经验：第 3 步是省时间的关键。`plan` 不连库，1 秒内跑完，能把“列名写错、少一列、外键列不该手写、子表主键撞车、值超长、枚举越界”这些最常见错误全部挡在写库之前。

## 3. spec 文件长什么样

一个文件，三段：DDL 常量、表定义、`TABLES` 列表。约 60~120 行，可以直接进 code review。

> DDL 很多时可以拆到同目录的 `schema.py`，spec 里 `from schema import *` 即可：
> 加载 spec 时会把它的所在目录加进 `sys.path`。

```python
from datetime import datetime, timedelta
from MySQLScriptV4 import SKIP, Table, between, money, pick, ts

TITLE = "ecommerce"
SPEC_VERSION = "1"      # 改了字段规则就 +1
SEED = "demo"

USERS_DDL = """CREATE TABLE `users` (
  `id` VARCHAR(32) NOT NULL, `name` VARCHAR(40) NOT NULL,
  `country` VARCHAR(8) NOT NULL, `created_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`))"""

USERS = Table(
    name="users", ddl=USERS_DDL, rows=200_000,
    fields={
        "id": lambda r: f"u{r.i}",                  # r.i = 全局行号
        "name": lambda r: f"user_{r.i}",
        "country": pick(("CN", "US", "JP", "DE")),
        "created_at": ts(datetime(2026, 1, 1), step_seconds=60),
    },
)

ORDERS = Table(
    name="orders", ddl=ORDERS_DDL,
    parent=USERS, fk="user_id", per_parent=3,       # 每个用户 3 单
    fields={
        "id": lambda r: f"{r.parent['id']}-o{r.k}",  # 父键 + 兄弟序号
        "amount": money(10, 500),
        "status": pick(("paid", "pending", "shipped"), weights=(6, 3, 1)),
        "created_at": lambda r: r.parent["created_at"] + timedelta(days=r.rng.randint(0, 30)),
    },
)

TABLES = [USERS, ORDERS]
```

几个关键约定：

| 约定 | 说明 |
| --- | --- |
| `fk` 列不要写进 `fields` | 值自动取父行主键，保证不悬空 |
| 列顺序不用管 | 按 DDL 的列顺序自动组装；少列/多列/拼错列名 `plan` 直接报错 |
| 想用数据库默认值 | 写 `"updated_at": SKIP` |
| 子表键用 `父键 + r.k` | `r.i` 是任务内序号，不能单独拿来组子表主键（`plan` 有跨任务撞键检测） |
| 不要用全局 `random` | 用 `r.rng`，否则重跑/续跑数据会变 |
| 金额用 `money()` | 返回 `Decimal`，不会出现 `12.340000000001` |
| 时间用 `ts()/dates()` | 返回 `datetime/date`，SQLite/MySQL 都能写 |

### 字段规则的完整清单

| 规则 | 含义 |
| --- | --- |
| `lambda r: ...` | 任意逻辑，最常用；`r.parent['col']` 取父行，`r.rng` 取随机 |
| `pick(values, weights=None)` | 等概率/加权随机选一个 |
| `between(lo, hi)` | 闭区间随机整数 |
| `real(lo, hi, ndigits=None)` | 区间随机浮点 |
| `money(lo, hi, scale=2)` | 区间随机金额（`Decimal`） |
| `ts(start, span_seconds=None, step_seconds=None, jitter_seconds=0)` | 按行号步进，或区间随机，可叠加抖动 |
| `dates(start, end)` | 闭区间随机日期 |
| `uuid4()` | 确定性 32 位十六进制 |
| `token(length=12)` | 随机字符串 |
| `const(value)` / `maybe(rule, p)` / `boolean()` | 常量 / 以概率为空 / 随机布尔 |
| `SKIP` | 不写这一列，用数据库默认值 |

### 行上下文 `r`

| 字段 | 根表 | 子表 |
| --- | --- | --- |
| `r.i` | 全局行号 `0..rows-1` | 任务内序号（**不要单独做全局键**） |
| `r.k` | 0 | 同一父行下的第几个子行 |
| `r.parent` | 空 | 父行，`r.parent['id']` |
| `r.rng` | 任务级 `random.Random` | 同左 |
| `r.task` / `r.table` | 任务号 / 表名 | 同左 |

### `Table` 参数

| 参数 | 说明 |
| --- | --- |
| `name` / `ddl` | 必须与建表语句里的表名一致 |
| `fields` | 列名 -> 规则 |
| `rows` | **只有根表需要**：根表总行数 |
| `parent` / `fk` | 父表对象 + 子表里的外键列名 |
| `per_parent` | 每个父行产几行：整数 / `(lo, hi)` / `fn(parent_row, rng)` |
| `batch_rows` | 覆盖该表的每批行数（默认按行宽算） |

## 4. 命令行

```text
python -m MySQLScriptV4 plan   spec.py [--rows N] [--task-rows N] [--sample N]
                                        [--explain] [--json] [--no-warnings]
python -m MySQLScriptV4 run    spec.py --dsn DSN [--rows N] [--task-rows N]
                                        [--workers N] [--seed S] [--wipe] [--resume]
                                        [--verify] [--keep-going] [--check first|all|none]
                                        [--no-create] [--no-progress] [--dry-run]
python -m MySQLScriptV4 verify spec.py --dsn DSN [--rows N]
```

DSN 写法：

```text
dry-run                                 只生成，不落库（或不给 --dsn 直接 --dry-run）
sqlite:demo.db                          相对路径
sqlite:///C:/tmp/demo.db                URL 写法
mysql://root:pass@localhost:3306/demo?charset=utf8mb4
```

## 5. 它到底保证了什么

这些不是文档里的口号，是代码里真的会检查/会回滚的点：

1. **列不可能错位**：行按 DDL 列顺序组装，不是按 `fields` 顺序。
2. **外键不会悬空**：子表外键直接从“刚写入的这一批父行”取，不需要回读父表。
3. **行数可核对**：固定 `per_parent` 时，总行数在 `plan` 就能算出来，`verify` 逐表核对。
4. **一个任务一个事务**：任务失败整段回滚，不会出现“父表提交了、子表没提交”。
5. **续跑不重不漏**：进度行 `_dataforge_tasks` 和数据在**同一个事务**里提交；
   进程在任意时刻挂掉，续跑只会重做未提交的任务。进度记录在目标库内，换库/删库不会误判。
6. **失败分类型**：锁等待/死锁/连接断开才重试（默认 3 次，退避）；数据错误（超长、枚举、主键冲突）立刻失败。
7. **包大小双重保险**：先按行宽估字节切批；发送前再按实际值估一次，超限就缩批重试（实测 5000 行 × 16KB 触发 1153，5 次缩批后成功，数据完整）。
8. **启动时对照表结构**：连库后、`wipe` 之前检查每张表的列是否存在、类型/长度/精度是否兼容；不一致直接报错，**不会先清数据再失败**。
9. **事后三查**：根表/子表行数、孤儿外键、重复主键。

## 6. 性能（本机实测，Python 3.13）

| 场景 | 结果 |
| --- | --- |
| 生成 10,000,000 行，全确定性规则 | 6.8 s，**1.46M 行/s** |
| 生成 10,000,000 行，示例规则（加权枚举/金额/时间/可空） | 27.6 s，363k 行/s |
| SQLite 写入 1,000,000 行（与 V3 同规则） | 3.9 s，255k 行/s |
| SQLite 写入 2,000,000 行（示例规则，8 workers） | 10.5 s，190k 行/s |
| MySQL 写入 1,000,000 行（100k 用户，8 workers） | 6.1 s，163k 行/s |
| MySQL 写入 10,000,000 行（1M 用户，8 workers） | 写库约 63 s，端到端 98.6 s（含 verify），约 159k 行/s |
| MySQL `verify` 10,000,000 行 | 33.8 s（COUNT DISTINCT + 孤儿 LEFT JOIN） |
| 峰值 Python 堆，1M 行，4 / 8 workers | 20.7 MB / 40.6 MB |

MySQL 环境：MySQL 26.10.0（InnoDB，`max_allowed_packet=64MB`，REPEATABLE-READ）、
PyMySQL 1.2.3、本机 127.0.0.1；10M 行期间 `Innodb_row_lock_waits=0`、重试 0 次。

内存只跟 `workers × batch_rows` 有关，不随总行数增长；10M 行和 2M 行的峰值在同一量级。
（内存是用 `tracemalloc` 量的，它本身会让生成慢 3~5 倍，性能数字都是关掉它测的。）

参数经验值：

- `--task-rows`：默认 10,000 根表行。MySQL 按 redo 容量和复制延迟调，5k~20k。
- `--workers`：默认 `min(4, CPU)`。MySQL 4~8；SQLite 写并发收益有限，1~4。
- `--packet-mb`：默认 4 MB，按 `max_allowed_packet` 留余量。

## 7. 明确不做 / 已知限制

- **单根表**：一次 `run` 只允许一个 `parent=None` 的根表；无关的表分文件跑。
- **单列主键**：多列主键请先加代理键。
- **不回读 AUTO_INCREMENT**：所有列都要显式给值，自增主键请自己生成 id（这也让重跑幂等）。
- **子行只能引用同一任务的父行**：这是任务级事务/并发/续跑成立的前提。
- **规则代码不参与签名**：改了字段规则请把 `SPEC_VERSION` +1，否则可能拿旧进度续新数据。
- **目标表结构必须与 DDL 一致**：启动时会检查列名/类型/长度/精度；不一致直接报错，且不会执行 `wipe`。
- **SQLite 只是代理**：不建二级索引，CHECK/生成列/部分默认值与 MySQL 有差异；最终以 MySQL 为准。
- **不支持 `:memory:`**：并发任务用多条连接，会各看各的库。
- 表名/列名限定为 `[A-Za-z_][A-Za-z0-9_$]*`。

## 8. 常见报错

| 报错 | 原因 / 处理 |
| --- | --- |
| `缺少列 xxx 的取值规则` | `fields` 少了一列；或用 `"xxx": SKIP` |
| `未知列 'contry'` | 拼错了，提示里会给出最接近的列名 |
| `外键列值由框架自动填充` | 把 `fk` 列写进了 `fields`，删掉 |
| `外键列 VARCHAR(4) 比父表主键 VARCHAR(32) 短` | 子表外键列太短，改 DDL |
| `主键 id 在不同任务里重复` | 子表用了 `r.i` 组键，改成 `r.parent['id'] + r.k` |
| `是 VARCHAR(10)，值 '...' 有 N 个字符` | 值超长；`plan` 阶段就能看到 |
| `这个库已经有相同签名的运行记录` | 加 `--resume` 续跑，或 `--wipe` 重来 |
| `没有找到可续跑的记录` | 签名变了（行数/任务大小/种子/DDL/库）；去 `--resume` 或 `--wipe` |
| `--wipe 和 --resume 不能同时使用` | 这是有意为之：清空 + 跳过 = 空库 |
| `目标表结构与 DDL 不一致` | 连错库/表，或 DDL 过期；启动时已检查，**不会清数据** |
| `这一批预计 N 字节，超过上限` | 行太宽；框架会自动缩批重试，一般不需要处理 |

## 9. 代码结构

```text
MySQLScriptV4/
├── __init__.py     对外导出（Table / rules / run / RunConfig）
├── __main__.py     python -m MySQLScriptV4 入口
├── cli.py          plan / run / verify，终端报告
├── loader.py       从任意路径加载 spec 文件
├── ddl.py          解析 CREATE TABLE（列/类型/主键/默认值/生成列）
├── rules.py        字段规则库
├── spec.py         Table / Spec / Plan，静态校验与计划
├── gen.py          流式生成、行上下文、跨任务主键探针
├── sink.py         SQLite / MySQL / dry-run 写入器，进度表，verify
├── engine.py       任务划分、线程池、事务、重试、进度
├── progress.py     终端进度
├── tests/          52 个单元/集成测试
├── AI_PROMPT.md    给 AI 的提示词模板
└── DESIGN.md       设计说明与取舍

examples/
├── ecommerce.py          users → orders → order_items（通用）
└── mysql_fk_types.py     真实外键 + utf8mb4 中文/emoji + JSON/BLOB/DATE
```

测试：

```powershell
python -m unittest discover -s MySQLScriptV4/tests -t .
```
