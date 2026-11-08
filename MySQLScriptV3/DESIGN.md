# MySQL 数据生成与入库 V3 设计方案

## 0. 结论先行：用户写什么，框架做什么

V3 只有一种用法：**用户提供建表语句、为每张表写一个 `generate_data` 生成器、在 `main` 里配置连接并调用 `run`**。框架不要求用户理解任何表关系模型，只负责“调用生成器 → 批量 INSERT → 事务 / 并发 / 续跑”。

用户侧的三样东西：

```python
# 1) 建表语句：直接贴你库里的 CREATE TABLE
USERS_DDL = """
CREATE TABLE users (
    id         VARCHAR(32) NOT NULL,
    name       VARCHAR(32) NOT NULL,
    country    VARCHAR(8)  NOT NULL,
    created_at DATETIME    NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# 2) 每张表一个 generate_data，用 yield 返回行元组（列顺序 = 建表语句的列顺序）
def generate_users(ctx):
    for i in range(ctx.start, ctx.end):
        yield (f"u{i}", f"user_{i}", "CN", BASE_TIME + timedelta(minutes=i))

# 根表之外的表，多收一个参数：父表刚产出的这一批行
def generate_orders(ctx, users_batch):
    for user in users_batch:
        user_id = user[0]
        for k in range(3):
            yield (f"{user_id}-o{k}", user_id, 99.0, "paid", BASE_TIME)

# 3) main：声明表、配置连接、调用 run
TABLES = [
    Table(name="users", ddl=USERS_DDL, generate_data=generate_users),
    Table(name="orders", ddl=ORDERS_DDL, generate_data=generate_orders,
          depends_on="users", fk_column="user_id"),
]

def main():
    run(TABLES, rows=1_000_000,
        db=MySQLConfig(host="localhost", user="root", password="***", database="demo"))
```

框架内部做的事（用户不需要配，也不需要先学）：

| 关注点 | 旧方案（根目录 V1/V2/Example） | V3 |
| --- | --- | --- |
| 生成方式 | 先把全部行 append 进 List，再按 index 切片 | `yield` 逐行产出，下游拉一批才前进一批 |
| 内存 | O(总行数)，10M 行约 2.5~4 GB | O(任务数 × 每批行数)，实测 10M 行 15.8 MB |
| 子表外键 | 回读父表 ID，或 `random.choice(父ID列表)` | 子表生成器直接收到**父表刚写入的这一批行** |
| 事务 | 每批一提交（父有子无）或整库一个大事务 | 一个任务（一段根表行号）一个事务 |
| 并发 | 每批新建连接 + 全量切片二次放大 | 线程池跑任务，每 worker 一条长连接 |
| 断点 | 表级（一张表失败全废） | 任务级 checkpoint，可续跑 |
| 正确性 | 无校验，列数都可能对不上 | 启动校验拓扑/列数/外键列，运行校验行数，`verify` 校验孤儿外键 |

---

## 1. 现状分析：旧方案到底哪里不行

### 1.1 生成侧：全量物化 + 二次切片

- `MySQLScript.py:823` `generate_test_data(count)` 逐行 `data.append(...)`（`:846`），返回 `List[Tuple]`。10M 行时仅数据本身（4~6 字段元组 + 2 个字符串）约 250 B/行，即 **2.5 GB 量级**，还未算 list 指针数组（10M × 8 B ≈ 80 MB）。
- `MySQLScript.py:152` `batches = [data_list[i:i + batch_size] ...]` 在真正写入前又把整表切一次；10M 行 / 10000 = 1000 个切片列表 → **再复制一遍指针数组**，峰值内存继续上抬。
- `MySQLScript.py:195` 一次性 `executor.submit` 全部 1000 个批次（无界任务队列），future 与批次数据同时驻留。
- `MySQLScriptExample.py:28` 每行生成一个 UUID；`MySQLScriptExample.py:39` 每行调用 `datetime.now()` —— **生成过程本身不可复现**：重跑、重试得到的数据不同，这也是为什么旧方案只能靠“清表重来”。

### 1.2 关联侧：靠回读和随机选父键，生成与写入强耦合

- `MutiTableBatchDeal.py:108-133` `_update_id_mapping`：先插父表，再 `SELECT id, key FROM 父表 WHERE key IN ('k1','k2',...)`（`:122-127`）。两处硬伤：SQL 用字符串拼接（键里含引号即坏），以及**必须等父表整表插完才能生成子表**。
- `MySQLScript.py:466-486` `_update_foreign_keys`：从 `_save_id_mapping` 保存的全部父 ID 列表里 `random.choice` 挑一个（`:481-482`）。子表外键在生成阶段无法验证，父键集合还得整表常驻内存。
- `Tool/MySQLScriptV2.py:597` `_generate_table_data` 里，每生成一行都重建一次父表 ID 列表：`parent_ids = [row[0] for row in existing_data[parent_table]]` 位于行循环内部。复杂度 O(子行数 × 父行数)。
- `MySQLScriptExample.py:308-316` `plan_one` 传入 4 个列名，而 `generate_test_data` 产出的行是 6 元组（`:41`）——**没有任何列数/表结构校验，代码直接跑挂**。

### 1.3 表间比例：所谓“比例参数”是假的

- `Tool/MySQLScriptV2.py:676-681`（根目录 `MySQLScript.py:676-681` 同）`relationship_multiplier` 的唯一用途是 `actual_batch_size = int(base_batch_size * multiplier)`——**它改的是批量大小，不是数据量配比**。
- 真实比例只写死在 `MySQLScriptExample.py:425-441` 的循环次数里（users 1000 / orders 3000 / order_items 9000），与配置无关。
- 跨表批次按 `max_batches` 对齐（`MySQLScript.py:574`、`:578-607`）：某张表数据先耗尽后，后续批次只剩剩余表，事务内容和提交粒度随数据规模漂移。

### 1.4 写入与事务：提交粒度和失败语义都不成立

- `MySQLScript.py:117-121` 每批 `executemany` 后立刻 `commit()`；`:126-128` 失败只回滚“当前这一批”，**已提交的批次留在库里**，函数只返回 `False`，没有失败批次清单。
- `MutiTableBatchDeal.py:33-42` 另一个极端：所有表塞进**一个事务**。10M 行一个事务时回滚要重放全部 undo；`:224-226` 的断点又是“整张表完成”粒度。
- `MySQLScript.py:166` 每个批次都 `pymysql.connect(...)` 新建连接，`:169-189` 每次开/关各跑 3 条 `SET`。1000 个批次 → 至少 1000 次握手 + 6000 条 SET 往返，纯开销。
- `MySQLScript.py:344-359` `_apply_bulk_optimizations` 无条件关掉 `unique_checks` 与 `foreign_key_checks`；主键唯一性是幂等与续跑的基础，V3 默认保持开启。
- `MySQLScriptExample.py:56` `id VARCHAR(64) NOT NULL` **没有声明 PRIMARY KEY**，重复写入不会被拦。

### 1.5 结论

旧方案把“生成—关联—写入”揉在同一个函数里，只能用“全量 List + index 切片”来对齐，于是：比例只能假借 batch_size、关联只能靠回读或随机、事务只能靠跨表批次循环。**这不是参数没调好，而是模型不成立。** V3 重新划分职责：用户只管“每张表怎么产出下一行”，框架管“怎么把行安全、并发、可续地写进去”。

---

## 2. 用户侧写法

### 2.1 三个对象

- **建表语句**：一张表一个 DDL 常量。V3 会解析列名/列顺序/主键（只支持单列主键），并用于：
  - 列数校验（生成器产出的元组长度必须等于列数）；
  - 建表（`CREATE TABLE IF NOT EXISTS`，MySQL 与 SQLite 均支持）；
  - 外键校验（子表 `fk_column` 必须存在于 DDL 列里）。
- **`generate_data`**：一张表一个 Python 函数，函数体里 `yield` 行元组。
- **`Table(name, ddl, generate_data, depends_on=None, fk_column=None, batch_size=None)`**：`depends_on` / `fk_column` 只用于**排序和校验**，不影响生成器怎么写。
- **`main`**：配置 `MySQLConfig` / `SQLiteConfig`（或 `None` 表示 dry-run），调用 `run(...)`。

### 2.2 两种签名

| 表类型 | 签名 | 说明 |
| --- | --- | --- |
| 根表（`depends_on=None`） | `generate_data(ctx)` | 按 `ctx.start..ctx.end` 产出根表行 |
| 子表（`depends_on` 已声明） | `generate_data(ctx, parent_rows)` | `parent_rows` 是父表**刚写入的这一批行**（元组列表） |

`ctx` 提供：

| 字段 | 含义 |
| --- | --- |
| `ctx.start` / `ctx.end` | 根表行号区间（根表生成器必须严格使用） |
| `ctx.size` | `end - start` |
| `ctx.rng` | 按 `(seed, 表名, 任务号)` 播种的 `random.Random`，同一任务重跑得到相同数据 |

### 2.3 唯一需要遵守的约定

**根表的 `generate_data` 必须从 `ctx.start` 产到 `ctx.end`，而且正好产出 `ctx.size` 行。** 原因：一段根表行号 = 一个任务 = 一个事务 = 一个并行单元 = 一个续跑单元。子表生成器不需要关心这个区间，它只处理传进来的父表批次；但为了数据可重放，`ctx` 一样会传给子表（同一个任务内 `ctx.rng` 稳定）。

框架在提交前会核对根表实际行数 == `ctx.size`，不一致直接报错并回滚该任务，不会写入半个任务的数据。

### 2.4 多表引用一致性怎么保证（不需要用户学模型）

子表的 `generate_data` 拿到的是父表**刚写入这一批**的行元组，外键直接从父行里取：

```python
def generate_orders(ctx, users_batch):
    for user in users_batch:
        user_id = user[0]                # 直接用父行的主键
        for k in range(ORDER_PER_USER):
            yield (f"{user_id}-o{k}", user_id, ...)
```

这带来三个性质：

1. **外键天然正确**：子行的外键必然来自同一任务、同一批次内的父行，不会悬挂。
2. **不需要 ID 回读**：不需要 `SELECT id FROM 父表`，也不需要“逻辑键 → 实际 ID”映射字典。
3. **不需要扇出/行号算术**：每父行几行、怎么分配，完全由用户自己的循环决定（甚至可以不固定比例）。

任务的写入顺序是拓扑序（父 → 子 → 孙），同一事务内父表先写，所以即使 `foreign_key_checks=1` 也能立刻通过校验。

### 2.5 用户可调参数

| 参数 | 含义 | 默认 |
| --- | --- | --- |
| `rows` | 根表总行数 | 必填 |
| `task_rows` | 一个任务的根表行数（= 一个事务/并行/续跑单位） | 20,000 |
| `batch_size` | 每批行数，`Table.batch_size` 可覆盖 | 5,000 |
| `workers` | 并发任务数，每 worker 一条连接 | 4 |
| `seed` | 生成种子 | `"v3"` |
| `db` | `None` / `SQLiteConfig` / `MySQLConfig` | `None`（dry-run） |
| `wipe` / `verify` / `resume` | 清表 / 校验 / 断点续跑 | `False` |

---

## 3. 框架内部实现

### 3.1 任务划分

```
任务 k = 根表行区间 [k*task_rows, min((k+1)*task_rows, rows))
```

表顺序由 `depends_on` 拓扑排序（父表在前），启动时校验：单根、无环、表名不重复、`fk_column` 存在于建表语句。

### 3.2 两级流式与背压

```
generate_data(ctx)  -> 逐行 yield tuple          （Python 生成器）
batched(rows, N)    -> 每 N 行 yield 一个 list     （islice，O(N) 内存）
```

写入一行批才拉下一批：`sink.write_batch(batch)` 返回后才 `next()`。**消费者拉动生产者，天然背压**，不需要有界队列、缓存池或“生产太快/消费太慢”的判定。一个 worker 同时最多持有一批数据，内存上限 = `workers × batch_size × 行宽`。

### 3.3 批量 INSERT

- MySQL：显式多值 `INSERT INTO t (cols) VALUES (%s),(%s),...`，SQL 模板按 `(表, 批行数)` 缓存，参数摊平成一个大 list，一次 `execute` 一次往返；捕获 1153（包过大）时对半拆批递归重试（最多 8 层）。
- SQLite：`executemany` + 单行占位符（SQLite 驱动不支持多值语句的批量绑定），SQL 按表缓存。
- 严格 `INSERT`，不用 `INSERT IGNORE`：主键冲突应该直接暴露，而不是静默吞掉。

### 3.4 并发与连接

```
ThreadPoolExecutor(max_workers=W)
  └─ 每个 worker：领一个任务 → 开一条连接（复用整个任务）→ 拓扑序流式写入 → COMMIT
```

- 并行单位是任务，不是表、不是批次：任务之间根表行号不重叠，键空间不重叠，无共享状态。
- 连接数 = worker 数，不做“每批新建连接”，也不做连接池排队。会话级设置只在开连接时执行一次：`unique_checks=1`、`foreign_key_checks` 可配、`innodb_lock_wait_timeout=600`、`net_*_timeout=600`。
- 生成是 CPU-bound（GIL 下多线程不加速），写入是 I/O-bound（等 socket 释放 GIL），所以 W 的收益来自写并发。起点 4，按 CPU 核数 × 2、`max_connections` 余量、redo 承受能力收敛。

### 3.5 事务、重试、续跑

- **一个任务 = 一个事务**：任务内所有表的全部批次同一事务，父先子后，最后一次 `COMMIT`。失败则整任务回滚，不会留下“父已提交、子未提交”的中间态。
- **重试单位 = 任务**：回滚后重开连接重跑；因为是同一 `(seed, 任务号)`，重放出的行逐字节一致。
- **断点 = 已完成任务集合**：每个任务提交成功后写 checkpoint（临时文件 + `os.replace` + `fsync`）。续跑前校验 signature（表名、依赖、DDL 哈希、`rows`、`task_rows`、`seed`），参数变了直接拒绝，避免“拿旧进度续新数据”。
- 重试策略：连接类 / 锁类按任务退避重试（`0.5s × 2^n`）；包过大拆批；列数错误等数据类问题不重试，直接判失败并记录。

### 3.6 校验

- 启动时（不连库）：拓扑、单根、表名唯一、`fk_column` 存在、`batch_size > 0`。
- 运行时（连库）：每行元组长度 == 建表列数；根表产出行数 == `ctx.size`；不一致则抛错 + 回滚。
- 运行后（`verify=True`）：根表 `COUNT(*)` == `rows`；每张子表做 `LEFT JOIN` 父表统计孤儿外键，必须为 0。

---

## 4. 插入方式对比（只在“连库 + 批量 INSERT”内取舍）

| 方案 | 往返次数 | 吞吐（相对） | 内存 | 可靠性 | 结论 |
| --- | --- | --- | --- | --- | --- |
| 逐行 `execute` | 每行 1 次 | 1× | 极低 | 好 | 排除，纯往返开销 |
| `cursor.executemany` | 取决于驱动版本 | 3~15× | 低 | 好 | 不作为默认：pymysql 1.0 之前是逐行往返，1.0+ 才合并简单 `INSERT`，不能把吞吐寄托在驱动版本上 |
| **显式多值 VALUES**（采用） | 每批 1 次 | 10~20× | 低（=1 批） | 好 | 不依赖驱动行为，一次往返，SQL 可缓存 |
| 单事务全量提交 | 1 次 | 高 | 高（undo/锁） | 差：崩溃回滚代价极大 | 不用 |
| 线程池并行写入 | 每批 1 次 × 并发 | 随并发线性到拐点 | 低 | 好（任务互不干扰） | **采用**，并发单位 = 任务 |

### 4.1 `executemany` 与多值 INSERT 的真实差异

- pymysql 1.0 之前：`executemany` 逐条 `execute`，每行一次往返，和逐行插入没有本质区别。
- pymysql 1.0+：只有 SQL 是单条 `INSERT ... VALUES (...)` 时才合并成多行，且按 `max_allowed_packet` 自行切分。
- 无论哪个版本，SQL 都由驱动在客户端拼装，批量上限和切分策略都不归我们控制。

V3 自己拼多值 SQL：模板按 `(表, 批行数)` 缓存，往返固定为“每批一次”，包大小由 `batch_size` 直接控制。代价是要自己处理 1153，用对半拆批兜住（§3.3）。

### 4.2 批量大小与提交粒度

- 批太小 → 往返次数上升；批太大 → 单语句解析时间、包大小、undo 一起上升，且失败重试粒度变粗。经验甜点 1k~5k 行/批（按行宽约 0.2~1 MB），默认 5,000。
- 提交粒度选“任务”而不是“每批”：每批一提交会出现跨表的中间态；整库一事务则回滚代价随数据量线性爆炸。任务大小（`task_rows`）就是这个取舍的旋钮，按 redo 容量和复制延迟调。

---

## 5. 可观测性

`run()` 返回 `RunResult`，并默认打印：

- 每表行数与批次数；
- 端到端耗时、端到端吞吐（行/s）；
- 峰值 Python 堆（`tracemalloc`，可用 `memory_profile=False` 关掉，避免测量本身约 2~5 倍的开销）；
- 任务耗时 p50 / p95、重试次数、拆批次数、失败任务列表；
- `verify=True` 时的校验明细。

日志默认每 10% 任务打印一次进度（`任务 k/K，累计 N 行，X 秒，R 行/s`）。

---

## 6. 实测数据与验证方法（本机、无 MySQL，SQLite / dry-run）

| 场景 | 数据量 | 耗时 | 吞吐 | 峰值 Python 堆 |
| --- | --- | --- | --- | --- |
| dry-run（只生成+分批，不开 tracemalloc） | 10,000,000 行 | 16.34s | 612k 行/s | — |
| dry-run（开 tracemalloc） | 10,000,000 行 | 83.08s | 120k 行/s | 15.8 MB |
| SQLite（真实 INSERT + 事务 + verify） | 1,000,000 行 | 10.88s | 91.9k 行/s | 7.3 MB |

SQLite 那一档的比例是 users 100k / orders 300k / order_items 600k，校验结果：

```
PASS 根表行数 users                期望 100000，实际 100000
PASS 孤儿外键 orders.user_id       孤儿行 0
PASS 孤儿外键 order_items.order_id 孤儿行 0
```

复现命令（在仓库根目录执行）：

```powershell
# 只验证生成与分批（不连库）
python -m MySQLScriptV3.example --backend dry-run --rows 30000

# 真实写入 SQLite 并校验（首次会自动建表）
python -m MySQLScriptV3.example --backend sqlite --rows 30000 --db "$env:TEMP\v3_demo.db" --wipe --verify

# 断点续跑：第一次跑完后，第二次加 --resume（应跳过全部任务、校验仍通过）
python -m MySQLScriptV3.example --backend sqlite --rows 30000 --db "$env:TEMP\v3_demo.db" `
  --checkpoint "$env:TEMP\v3_cp.json" --wipe
python -m MySQLScriptV3.example --backend sqlite --rows 30000 --db "$env:TEMP\v3_demo.db" `
  --checkpoint "$env:TEMP\v3_cp.json" --resume --verify

# MySQL（需要 pip install pymysql）
python -m MySQLScriptV3.example --backend mysql --rows 1000000 `
  --host localhost --user root --password ... --database performance_db --wipe --verify
```

MySQL 的写入吞吐要在目标机器上按 §4 的参数扫描（`batch_size` 1k/5k/10k，`workers` 1/4/8），本文的 SQLite 数字只用于证明生成与调度不自带瓶颈，不代表 MySQL 的绝对值。

---

## 7. 代码结构

```
MySQLScriptV3/
├── __init__.py    对外导出：Table / MySQLConfig / SQLiteConfig / RunResult / run
├── ddl.py         解析用户建表语句：列名、列顺序、主键（单列）
├── tables.py      Table、TableGraph（拓扑排序/校验）、GenContext、batched
├── sink.py        NullSink / SQLiteSink / MySQLSink：多值 INSERT、建表、清表、校验
├── engine.py      任务划分、线程池、任务事务、重试、拆批、checkpoint、指标
├── api.py         run(...) 主流程、连接配置、报告输出、RunResult
├── example.py     完整示例：users / orders / order_items + main
└── DESIGN.md      本文
```

用户实际要读的只有 `example.py` 的 DDL + 三个 `generate_data` + `main`。`ddl.py` / `tables.py` / `sink.py` / `engine.py` / `api.py` 是框架内部实现，不需要先理解才能用。

---

## 8. 明确不做（一期边界）

- **多列主键**：DDL 解析直接报错，提示只支持单列主键（外键校验与确定性键都依赖单列）。
- **多根表**：一次 `run` 只允许一个 `depends_on=None` 的根表；互不相关的表请分多次 `run`。
- **跨任务的父子引用**：子行只能引用同一任务内父表的行。这正是外键在单事务内闭合、任务可以并行和续跑的前提。
- **M:N 关联表**：需要用户自己在生成器里决定配对方式（例如按行号取模），框架不提供 join 语义。
- **跨库 / 分库分表**：不在本次范围内。

---

## 9. 取舍清单

1. **主键由用户生成**：V3 不读 AUTO_INCREMENT 回读值，也不替用户生成主键。用户在自己的 `generate_data` 里产出主键（示例用 `f"u{i}"`），重跑/重试/续跑得到同样的键，幂等才有锚点。
2. **子表靠“父批次入参”而不是“回读父表”**：换来零回读、零映射字典、生成与写入解耦；代价是子表行只能在当前任务的父行范围内分配。
3. **一个任务一个事务**：换来干净的重试/续跑语义；代价是单事务行数上限由 `task_rows` 控制，需要通过它来适配 redo/undo 和复制延迟。
4. **不做队列/缓存池**：任务自治（生成一批—写一批）已经提供背压与重叠，队列只增加调度和内存上限管理。
5. **`unique_checks` 保持开启**：主键唯一性是幂等/续跑的基础，不能像旧代码那样默认关掉。
6. **dry-run / SQLite 作为一等后端**：保证没有 MySQL 环境也能验证生成器与调度逻辑，不让人卡在环境上。
