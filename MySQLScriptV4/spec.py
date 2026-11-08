"""表定义、规格校验、执行计划。

用户（或 AI）只需要给出：
  DDL + 每张表的字段规则 + 父子关系 + 每父行几个子行
框架负责：
  列顺序/列数、外键取值、行数推导、分批、事务、并发、断点、校验
"""
from __future__ import annotations

import difflib
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from .ddl import Column, DdlError, TableInfo, parse_table
from .rules import SKIP

__all__ = ["Problem", "SpecError", "Table", "Spec", "Plan", "estimate_row_bytes"]

PerParent = Union[int, Tuple[int, int], Callable[..., int]]


@dataclass(frozen=True)
class Problem:
    level: str  # error / warning
    message: str
    table: str = ""
    column: str = ""
    hint: str = ""

    def __str__(self) -> str:
        where = ".".join(x for x in (self.table, self.column) if x)
        prefix = f"{where}: " if where else ""
        text = f"[{self.level.upper()}] {prefix}{self.message}"
        if self.hint:
            text += f"\n        提示：{self.hint}"
        return text

    def as_dict(self) -> dict:
        return {
            "level": self.level,
            "table": self.table,
            "column": self.column,
            "message": self.message,
            "hint": self.hint,
        }


class SpecError(RuntimeError):
    """规格有致命错误，无法生成计划。"""

    def __init__(self, problems: Sequence[Problem]) -> None:
        self.problems = [p for p in problems if p.level == "error"]
        super().__init__("; ".join(p.message for p in self.problems) or "spec error")


# ---------------------------------------------------------------------------
# 表
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Table:
    """一张表 = 建表语句 + 字段规则 + 父子关系。

    fields      列名 -> 规则；外键列不要写（框架自动取父行主键值）
    rows        根表总行数（只有根表需要）
    parent      父表对象（根表为 None）
    fk          子表里指向父表主键的列名
    per_parent  每个父行产出几个子行：整数 / (lo, hi) / fn(parent_row, rng) -> int
    batch_rows  覆盖该表的每批行数（默认按行宽自动算）
    """

    name: str
    ddl: str
    fields: Mapping[str, Any] = field(default_factory=dict)
    rows: Optional[int] = None
    parent: Optional["Table"] = field(default=None, compare=False)
    fk: Optional[str] = None
    per_parent: PerParent = 1
    batch_rows: Optional[int] = None
    note: str = ""

    info: TableInfo = field(init=False, compare=False, repr=False)
    active_columns: Tuple[Column, ...] = field(init=False, compare=False, repr=False)
    columns: Tuple[Tuple[Column, Optional[Any], bool], ...] = field(
        init=False, compare=False, repr=False
    )
    fk_index: Optional[int] = field(init=False, compare=False, repr=False)
    parent_pk_index: Optional[int] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        try:
            info = parse_table(self.ddl, self.name)
        except DdlError as exc:
            raise DdlError(f"表 {self.name}: {exc}") from exc
        object.__setattr__(self, "info", info)
        if self.batch_rows is not None and self.batch_rows <= 0:
            raise ValueError(f"{self.name}.batch_rows 必须 > 0")

        fields = dict(self.fields)
        active = tuple(c for c in info.insert_columns if fields.get(c.name) is not SKIP)
        object.__setattr__(self, "active_columns", active)
        columns = []
        for col in active:
            is_fk = self.fk is not None and col.name == self.fk
            columns.append((col, None if is_fk else fields.get(col.name), is_fk))
        object.__setattr__(self, "columns", tuple(columns))

        # 外键取值的下标预先算好：每行只做一次 tuple 索引，不做字典查找
        fk_index: Optional[int] = None
        parent_pk_index: Optional[int] = None
        if self.parent is not None and self.fk:
            names = [c.name for c in active]
            if self.fk in names:
                fk_index = names.index(self.fk)
            pk = self.parent.info.primary_key
            if pk and pk in self.parent.column_names:
                parent_pk_index = self.parent.column_names.index(pk)
        object.__setattr__(self, "fk_index", fk_index)
        object.__setattr__(self, "parent_pk_index", parent_pk_index)

    # -- 便捷属性 ---------------------------------------------------------
    @property
    def primary_key(self) -> Optional[str]:
        return self.info.primary_key

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(c.name for c in self.active_columns)

    @property
    def is_root(self) -> bool:
        return self.parent is None

    def per_parent_value(self, parent_row, rng) -> int:
        pp = self.per_parent
        if isinstance(pp, int) and not isinstance(pp, bool):
            return pp
        if isinstance(pp, (tuple, list)):
            return rng.randint(int(pp[0]), int(pp[1]))
        if callable(pp):
            value = pp(parent_row, rng)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(
                    f"{self.name}.per_parent 返回了 {value!r}，必须是 >= 0 的整数"
                )
            return value
        raise ValueError(f"{self.name}.per_parent 写法不支持：{pp!r}")


# ---------------------------------------------------------------------------
# 规格
# ---------------------------------------------------------------------------


class Spec:
    def __init__(
        self,
        tables: Sequence[Table],
        *,
        seed: str = "v4",
        version: str = "1",
        title: str = "",
    ) -> None:
        self.tables: Tuple[Table, ...] = tuple(tables)
        self.seed = str(seed)
        self.version = str(version)
        self.title = title or "spec"
        self.problems: List[Problem] = []
        self.by_name: Dict[str, Table] = {}
        self.children: Dict[str, List[Table]] = {}
        self.root: Optional[Table] = None
        self.order: Tuple[Table, ...] = ()
        self._check()

    # -- 校验 -------------------------------------------------------------
    def _error(self, message: str, table: str = "", column: str = "", hint: str = "") -> None:
        self.problems.append(Problem("error", message, table, column, hint))

    def _warn(self, message: str, table: str = "", column: str = "", hint: str = "") -> None:
        self.problems.append(Problem("warning", message, table, column, hint))

    def _check(self) -> None:
        if not self.tables:
            self._error("TABLES 是空的", hint="至少定义一张根表")
            return

        names = [t.name for t in self.tables]
        for raw in names:
            if not isinstance(raw, str) or not raw:
                self._error("表名必须是非空字符串")
        for name in sorted({n for n in names if names.count(n) > 1}):
            self._error(f"表名重复：{name}")
        for table in self.tables:
            if not isinstance(table, Table):
                self._error(f"TABLES 里出现了非 Table 对象：{type(table).__name__}")
        if any(p.level == "error" for p in self.problems):
            return

        self.by_name = {t.name: t for t in self.tables}
        for table in self.tables:
            if table.name.startswith("_dataforge"):
                self._error("表名不能以 _dataforge 开头（框架进度表保留前缀）", table.name)

        # 根表
        roots = [t for t in self.tables if t.parent is None]
        if len(roots) != 1:
            self._error(
                f"一次运行必须有且仅有一个根表（parent=None），当前有 {len(roots)} 个："
                f"{[t.name for t in roots]}",
                hint="互不相关的表请分成多个 spec 文件分别运行",
            )
            return
        self.root = roots[0]

        # 父表必须是本次 TABLES 里的对象
        for table in self.tables:
            parent = table.parent
            if parent is None:
                continue
            if not isinstance(parent, Table):
                self._error(f"parent={parent!r} 不是 Table 对象", table.name)
                continue
            target = self.by_name.get(parent.name)
            if target is None or target is not parent:
                self._error(
                    f"parent 指向的表 {parent.name!r} 不在本次 TABLES 里（或不是同一个对象）",
                    table.name,
                )

        # 环
        for table in self.tables:
            seen = set()
            cur: Optional[Table] = table
            chain = []
            while cur is not None:
                if id(cur) in seen:
                    chain.append(cur.name)
                    self._error(f"表依赖成环：{' -> '.join(chain)}")
                    break
                seen.add(id(cur))
                chain.append(cur.name)
                cur = cur.parent

        self.children = {t.name: [] for t in self.tables}
        for table in self.tables:
            if table.parent is not None and self.by_name.get(table.parent.name) is table.parent:
                self.children[table.parent.name].append(table)

        # 拓扑序（父在前）
        ordered: List[Table] = []
        frontier = [self.root]
        while frontier:
            nxt: List[Table] = []
            for table in frontier:
                ordered.append(table)
                nxt.extend(self.children.get(table.name, []))
            frontier = nxt
        reached = {t.name for t in ordered}
        missing = [t.name for t in self.tables if t.name not in reached]
        if missing:
            self._error(
                f"这些表无法从根表 {self.root.name} 到达：{missing}",
                hint="检查 parent 是否接错了",
            )
        self.order = tuple(ordered) if not missing else ()

        for table in self.tables:
            self._check_table(table)

    def _check_table(self, table: Table) -> None:
        info = table.info
        cols = {c.name: c for c in info.columns}
        fields = dict(table.fields)
        pk = info.primary_key

        if table.parent is None:
            if table.rows is None:
                self._error("根表必须设置 rows", table.name, hint="rows=1_000_000")
            elif not isinstance(table.rows, int) or isinstance(table.rows, bool) or table.rows < 1:
                self._error(f"rows 必须是 >= 1 的整数，收到 {table.rows!r}", table.name)
            if table.fk:
                self._error("根表不需要 fk", table.name)
            if isinstance(table.per_parent, int) and table.per_parent != 1:
                self._warn("根表的 per_parent 不生效", table.name)
        else:
            if table.rows is not None:
                self._error(
                    "子表的行数由 父表行数 × per_parent 推导，不要再写 rows", table.name
                )
            if not table.fk:
                self._error(
                    "子表必须声明 fk（子表里指向父表主键的列名）",
                    table.name,
                    hint=f'fk="{info.column_names[0]}" 之类',
                )
            elif table.fk not in cols:
                self._error(
                    f"fk={table.fk!r} 不在建表语句的列里，可用列：{list(cols)}", table.name
                )
            else:
                self._check_fk_type(table, cols[table.fk])

        # 每个 per_parent 写法
        pp = table.per_parent
        if isinstance(pp, bool):
            self._error("per_parent 不支持 bool", table.name)
        elif isinstance(pp, int):
            if pp < 0:
                self._error(f"per_parent 不能为负数：{pp}", table.name)
        elif isinstance(pp, (tuple, list)):
            if len(pp) != 2 or not all(isinstance(x, int) and not isinstance(x, bool) for x in pp):
                self._error(f"per_parent 区间必须是 (lo, hi) 两个整数，收到 {pp!r}", table.name)
            elif pp[0] < 0 or pp[0] > pp[1]:
                self._error(f"per_parent 区间不合法：{pp!r}", table.name)
        elif not callable(pp):
            self._error(
                f"per_parent 只能是整数、(lo, hi) 或函数，收到 {pp!r}", table.name
            )

        # 字段覆盖
        for key in fields:
            if key not in cols:
                close = difflib.get_close_matches(key, list(cols), n=2)
                hint = f"是不是想写 {close}？" if close else f"可用列：{list(cols)}"
                self._error(f"未知列 {key!r}", table.name, key, hint)
                continue
            col = cols[key]
            if col.generated:
                self._error(
                    f"{key} 是生成列（GENERATED/AS），数据库会自己算，不能赋值",
                    table.name,
                    key,
                    hint="从 fields 里删掉这一列",
                )
            if key == table.fk:
                self._error(
                    f"{key} 是外键列，值由框架从父行主键自动填充",
                    table.name,
                    key,
                    hint="从 fields 里删掉这一列",
                )
            if key == pk and fields[key] is SKIP:
                self._error("主键不能 SKIP", table.name, key)
            if fields[key] is not SKIP and not callable(fields[key]):
                self._error(
                    f"规则必须是可调用对象，收到 {type(fields[key]).__name__}",
                    table.name,
                    key,
                    hint=f'常量写成 const({fields[key]!r})，或用 lambda r: ...',
                )

        for col in table.active_columns:
            if col.name == table.fk:
                continue
            if col.name not in fields:
                hint = (
                    f'"{col.name}": SKIP  # 用数据库默认值'
                    if col.has_default or not col.not_null
                    else f'"{col.name}": <规则>'
                )
                self._error(
                    f"缺少列 {col.name} {col.sql_type} 的取值规则",
                    table.name,
                    col.name,
                    hint,
                )
        for col in info.insert_columns:
            if fields.get(col.name) is not SKIP:
                continue
            if col.name == table.fk:
                self._error("外键列不能 SKIP", table.name, col.name)
            if col.name == pk:
                self._error("主键不能 SKIP", table.name, col.name)

        if pk is None:
            self._warn(
                "建表语句没有主键：无法用主键做幂等/去重校验，续跑也可能重复",
                table.name,
                hint="建议 DDL 里声明 PRIMARY KEY",
            )

    def _check_fk_type(self, table: Table, child_col: Column) -> None:
        parent = table.parent
        if parent is None:
            return
        parent_pk = parent.info.primary_key
        if not parent_pk:
            self._error(
                f"父表 {parent.name} 没有主键，无法确定 {table.fk} 该填什么",
                table.name,
                hint="给父表加 PRIMARY KEY",
            )
            return
        parent_col = parent.info.column(parent_pk)
        if child_col.kind in ("text", "enum") and parent_col.kind in ("text", "enum"):
            if child_col.length and parent_col.length and child_col.length < parent_col.length:
                self._error(
                    f"外键列 {child_col.sql_type} 比父表主键 {parent_col.sql_type} 短，会被截断",
                    table.name,
                    child_col.name,
                )
        elif child_col.kind != parent_col.kind:
            if {child_col.kind, parent_col.kind} & {"int", "text", "decimal", "temporal"}:
                self._warn(
                    f"外键列类型 {child_col.sql_type} 与父表主键 {parent_col.sql_type} 不一致",
                    table.name,
                    child_col.name,
                )

    # -- 计划 -------------------------------------------------------------
    def plan(
        self,
        rows: Optional[int] = None,
        *,
        task_rows: int = 10_000,
        packet_bytes: int = 4 * 1024 * 1024,
        max_batch_rows: int = 5_000,
        sample: int = 3,
    ) -> "Plan":
        if not self.order or self.root is None:
            raise SpecError([p for p in self.problems if p.level == "error"] or self.problems)
        if task_rows < 1:
            raise ValueError("task_rows 必须 >= 1")
        root_rows = self.root.rows if rows is None else rows
        if not isinstance(root_rows, int) or isinstance(root_rows, bool) or root_rows < 1:
            raise ValueError(f"root rows 必须是 >= 1 的整数，收到 {root_rows!r}")

        rows_by_table: Dict[str, Optional[int]] = {}
        for table in self.order:
            if table.parent is None:
                rows_by_table[table.name] = root_rows
            else:
                parent_rows = rows_by_table.get(table.parent.name)
                pp = table.per_parent
                if parent_rows is not None and isinstance(pp, int) and not isinstance(pp, bool):
                    rows_by_table[table.name] = parent_rows * pp
                else:
                    rows_by_table[table.name] = None

        bytes_by_table = {t.name: estimate_row_bytes(t) for t in self.order}
        batch_rows = {
            t.name: t.batch_rows
            or max(1, min(max_batch_rows, packet_bytes // max(1, bytes_by_table[t.name])))
            for t in self.order
        }

        plan = Plan(
            spec=self,
            root_rows=root_rows,
            task_rows=task_rows,
            tasks=math.ceil(root_rows / task_rows),
            order=self.order,
            rows_by_table=rows_by_table,
            bytes_by_table=bytes_by_table,
            batch_rows=batch_rows,
            sample=sample,
        )
        plan.problems = list(self.problems)
        if any(p.level == "error" for p in plan.problems):
            return plan
        try:
            from .gen import collect_samples, probe_pk_collisions

            plan.samples, sample_problems = collect_samples(self, plan, sample)
            plan.problems.extend(sample_problems)
            if not sample_problems:
                plan.problems.extend(probe_pk_collisions(self, plan))
        except Exception as exc:  # noqa: BLE001 - 规则本身出错也要变成可读问题
            plan.problems.append(
                Problem(
                    "error",
                    f"抽样生成失败：{type(exc).__name__}: {exc}",
                    hint="检查字段规则是否能在没有数据库的情况下生成一行",
                )
            )
        return plan


# ---------------------------------------------------------------------------
# 计划
# ---------------------------------------------------------------------------


_KIND_BYTES = {
    "int": 8,
    "bool": 1,
    "float": 8,
    "decimal": 12,
    "temporal": 19,
    "text": 32,
    "blob": 40,
    "json": 40,
    "enum": 8,
    "other": 24,
}


def _column_bytes(column: Column) -> int:
    if column.kind == "text":
        return min(column.length or 32, 64) + 2
    if column.kind == "enum":
        return max((len(v) for v in column.enum_values), default=8) + 2
    return _KIND_BYTES.get(column.kind, 24)


def estimate_row_bytes(table: Table) -> int:
    return 8 + sum(_column_bytes(c) for c in table.active_columns)


@dataclass
class Plan:
    spec: Spec
    root_rows: int
    task_rows: int
    tasks: int
    order: Tuple[Table, ...]
    rows_by_table: Dict[str, Optional[int]]
    bytes_by_table: Dict[str, int]
    batch_rows: Dict[str, int]
    sample: int
    problems: List[Problem] = field(default_factory=list)
    samples: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)

    @property
    def errors(self) -> List[Problem]:
        return [p for p in self.problems if p.level == "error"]

    @property
    def warnings(self) -> List[Problem]:
        return [p for p in self.problems if p.level == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def total_rows(self) -> Optional[int]:
        values = list(self.rows_by_table.values())
        return None if any(v is None for v in values) else sum(v for v in values if v)

    @property
    def total_bytes(self) -> Optional[int]:
        total = 0
        for name, rows in self.rows_by_table.items():
            if rows is None:
                return None
            total += rows * self.bytes_by_table[name]
        return total

    def expected_in_task(self, table: Table, root_rows_in_task: int) -> Optional[int]:
        if table.parent is None:
            return root_rows_in_task
        parent = self.expected_in_task(table.parent, root_rows_in_task)
        if parent is None:
            return None
        pp = table.per_parent
        if isinstance(pp, int) and not isinstance(pp, bool):
            return parent * pp
        return None

    def table_rows(self, name: str) -> Optional[int]:
        return self.rows_by_table.get(name)
