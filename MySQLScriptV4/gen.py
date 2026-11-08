"""流式生成：任务内按拓扑序把行按批吐出来。

约定：
  * 一个任务 = 根表的一段行区间 = 一个事务 = 一个并行/续跑单位；
  * 父行这一批写完后，子表就用这一批父行生成，外键直接从父行取，天然不会悬空；
  * 每张表在任务内的随机流只播种一次（seed|表名|任务号），所以
    - 重跑/重试得到相同数据；
    - 改 batch_size / workers 不改变数据（只有 task_rows 会改变任务边界）。
"""
from __future__ import annotations

import random
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from .spec import Plan, Problem, Spec, Table

__all__ = [
    "DataError",
    "Row",
    "ParentView",
    "TaskGenerator",
    "validate_value",
    "collect_samples",
    "probe_pk_collisions",
]


class DataError(ValueError):
    """生成出来的数据不满足建表约束（列宽、枚举、NOT NULL……）。"""


class ParentView(Mapping):
    """父行按列名访问；每行复用同一个对象，避免 10M 次字典分配。"""

    __slots__ = ("_row", "_index", "_names")

    def __init__(self, names: Sequence[str]) -> None:
        self._names = tuple(names)
        self._index = {name: i for i, name in enumerate(names)}
        self._row: Tuple[Any, ...] = ()

    def bind(self, row: Tuple[Any, ...]) -> "ParentView":
        self._row = row
        return self

    def __getitem__(self, key) -> Any:
        if isinstance(key, int):
            return self._row[key]
        try:
            return self._row[self._index[key]]
        except KeyError:
            raise KeyError(
                f"父行没有列 {key!r}；可用列：{list(self._names)}"
            ) from None

    def __iter__(self):
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    @property
    def raw(self) -> Tuple[Any, ...]:
        return self._row

    def as_dict(self) -> Dict[str, Any]:
        return dict(zip(self._names, self._row))

    def __repr__(self) -> str:
        return f"ParentView({self.as_dict()})"


class Row:
    """传入字段规则的行上下文；生成过程中复用同一个实例。"""

    __slots__ = ("i", "k", "task", "table", "parent", "rng", "seed")

    def __init__(self) -> None:
        self.i = 0
        self.k = 0
        self.task = 0
        self.table = ""
        self.parent: Mapping[str, Any] = ParentView(())
        self.rng = random.Random(0)
        self.seed = ""

    def __repr__(self) -> str:  # 方便用户调试
        return f"Row(table={self.table!r}, i={self.i}, k={self.k}, task={self.task})"


_EMPTY_PARENT = ParentView(())


def validate_value(table: Table, column, value: Any) -> None:
    """单值校验：类型、字符长度、枚举、NOT NULL、小数位。"""
    where = f"{table.name}.{column.name}"
    if value is None:
        if column.not_null:
            raise DataError(f"{where} 声明了 NOT NULL，但规则返回了 None")
        return
    kind = column.kind
    if kind == "text":
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise DataError(f"{where} 需要字符串，实际是 {type(value).__name__}: {value!r}")
        if isinstance(value, str) and column.length and len(value) > column.length:
            raise DataError(
                f"{where} 是 {column.sql_type}，值 {value!r} 有 {len(value)} 个字符（上限 {column.length}）"
            )
    elif kind == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            raise DataError(f"{where} 需要整数，实际是 {type(value).__name__}: {value!r}")
        if column.unsigned and value < 0:
            raise DataError(f"{where} 是 UNSIGNED，不能写负数 {value}")
    elif kind == "bool":
        if not isinstance(value, (bool, int)):
            raise DataError(f"{where} 需要 0/1，实际是 {type(value).__name__}: {value!r}")
    elif kind == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise DataError(f"{where} 需要数字，实际是 {type(value).__name__}: {value!r}")
    elif kind == "decimal":
        if not isinstance(value, (Decimal, int, float)) or isinstance(value, bool):
            raise DataError(f"{where} 需要 Decimal/数字，实际是 {type(value).__name__}: {value!r}")
        number = Decimal(str(value))
        if column.scale is not None:
            exponent = -number.as_tuple().exponent
            if exponent > column.scale:
                raise DataError(
                    f"{where} 是 {column.sql_type}，值 {value!r} 的小数位超过 {column.scale}"
                )
            if column.precision is not None:
                integer_limit = Decimal(10) ** (column.precision - column.scale)
                if abs(number) >= integer_limit:
                    raise DataError(
                        f"{where} 是 {column.sql_type}，值 {value!r} 的整数位超过 "
                        f"{column.precision - column.scale} 位"
                    )
    elif kind == "temporal":
        if not isinstance(value, (datetime, date, str)):
            raise DataError(
                f"{where} 需要 datetime/date/字符串，实际是 {type(value).__name__}: {value!r}"
            )
    elif kind == "enum":
        if column.enum_values and value not in column.enum_values:
            raise DataError(
                f"{where} 的枚举值 {value!r} 不在 {list(column.enum_values)} 里"
            )
    elif kind == "blob":
        if not isinstance(value, (bytes, bytearray, str)):
            raise DataError(f"{where} 需要 bytes，实际是 {type(value).__name__}")
    elif kind == "json":
        if not isinstance(value, (str, bytes, dict, list, int, float)):
            raise DataError(f"{where} 需要 JSON 字符串或 dict/list，实际是 {type(value).__name__}")


def build_row(table: Table, row: Row, check: bool = False) -> Tuple[Any, ...]:
    """通用构建器：逐列 call，出错时能指出列名。检查首行/兜底时用。"""
    values: List[Any] = []
    columns = table.columns
    parent = row.parent
    pk_index = table.parent_pk_index
    for column, rule, is_fk in columns:
        if is_fk:
            value = parent.raw[pk_index]  # type: ignore[index]
        else:
            try:
                value = rule(row)  # type: ignore[misc]
            except DataError:
                raise
            except Exception as exc:  # noqa: BLE001 - 规则是用户代码
                raise DataError(
                    f"{table.name}.{column.name}: 规则执行失败 "
                    f"({type(exc).__name__}: {exc})"
                ) from exc
        if check:
            validate_value(table, column, value)
        values.append(value)
    return tuple(values)


# 每张表生成一个专用的行构建函数：省掉逐列循环和分支，10M 行级别差别明显
_BUILDERS: Dict[int, Any] = {}


def builder_for(table: Table):
    entry = _BUILDERS.get(id(table))
    if entry is not None:
        reference, cached = entry
        if reference() is table:  # id 可能被复用，必须确认还是同一张表
            return cached
    rules: List[Any] = []
    parts: List[str] = []
    for column, rule, is_fk in table.columns:
        if is_fk:
            parts.append(f"p[{table.parent_pk_index}]")
        else:
            index = len(rules)
            rules.append(rule)
            parts.append(f"r[{index}](row)")
    body = ", ".join(parts)
    if len(parts) == 1:
        body += ","
    source = f"def _build(row, p, r=rules):\n    return ({body})\n"
    namespace: Dict[str, Any] = {"rules": tuple(rules)}
    exec(source, namespace)  # 模板是我们自己拼的，不含用户字符串
    builder = namespace["_build"]
    import weakref

    _BUILDERS[id(table)] = (weakref.ref(table), builder)
    return builder


class TaskGenerator:
    """生成一个任务里的所有批次，按 父 → 子 → 孙 的顺序 yield (table, rows)。"""

    def __init__(
        self,
        spec: Spec,
        plan: Plan,
        task: int,
        start: int,
        end: int,
        *,
        seed: Optional[str] = None,
        check: str = "first",
        batch_rows: Optional[int] = None,
        max_batch_rows: Optional[int] = None,
    ) -> None:
        self.spec = spec
        self.plan = plan
        self.task = task
        self.start = start
        self.end = end
        self.seed = spec.seed if seed is None else seed
        self.check = check
        self.batch_override = batch_rows
        self.max_batch_rows = max_batch_rows
        # 每张表一个 Row：父/子生成器嵌套时互不覆盖（共享一个 Row 会串 rng / parent）
        self._rows = {t.name: Row() for t in spec.order}
        self._rng = {
            t.name: random.Random(f"{self.seed}|{t.name}|{task}") for t in spec.order
        }
        self._idx = {t.name: 0 for t in spec.order}
        self._checked: set = set()
        self._views = {t.name: ParentView(t.column_names) for t in spec.order}

    # -- 对外 -------------------------------------------------------------
    def iter_batches(self) -> Iterator[Tuple[Table, List[Tuple[Any, ...]]]]:
        if self.spec.root is None:
            return
        yield from self._walk(self.spec.root, None)

    # -- 内部 -------------------------------------------------------------
    def _batch_size(self, table: Table) -> int:
        size = self.batch_override or self.plan.batch_rows[table.name]
        if self.max_batch_rows:
            size = min(size, self.max_batch_rows)
        return max(1, size)

    def _walk(
        self,
        table: Table,
        parent_batches: Optional[List[List[Tuple[Any, ...]]]],
    ) -> Iterator[Tuple[Table, List[Tuple[Any, ...]]]]:
        for batch in self._own_batches(table, parent_batches):
            yield table, batch
            for child in self.spec.children[table.name]:
                yield from self._walk(child, [batch])

    def _own_batches(
        self, table: Table, parent_batches: Optional[List[List[Tuple[Any, ...]]]]
    ) -> Iterator[List[Tuple[Any, ...]]]:
        size = max(1, self._batch_size(table))
        row = self._rows[table.name]
        rng = self._rng[table.name]
        row.table = table.name
        row.task = self.task
        row.seed = self.seed
        row.rng = rng

        do_check = self.check == "all" or (
            self.check == "first" and table.name not in self._checked
        )
        if do_check:
            self._checked.add(table.name)

        buffer: List[Tuple[Any, ...]] = []
        build = builder_for(table)
        if table.parent is None:
            row.parent = _EMPTY_PARENT
            row.k = 0
            for i in range(self.start, self.end):
                row.i = i
                if do_check:
                    buffer.append(build_row(table, row, True))
                    if self.check == "first":
                        do_check = False
                else:
                    try:
                        buffer.append(build(row, ()))
                    except Exception:  # noqa: BLE001 - 用慢路径给出带列名的错误
                        build_row(table, row, True)
                        raise
                if len(buffer) >= size:
                    yield buffer
                    buffer = []
        else:
            assert table.parent is not None
            view = self._views[table.parent.name]
            index = self._idx[table.name]
            row.parent = view
            for parent_batch in parent_batches or ():
                for parent_row in parent_batch:
                    view.bind(parent_row)
                    raw = view.raw
                    count = table.per_parent_value(view, rng)
                    for k in range(count):
                        row.i = index
                        row.k = k
                        if do_check:
                            buffer.append(build_row(table, row, True))
                            if self.check == "first":
                                do_check = False
                        else:
                            try:
                                buffer.append(build(row, raw))
                            except Exception:  # noqa: BLE001
                                build_row(table, row, True)
                                raise
                        index += 1
                        if len(buffer) >= size:
                            yield buffer
                            buffer = []
            self._idx[table.name] = index
        if buffer:
            yield buffer


# ---------------------------------------------------------------------------
# 计划阶段的抽样与探针
# ---------------------------------------------------------------------------


def collect_samples(
    spec: Spec, plan: Plan, count: int
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Problem]]:
    """在每个表上各生成 count 行样例，顺便做完整校验。"""
    samples: Dict[str, List[Dict[str, Any]]] = {}
    problems: List[Problem] = []
    end = min(plan.root_rows, max(1, count))
    generator = TaskGenerator(
        spec, plan, task=0, start=0, end=end, check="all", batch_rows=max(1, count)
    )
    try:
        for table, batch in generator.iter_batches():
            bucket = samples.setdefault(table.name, [])
            if len(bucket) >= count:
                continue
            for row in batch:
                if len(bucket) >= count:
                    break
                bucket.append(dict(zip(table.column_names, row)))
    except DataError as exc:
        problems.append(Problem("error", str(exc), hint="修正该列规则后重试"))
    except Exception as exc:  # noqa: BLE001
        problems.append(
            Problem("error", f"生成样例失败：{type(exc).__name__}: {exc}")
        )
    return samples, problems


def probe_pk_collisions(spec: Spec, plan: Plan) -> List[Problem]:
    """跨任务探测主键是否会重复（最常见的错误是用子表的 r.i 组键）。"""
    if plan.tasks < 2:
        return []
    targets = {0, 1, plan.tasks // 2, plan.tasks - 1}
    starts = sorted({t for t in targets if 0 <= t < plan.tasks})
    problems: List[Problem] = []
    for table in spec.order:
        pk = table.primary_key
        if not pk:
            continue
        try:
            pk_index = table.column_names.index(pk)
        except ValueError:
            continue
        seen: Dict[Any, int] = {}
        for task in starts:
            start = task * plan.task_rows
            end = min(start + 1, plan.root_rows)
            generator = TaskGenerator(
                spec, plan, task=task, start=start, end=end, check="none", batch_rows=1
            )
            first = None
            try:
                for current, batch in generator.iter_batches():
                    if current.name == table.name:
                        first = batch[0]
                        break
            except Exception as exc:  # noqa: BLE001 - 抽样失败已经在别处报
                problems.append(
                    Problem("error", f"跨任务抽样失败：{type(exc).__name__}: {exc}", table.name)
                )
                break
            if first is None:
                continue
            value = first[pk_index]
            if value in seen and seen[value] != task:
                problems.append(
                    Problem(
                        "error",
                        f"主键 {pk} 在不同任务里重复：任务 {seen[value]} 和任务 {task} 都生成了 {value!r}",
                        table.name,
                        pk,
                        hint="子表的 r.i 是任务内序号；请用父行主键 + r.k 组键，例如 lambda r: f\"{r.parent['id']}-{r.k}\"",
                    )
                )
                break
            seen[value] = task
    return problems
