"""用户侧模型：Table（建表语句 + generate_data）、表依赖图、生成上下文。"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from itertools import islice
from typing import Callable, Iterable, Iterator, Optional, Sequence

from .ddl import DdlInfo, parse_ddl


@dataclass(frozen=True)
class Table:
    """一张表 = 建表语句 + 一个 yield 生成器。

    generate_data 的签名：
      - 根表：def generate_data(ctx) -> Iterator[tuple]
      - 子表：def generate_data(ctx, parent_rows) -> Iterator[tuple]
    每行元组按建表语句里列出现的顺序给出。
    """

    name: str
    ddl: str
    generate_data: Callable[..., Iterator[tuple]]
    depends_on: Optional[str] = None
    fk_column: Optional[str] = None
    batch_size: Optional[int] = None
    info: DdlInfo = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        info = parse_ddl(self.ddl, self.name)
        object.__setattr__(self, "info", info)
        if self.batch_size is not None and self.batch_size <= 0:
            raise ValueError(f"{self.name}.batch_size 必须 > 0")
        if self.depends_on is None:
            if self.fk_column:
                raise ValueError(f"{self.name} 没有 depends_on，不应声明 fk_column")
            return
        if not self.fk_column:
            raise ValueError(
                f"子表 {self.name} 需要声明 fk_column（例如 fk_column='user_id'），用于外键校验"
            )
        if self.fk_column not in info.column_names:
            raise ValueError(f"{self.name} 的 fk_column={self.fk_column} 不在建表语句的列里")

    @property
    def columns(self) -> tuple[str, ...]:
        return self.info.column_names

    @property
    def primary_key(self) -> Optional[str]:
        return self.info.primary_key


@dataclass(frozen=True)
class TableGraph:
    tables: tuple[Table, ...]  # 拓扑序：父表在前
    children: dict[str, tuple[str, ...]]
    root: Table

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    def child_tables(self, name: str) -> tuple[Table, ...]:
        return tuple(self.table(child) for child in self.children.get(name, ()))


def build_graph(tables: Sequence[Table]) -> TableGraph:
    if not tables:
        raise ValueError("至少需要一张表")
    names = [t.name for t in tables]
    if len(set(names)) != len(names):
        raise ValueError(f"表名重复: {names}")

    roots = [t for t in tables if t.depends_on is None]
    if len(roots) != 1:
        raise ValueError(
            "一次 run 只支持一个根表（depends_on 为空的表）；当前根表: "
            f"{[t.name for t in roots]}。多组无关的表请分多次 run。"
        )
    by_name = {t.name: t for t in tables}

    # 环检测
    for t in tables:
        seen: list[str] = []
        cur: Optional[str] = t.name
        while cur is not None:
            if cur in seen:
                raise ValueError(f"表依赖存在环: {' -> '.join(seen + [cur])}")
            seen.append(cur)
            rel = by_name.get(cur)
            if rel is None:
                raise ValueError(f"{seen[-2]} 的 depends_on={cur} 不存在")
            cur = rel.depends_on

    children: dict[str, list[str]] = {t.name: [] for t in tables}
    for t in tables:
        if t.depends_on:
            children[t.depends_on].append(t.name)

    ordered: list[Table] = []
    frontier = [roots[0].name]
    while frontier:
        nxt: list[str] = []
        for name in frontier:
            ordered.append(by_name[name])
            nxt.extend(children[name])
        frontier = nxt
    if len(ordered) != len(tables):
        missing = [n for n in names if n not in {t.name for t in ordered}]
        raise ValueError(f"存在无法从根表到达的表: {missing}")

    return TableGraph(
        tables=tuple(ordered),
        children={k: tuple(v) for k, v in children.items()},
        root=roots[0],
    )


@dataclass
class GenContext:
    """传给 generate_data 的上下文。

    ctx.start / ctx.end 是根表行号区间：根表生成器必须按这个区间产出，
    框架才能把一个区间当成一个任务（一个事务、一次提交、一个续跑单位）。
    """

    table_name: str
    start: int
    end: int
    seed: str
    task: int
    _rng: Optional[random.Random] = field(default=None, repr=False, compare=False)

    @property
    def size(self) -> int:
        return self.end - self.start

    @property
    def rng(self) -> random.Random:
        """按 (seed, 表名, 任务号) 播种：同一任务重跑得到相同数据。"""
        if self._rng is None:
            self._rng = random.Random(f"{self.seed}|{self.table_name}|{self.task}")
        return self._rng


def batched(rows: Iterable[tuple], size: int) -> Iterator[list[tuple]]:
    if size <= 0:
        raise ValueError("batch_size 必须 > 0")
    it = iter(rows)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk
