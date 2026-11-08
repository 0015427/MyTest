"""字段规则库：列值 = 一个可调用对象 (row) -> value。

规则本身是普通的可调用对象，所以既能用内置工厂，也能直接写 lambda：
    "id":       lambda r: f"u{r.i}"
    "country":  pick(["CN", "US", "JP"])
    "amount":   money(10, 500)
    "created":  ts(BASE, span_seconds=86400)

所有随机都走 row.rng（按 seed/表/任务播种），不要用全局 random，
否则同一任务重跑/续跑得不到相同数据。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Optional, Tuple

__all__ = [
    "Rule",
    "describe",
    "source_at",
    "SKIP",
    "pick",
    "between",
    "real",
    "money",
    "ts",
    "dates",
    "uuid4",
    "token",
    "const",
    "maybe",
    "boolean",
]


class _Skip:
    """放在 fields 里表示“这一列不写，交给数据库默认值”。"""

    __slots__ = ()

    def __repr__(self) -> str:
        return "SKIP"

    def __bool__(self) -> bool:  # 防止误用
        return False


SKIP = _Skip()


class Rule:
    """规则基类：子类实现 __call__，并给出可读的 describe()。"""

    def __call__(self, row) -> Any:  # pragma: no cover - 抽象
        raise NotImplementedError

    def describe(self) -> str:
        return repr(self)


# ---------------------------------------------------------------------------
# 随机取值
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class pick(Rule):
    """从候选值里按权重取一个。"""

    values: Tuple[Any, ...]
    weights: Optional[Tuple[float, ...]] = None
    _cumulative: Tuple[float, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        values = tuple(self.values)
        if not values:
            raise ValueError("pick() 至少需要一个候选值")
        weights = None if self.weights is None else tuple(float(w) for w in self.weights)
        if weights is not None:
            if len(weights) != len(values):
                raise ValueError("pick() 的 weights 与 values 长度不一致")
            if any(w < 0 for w in weights) or sum(weights) <= 0:
                raise ValueError("pick() 的 weights 必须非负且总和 > 0")
            cum: list = []
            total = 0.0
            for w in weights:
                total += w
                cum.append(total)
            weights = tuple(weights)
            cumulative = tuple(cum)
        else:
            cumulative = tuple(float(i + 1) for i in range(len(values)))
        object.__setattr__(self, "values", values)
        object.__setattr__(self, "weights", weights)
        object.__setattr__(self, "_cumulative", cumulative)

    def __call__(self, row) -> Any:
        total = self._cumulative[-1]
        x = row.rng.random() * total
        for i, edge in enumerate(self._cumulative):
            if x < edge:
                return self.values[i]
        return self.values[-1]

    def describe(self) -> str:
        shown = ", ".join(repr(v) for v in self.values[:4])
        if len(self.values) > 4:
            shown += f", …({len(self.values)})"
        weight = "" if self.weights is None else " weighted"
        return f"pick({shown}{weight})"


@dataclass(frozen=True)
class between(Rule):
    """闭区间随机整数。"""

    lo: int
    hi: int

    def __post_init__(self) -> None:
        if self.lo > self.hi:
            raise ValueError(f"between({self.lo}, {self.hi}) 区间为空")

    def __call__(self, row) -> int:
        return row.rng.randint(self.lo, self.hi)

    def describe(self) -> str:
        return f"between({self.lo}..{self.hi})"


@dataclass(frozen=True)
class real(Rule):
    """闭区间随机小数。"""

    lo: float
    hi: float
    ndigits: Optional[int] = None

    def __call__(self, row) -> float:
        value = self.lo + row.rng.random() * (self.hi - self.lo)
        return round(value, self.ndigits) if self.ndigits is not None else value

    def describe(self) -> str:
        return f"real({self.lo}..{self.hi})"


@dataclass(frozen=True)
class money(Rule):
    """金额：精确到 scale 位小数的 Decimal。"""

    lo: float
    hi: float
    scale: int = 2

    def __post_init__(self) -> None:
        if self.lo > self.hi:
            raise ValueError(f"money({self.lo}, {self.hi}) 区间为空")
        if self.scale < 0:
            raise ValueError("money() 的 scale 必须 >= 0")

    def __call__(self, row) -> Decimal:
        factor = 10 ** self.scale
        cents = row.rng.randint(int(round(self.lo * factor)), int(round(self.hi * factor)))
        return Decimal(cents).scaleb(-self.scale)

    def describe(self) -> str:
        return f"money({self.lo}..{self.hi}, {self.scale}dp)"


@dataclass(frozen=True)
class ts(Rule):
    """时间：start + step*i 或 start + [0, span) 的随机偏移，可再叠加 jitter。"""

    start: datetime
    span_seconds: Optional[int] = None
    step_seconds: Optional[int] = None
    jitter_seconds: int = 0

    def __call__(self, row) -> datetime:
        if self.step_seconds is not None:
            value = self.start + timedelta(seconds=self.step_seconds * row.i)
        elif self.span_seconds:
            value = self.start + timedelta(seconds=row.rng.randrange(self.span_seconds))
        else:
            value = self.start
        if self.jitter_seconds:
            value += timedelta(seconds=row.rng.randint(-self.jitter_seconds, self.jitter_seconds))
        return value

    def describe(self) -> str:
        bits = [self.start.strftime("%Y-%m-%d %H:%M:%S")]
        if self.step_seconds is not None:
            bits.append(f"+{self.step_seconds}s*i")
        if self.span_seconds:
            bits.append(f"+0..{self.span_seconds}s")
        if self.jitter_seconds:
            bits.append(f"±{self.jitter_seconds}s")
        return "ts(" + " ".join(bits) + ")"


@dataclass(frozen=True)
class dates(Rule):
    """闭区间随机日期。"""

    start: date
    end: date

    def __call__(self, row) -> date:
        span = (self.end - self.start).days
        if span <= 0:
            return self.start
        return self.start + timedelta(days=row.rng.randint(0, span))

    def describe(self) -> str:
        return f"dates({self.start}..{self.end})"


@dataclass(frozen=True)
class uuid4(Rule):
    """确定性 UUID（128 位十六进制，32 字符）。"""

    def __call__(self, row) -> str:
        return f"{row.rng.getrandbits(128):032x}"

    def describe(self) -> str:
        return "uuid4()"


@dataclass(frozen=True)
class token(Rule):
    """随机字符串。"""

    length: int = 12
    alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789"

    def __call__(self, row) -> str:
        rng = row.rng
        return "".join(rng.choice(self.alphabet) for _ in range(self.length))

    def describe(self) -> str:
        return f"token({self.length})"


@dataclass(frozen=True)
class const(Rule):
    value: Any

    def __call__(self, row) -> Any:
        return self.value

    def describe(self) -> str:
        return f"const({self.value!r})"


@dataclass(frozen=True)
class maybe(Rule):
    """以概率 p 返回 None（用于可空列）。"""

    rule: Callable
    p: float = 0.1

    def __call__(self, row) -> Any:
        if row.rng.random() < self.p:
            return None
        return self.rule(row)

    def describe(self) -> str:
        return f"maybe({describe(self.rule)}, p={self.p})"


@dataclass(frozen=True)
class boolean(Rule):
    def __call__(self, row) -> bool:
        return row.rng.random() < 0.5

    def describe(self) -> str:
        return "boolean()"


# ---------------------------------------------------------------------------
# 展示辅助（plan --explain 用）
# ---------------------------------------------------------------------------


def describe(rule: Callable) -> str:
    if isinstance(rule, Rule):
        return rule.describe()
    code = getattr(rule, "__code__", None)
    name = getattr(rule, "__name__", None) or type(rule).__name__
    if code is not None:
        return f"{name}@{os.path.basename(code.co_filename)}:{code.co_firstlineno}"
    return name


def source_at(rule: Callable) -> str:
    """尽量取到规则定义所在的那一行源码，便于人工复核。"""
    code = getattr(rule, "__code__", None)
    if code is None:
        return ""
    try:
        import linecache

        line = linecache.getline(code.co_filename, code.co_firstlineno)
        return " ".join(line.split())
    except Exception:  # noqa: BLE001 - 纯展示，失败无所谓
        return ""
