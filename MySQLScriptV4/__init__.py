"""MySQLScriptV4：声明式造数。

spec 文件只写三样东西：
  1. 建表语句（从库里复制）
  2. 每张表的字段规则 Table(...)
  3. 父子关系 parent / fk / per_parent

然后：
    python -m MySQLScriptV4 plan   demo.py          # 校验 + 样例，不连库
    python -m MySQLScriptV4 run    demo.py --dsn sqlite:demo.db --wipe --verify
    python -m MySQLScriptV4 verify demo.py --dsn mysql://root:pw@localhost:3306/db
"""
from __future__ import annotations

from .engine import RunConfig, RunResult, run
from .rules import (
    SKIP,
    between,
    boolean,
    const,
    dates,
    maybe,
    money,
    pick,
    real,
    token,
    ts,
    uuid4,
)
from .spec import Plan, Problem, Spec, SpecError, Table

__version__ = "4.0.0"

__all__ = [
    "Table",
    "Spec",
    "Plan",
    "Problem",
    "SpecError",
    "run",
    "RunConfig",
    "RunResult",
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
    "__version__",
]
