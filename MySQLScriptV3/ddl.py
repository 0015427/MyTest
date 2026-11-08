"""解析用户手写的 CREATE TABLE 语句：列顺序、列类型、主键。

只支持"常规列定义 + 表级约束行"的写法；约束行（PRIMARY KEY / KEY / CONSTRAINT ...）
只用来提取主键，其余细节原样交给数据库执行。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

_IDENT = re.compile(r"[`\[]?([A-Za-z_][A-Za-z0-9_$]*)[`\]]?")
_CREATE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"\[]?([A-Za-z0-9_$]+)[`\"\]]?\s*\(",
    re.I,
)
_CONSTRAINT_WORDS = ("PRIMARY", "UNIQUE", "KEY", "INDEX", "CONSTRAINT", "FOREIGN", "FULLTEXT", "SPATIAL", "CHECK")
_TYPE_STOP = re.compile(
    r"\s+(?=(?:NOT\s+NULL|NULL|PRIMARY\s+KEY|UNIQUE|DEFAULT|COMMENT|COLLATE|REFERENCES|CHECK|GENERATED|AUTO_INCREMENT)\b)",
    re.I,
)


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    sql_type: str
    not_null: bool = False


@dataclass(frozen=True)
class DdlInfo:
    name: str
    columns: Tuple[ColumnInfo, ...]
    primary_key: Optional[str]

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(c.name for c in self.columns)


def _split_top_level(body: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    quote: Optional[str] = None
    for ch in body:
        if quote is not None:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return [p.strip() for p in parts if p.strip()]


def _first_identifier(text: str) -> Optional[str]:
    match = _IDENT.search(text)
    return match.group(1) if match else None


def parse_ddl(ddl: str, expected_name: Optional[str] = None) -> DdlInfo:
    match = _CREATE.search(ddl)
    if not match:
        raise ValueError(
            "无法解析建表语句：需要形如 `CREATE TABLE users (id VARCHAR(64) PRIMARY KEY, ...)` 的写法"
        )
    table_name = match.group(1)
    if expected_name and table_name != expected_name:
        raise ValueError(f"建表语句里的表名 {table_name} 与 Table.name={expected_name} 不一致")

    # 找到与之配对的右括号
    index = match.end() - 1
    depth = 0
    quote: Optional[str] = None
    while index < len(ddl):
        ch = ddl[index]
        if quote is not None:
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        index += 1
    if depth != 0:
        raise ValueError("建表语句括号不匹配")

    body = ddl[match.end():index]
    columns: List[ColumnInfo] = []
    inline_pk: Optional[str] = None
    table_pk: Optional[str] = None

    for part in _split_top_level(body):
        first = _first_identifier(part)
        if not first:
            continue
        upper_first = first.upper()
        if upper_first in _CONSTRAINT_WORDS:
            pk_match = re.search(r"PRIMARY\s+KEY\s*\(([^)]*)\)", part, re.I)
            if pk_match:
                keys = [k.strip(" `[]") for k in pk_match.group(1).split(",") if k.strip()]
                if len(keys) != 1:
                    raise ValueError(f"一期只支持单列主键，收到: {keys}")
                table_pk = keys[0]
            continue

        ident = _IDENT.match(part)
        if not ident:
            raise ValueError(f"无法解析列定义: {part!r}")
        column_name = ident.group(1)
        rest = part[ident.end():].strip()
        stop = _TYPE_STOP.search(rest)
        sql_type = (rest[: stop.start()] if stop else rest).strip()
        if not sql_type:
            raise ValueError(f"列 {column_name} 缺少类型定义")
        not_null = bool(re.search(r"NOT\s+NULL", rest, re.I))
        if re.search(r"PRIMARY\s+KEY", rest, re.I):
            inline_pk = column_name
        columns.append(ColumnInfo(name=column_name, sql_type=sql_type, not_null=not_null))

    if not columns:
        raise ValueError("建表语句里没有解析到任何列")

    primary_key = table_pk or inline_pk
    if primary_key and primary_key not in [c.name for c in columns]:
        raise ValueError(f"主键列 {primary_key} 不在列定义里")

    return DdlInfo(name=table_name, columns=tuple(columns), primary_key=primary_key)


def ensure_if_not_exists(ddl: str) -> str:
    if re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", ddl, re.I):
        return ddl
    return re.sub(r"CREATE\s+TABLE", "CREATE TABLE IF NOT EXISTS", ddl, count=1, flags=re.I)
