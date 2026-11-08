"""解析 CREATE TABLE：只提取造数需要的信息。

提取内容：列顺序、列名、类型/长度/精度、NOT NULL、默认值、生成列、主键。
不负责：索引、字符集、分区、外键约束（这些原样交给数据库执行）。

支持的写法：
  CREATE TABLE [IF NOT EXISTS] `t` (
      `id` VARCHAR(32) NOT NULL,
      price DECIMAL(10,2) DEFAULT 0.00,
      total DECIMAL(10,2) GENERATED ALWAYS AS (price*2) STORED,
      PRIMARY KEY (`id`),
      KEY idx_x (price)
  ) ENGINE=InnoDB;
  标识符可用 `反引号` / "双引号" / [方括号]，可用 --  #  /* */ 注释。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

__all__ = [
    "DdlError",
    "Column",
    "TableInfo",
    "parse_table",
    "ensure_if_not_exists",
    "sqlite_type",
]


class DdlError(ValueError):
    """建表语句无法解析，或者用了本期不支持的写法。"""


_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_STRICT_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

_ATTR_WORDS = frozenset(
    "NOT NULL DEFAULT PRIMARY UNIQUE KEY COMMENT COLLATE REFERENCES CHECK "
    "GENERATED AUTO_INCREMENT ON CHARACTER CHARSET AS STORED VIRTUAL VISIBLE "
    "INVISIBLE COLUMN_FORMAT STORAGE SRID".split()
)
_CONSTRAINT_WORDS = frozenset(
    "PRIMARY UNIQUE KEY INDEX CONSTRAINT FOREIGN FULLTEXT SPATIAL CHECK".split()
)

_INT_TYPES = frozenset("TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT SERIAL BIT YEAR".split())
_BOOL_TYPES = frozenset("BOOL BOOLEAN".split())
_FLOAT_TYPES = frozenset("FLOAT DOUBLE REAL".split())
_DECIMAL_TYPES = frozenset("DECIMAL NUMERIC DEC FIXED".split())
_TEMPORAL_TYPES = frozenset("DATE DATETIME TIMESTAMP TIME".split())
_TEXT_TYPES = frozenset(
    "CHAR VARCHAR NCHAR NVARCHAR TINYTEXT TEXT MEDIUMTEXT LONGTEXT CLOB NATIONAL".split()
)
_BLOB_TYPES = frozenset("BINARY VARBINARY TINYBLOB BLOB MEDIUMBLOB LONGBLOB BYTEA".split())


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    kind: str  # int / bool / float / decimal / temporal / text / blob / json / enum / other
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    enum_values: Tuple[str, ...] = ()
    not_null: bool = False
    has_default: bool = False
    default_sql: Optional[str] = None
    generated: bool = False
    auto_increment: bool = False
    unsigned: bool = False

    @property
    def max_chars(self) -> Optional[int]:
        """字符长度上限；TEXT/BLOB 等无上限的列返回 None。"""
        return self.length

    def __str__(self) -> str:  # 报告里用
        return f"{self.name} {self.sql_type}"


@dataclass(frozen=True)
class TableInfo:
    name: str
    columns: Tuple[Column, ...]
    primary_key: Optional[str]

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def insert_columns(self) -> Tuple[Column, ...]:
        """可以显式写值的列（生成列由数据库算，不能写）。"""
        return tuple(c for c in self.columns if not c.generated)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


# ---------------------------------------------------------------------------
# 词法辅助
# ---------------------------------------------------------------------------


def strip_comments(sql: str) -> str:
    """去掉 -- / # / /* */ 注释，字符串字面量内的内容保持原样。"""
    out: List[str] = []
    i, n = 0, len(sql)
    quote: Optional[str] = None
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:  # '' 转义
                    out.append(sql[i + 1])
                    i += 2
                    continue
                quote = None
            elif ch == "\\" and quote != "`" and i + 1 < n:
                out.append(sql[i + 1])
                i += 2
                continue
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-" and (i == 0 or sql[i - 1].isspace()):
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        if ch == "#":
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _scan_ident(text: str, pos: int = 0):
    """读取一个标识符，返回 (name, quoted, end)；读不到返回 None。"""
    n = len(text)
    i = pos
    while i < n and text[i].isspace():
        i += 1
    if i >= n:
        return None
    ch = text[i]
    if ch in ("`", '"', "["):
        close = "]" if ch == "[" else ch
        j = i + 1
        buf: List[str] = []
        while j < n:
            c = text[j]
            if c == close:
                if close != "]" and j + 1 < n and text[j + 1] == close:
                    buf.append(close)
                    j += 2
                    continue
                return "".join(buf), True, j + 1
            buf.append(c)
            j += 1
        raise DdlError(f"标识符引号没有闭合：{text[i:i + 40]!r}")
    m = _IDENT.match(text, i)
    if not m:
        return None
    return m.group(0), False, m.end()


def _split_top_level(body: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    quote: Optional[str] = None
    i, n = 0, len(body)
    while i < n:
        ch = body[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                if i + 1 < n and body[i + 1] == quote:
                    buf.append(body[i + 1])
                    i += 2
                    continue
                quote = None
            elif ch == "\\" and quote != "`" and i + 1 < n:
                buf.append(body[i + 1])
                i += 2
                continue
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return [p.strip() for p in parts if p.strip()]


def _match_paren(sql: str, open_pos: int) -> int:
    depth = 0
    quote: Optional[str] = None
    i, n = open_pos, len(sql)
    while i < n:
        ch = sql[i]
        if quote:
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            elif ch == "\\" and quote != "`" and i + 1 < n:
                i += 2
                continue
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise DdlError("建表语句括号不匹配")


def _find_type_end(rest: str) -> int:
    """类型定义结束的位置：遇到空白 + 属性关键字就停。"""
    depth = 0
    quote: Optional[str] = None
    i, n = 0, len(rest)
    while i < n:
        ch = rest[i]
        if quote:
            if ch == quote:
                if i + 1 < n and rest[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            elif ch == "\\" and quote != "`" and i + 1 < n:
                i += 2
                continue
            i += 1
            continue
        if ch in "'\"`":
            quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch.isspace() and depth == 0:
            j = i
            while j < n and rest[j].isspace():
                j += 1
            m = _IDENT.match(rest, j)
            if m and m.group(0).upper() in _ATTR_WORDS:
                return i
        i += 1
    return n


def _read_default(attrs: str) -> Optional[str]:
    """取出 DEFAULT 后面的原始字面量（给 SQLite 代理建表用）。"""
    m = re.search(r"\bDEFAULT\s+", attrs, re.I)
    if not m:
        return None
    s = attrs[m.end():]
    if not s:
        return None
    if s[0] in "'\"":
        q = s[0]
        j = 1
        while j < len(s):
            if s[j] == q:
                if j + 1 < len(s) and s[j + 1] == q:
                    j += 2
                    continue
                return s[: j + 1]
            if s[j] == "\\":
                j += 2
                continue
            j += 1
        return s
    if s[0] == "(":
        depth = 0
        for j, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return s[: j + 1]
        return s
    m = re.match(r"[-+]?[A-Za-z0-9_.]+(?:\([^)]*\))?", s)
    return m.group(0) if m else None


def _parse_type(raw: str) -> dict:
    text = " ".join(raw.split())
    upper = text.upper()
    m = re.match(r"[A-Za-z_]+", upper)
    if not m:
        raise DdlError(f"无法识别的类型：{raw!r}")
    base = m.group(0)

    info = dict(
        sql_type=text,
        kind="other",
        length=None,
        precision=None,
        scale=None,
        enum_values=(),
        unsigned="UNSIGNED" in upper,
    )

    if base in _INT_TYPES:
        info["kind"] = "int"
    elif base in _BOOL_TYPES:
        info["kind"] = "bool"
    elif base in _FLOAT_TYPES:
        info["kind"] = "float"
    elif base in _DECIMAL_TYPES:
        info["kind"] = "decimal"
        pm = re.search(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)", text)
        if pm:
            info["precision"], info["scale"] = int(pm.group(1)), int(pm.group(2))
        else:
            info["precision"], info["scale"] = 10, 0
    elif base in _TEMPORAL_TYPES:
        info["kind"] = "temporal"
    elif base in _TEXT_TYPES:
        info["kind"] = "text"
        lm = re.search(r"\(\s*(\d+)\s*\)", text)
        if lm:
            info["length"] = int(lm.group(1))
    elif base in _BLOB_TYPES:
        info["kind"] = "blob"
        lm = re.search(r"\(\s*(\d+)\s*\)", text)
        if lm:
            info["length"] = int(lm.group(1))
    elif base == "JSON":
        info["kind"] = "json"
    elif base in ("ENUM", "SET"):
        info["kind"] = "enum"
        values = tuple(v.replace("''", "'") for v in re.findall(r"'((?:[^']|'')*)'", text))
        info["enum_values"] = values
        if not values:
            raise DdlError(f"ENUM/SET 没有解析到候选值：{raw!r}")
    return info


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


def parse_table(ddl: str, expected_name: Optional[str] = None) -> TableInfo:
    sql = strip_comments(ddl)
    m = re.search(r"\bCREATE\s+(?:TEMPORARY\s+)?TABLE\b", sql, re.I)
    if not m:
        raise DdlError("没有找到 `CREATE TABLE ...` 语句（建表语句请原样贴入）")

    i = m.end()
    while True:
        mm = re.match(r"\s*IF\s+NOT\s+EXISTS\b", sql[i:], re.I)
        if not mm:
            break
        i += mm.end()

    ident = _scan_ident(sql, i)
    if not ident:
        raise DdlError("CREATE TABLE 后面没有表名")
    table_name, quoted, pos = ident
    if not _STRICT_IDENT.match(table_name):
        raise DdlError(
            f"暂不支持带空格/横线/中文等特殊字符的标识符：{table_name!r}；"
            "请改用英文字母、数字、下划线（可含 $）"
        )
    if expected_name and table_name != expected_name:
        raise DdlError(f"建表语句里的表名 {table_name} 与 Table.name={expected_name} 不一致")

    while pos < len(sql) and sql[pos].isspace():
        pos += 1
    if pos >= len(sql) or sql[pos] != "(":
        raise DdlError(f"表 {table_name} 的名称后面没有找到 '('")
    close = _match_paren(sql, pos)
    body = sql[pos + 1: close]

    columns: List[Column] = []
    seen = set()
    inline_pk: Optional[str] = None
    table_pk: Optional[str] = None

    for part in _split_top_level(body):
        ident = _scan_ident(part, 0)
        if ident is None:
            continue
        name, quoted, end = ident
        if not quoted and name.upper() in _CONSTRAINT_WORDS:
            upper = part.upper()
            if "PRIMARY" in upper:
                pm = re.search(r"PRIMARY\s+KEY\s*(?:USING\s+\w+\s*)?\(([^)]*)\)", part, re.I)
                if pm:
                    keys: List[str] = []
                    for raw_key in pm.group(1).split(","):
                        key_ident = _scan_ident(raw_key, 0)
                        if key_ident:
                            keys.append(key_ident[0])
                    if len(keys) != 1:
                        raise DdlError(
                            f"本期只支持单列主键，表 {table_name} 收到 ({', '.join(keys)})；"
                            "多列主键请改成单列代理键"
                        )
                    table_pk = keys[0]
            continue

        if not _STRICT_IDENT.match(name):
            raise DdlError(
                f"暂不支持带特殊字符的列名：{name!r}；请改用英文字母、数字、下划线"
            )
        if name in seen:
            raise DdlError(f"表 {table_name} 的列 {name} 重复定义")
        seen.add(name)

        rest = part[end:]
        type_end = _find_type_end(rest)
        type_text = rest[:type_end].strip()
        attrs = rest[type_end:]
        if not type_text:
            raise DdlError(f"列 {table_name}.{name} 缺少类型定义")
        if re.search(r"\bUNSIGNED\b", type_text, re.I) is None and re.search(
            r"\bUNSIGNED\b", attrs, re.I
        ):
            type_text += " UNSIGNED"

        parsed = _parse_type(type_text)
        has_default = bool(re.search(r"\bDEFAULT\b", attrs, re.I))
        generated = bool(
            re.search(r"\bGENERATED\b", attrs, re.I)
            or re.search(r"\bAS\s*\(", attrs, re.I)
        )
        column = Column(
            name=name,
            sql_type=parsed["sql_type"],
            kind=parsed["kind"],
            length=parsed["length"],
            precision=parsed["precision"],
            scale=parsed["scale"],
            enum_values=parsed["enum_values"],
            not_null=bool(re.search(r"\bNOT\s+NULL\b", attrs, re.I)),
            has_default=has_default,
            default_sql=_read_default(attrs) if has_default else None,
            generated=generated,
            auto_increment=bool(re.search(r"\bAUTO_INCREMENT\b", attrs, re.I)),
            unsigned=parsed["unsigned"],
        )
        columns.append(column)
        if re.search(r"\bPRIMARY\s+KEY\b", attrs, re.I):
            inline_pk = name

    if not columns:
        raise DdlError(f"表 {table_name} 没有解析到任何列")

    if inline_pk and table_pk and inline_pk != table_pk:
        raise DdlError(
            f"表 {table_name} 同时声明了内联主键 {inline_pk} 和表级主键 {table_pk}，请只保留一个"
        )
    primary_key = table_pk or inline_pk
    names = [c.name for c in columns]
    if primary_key and primary_key not in names:
        raise DdlError(f"表 {table_name} 的主键列 {primary_key} 不在列定义里")

    return TableInfo(name=table_name, columns=tuple(columns), primary_key=primary_key)


def ensure_if_not_exists(ddl: str) -> str:
    if re.search(r"CREATE\s+(?:TEMPORARY\s+)?TABLE\s+IF\s+NOT\s+EXISTS", ddl, re.I):
        return ddl
    return re.sub(
        r"(CREATE\s+(?:TEMPORARY\s+)?TABLE)\s+",
        r"\1 IF NOT EXISTS ",
        ddl,
        count=1,
        flags=re.I,
    )


def sqlite_type(column: Column) -> str:
    return {
        "int": "INTEGER",
        "bool": "INTEGER",
        "float": "REAL",
        "decimal": "TEXT",  # 保留精确字符串，避免 REAL 丢精度
        "temporal": "TEXT",
        "text": "TEXT",
        "blob": "BLOB",
        "json": "TEXT",
        "enum": "TEXT",
    }.get(column.kind, "TEXT")
