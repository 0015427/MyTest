"""DSN 解析：支持 dry-run / SQLite 文件 / MySQL。

    dry-run                              只生成，不落库
    sqlite:demo.db                       相对路径
    sqlite:///D:/tmp/demo.db             URL 写法
    D:/tmp/demo.db                       不带前缀时按 SQLite 处理
    mysql://root:pass@localhost:3306/db?charset=utf8mb4
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import unquote

__all__ = ["Dsn", "parse_dsn", "DsnError"]


class DsnError(ValueError):
    pass


@dataclass(frozen=True)
class Dsn:
    kind: str  # dry / sqlite / mysql
    path: str = ""
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    raw: str = ""

    @property
    def identity(self) -> str:
        if self.kind == "sqlite":
            return "sqlite:" + os.path.abspath(self.path)
        if self.kind == "mysql":
            return f"mysql:{self.host}:{self.port}/{self.database}"
        return "dry-run"

    @property
    def label(self) -> str:
        if self.kind == "sqlite":
            return f"sqlite:{self.path}"
        if self.kind == "mysql":
            return f"mysql:{self.user}@{self.host}:{self.port}/{self.database}"
        return "dry-run"

    @property
    def backend(self) -> str:
        return self.kind


def parse_dsn(text: str | None) -> Dsn:
    if text is None or text.strip() == "":
        return Dsn(kind="dry", raw="")
    raw = text.strip()
    lowered = raw.lower()
    if lowered in ("dry", "dry-run", "none", "-"):
        return Dsn(kind="dry", raw=raw)

    if lowered.startswith("mysql://"):
        return _parse_mysql(raw)

    if lowered.startswith("sqlite:"):
        path = raw[len("sqlite:"):]
        if path.startswith("//"):  # sqlite:///C:/x 或 sqlite://host/x
            path = path.lstrip("/")
        return _sqlite(path, raw)
    if "://" in raw:
        raise DsnError(f"不认识的 DSN：{raw}；支持 mysql:// 和 sqlite: 前缀")

    return _sqlite(raw, raw)


def _sqlite(path: str, raw: str) -> Dsn:
    path = path.strip()
    if not path:
        raise DsnError("SQLite 路径为空")
    if path in (":memory:", "file::memory:"):
        raise DsnError("不支持 :memory:（并发任务使用多条连接，会各自看到不同的库）；请用文件路径")
    return Dsn(kind="sqlite", path=path, raw=raw)


def _parse_mysql(raw: str) -> Dsn:
    rest = raw[len("mysql://"):]
    if "@" not in rest:
        raise DsnError("MySQL DSN 需要 mysql://user:password@host:port/database")
    creds, hostpart = rest.rsplit("@", 1)
    user, _, password = creds.partition(":")
    hostport, _, dbpart = hostpart.partition("/")
    if not dbpart:
        raise DsnError("MySQL DSN 里缺少数据库名：mysql://user:pass@host:3306/database")
    database, _, query = dbpart.partition("?")
    host, _, port_text = hostport.partition(":")
    port = 3306
    if port_text:
        try:
            port = int(port_text)
        except ValueError as exc:
            raise DsnError(f"MySQL 端口不是数字：{port_text!r}") from exc
    charset = "utf8mb4"
    for pair in query.split("&"):
        key, _, value = pair.partition("=")
        if key == "charset" and value:
            charset = unquote(value)
    return Dsn(
        kind="mysql",
        host=unquote(host) or "127.0.0.1",
        port=port,
        user=unquote(user),
        password=unquote(password),
        database=unquote(database),
        charset=charset,
        raw=raw,
    )
