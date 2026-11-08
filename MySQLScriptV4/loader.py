"""从任意路径加载用户 spec 文件（不需要 package 结构）。"""
from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass, field
from typing import List

from .spec import Table

__all__ = ["LoadedSpec", "load_spec", "SpecFileError"]


class SpecFileError(RuntimeError):
    pass


@dataclass
class LoadedSpec:
    path: str
    tables: List[Table]
    seed: str = "v4"
    version: str = "1"
    title: str = ""
    namespace: dict = field(default_factory=dict)


def load_spec(path: str) -> LoadedSpec:
    absolute = os.path.abspath(path)
    if not os.path.isfile(absolute):
        raise SpecFileError(f"找不到 spec 文件：{path}")
    directory = os.path.dirname(absolute)
    if directory not in sys.path:
        sys.path.insert(0, directory)

    module_name = "dataforge_spec_" + os.path.splitext(os.path.basename(absolute))[0]
    spec = importlib.util.spec_from_file_location(module_name, absolute)
    if spec is None or spec.loader is None:
        raise SpecFileError(f"无法加载 spec 文件：{path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise

    tables = getattr(module, "TABLES", None)
    if tables is None:
        raise SpecFileError(
            f"{path} 里没有 TABLES 变量；spec 文件应形如：\n"
            "    TABLES = [Table('users', USERS_DDL, rows=100_000, fields={...})]"
        )
    if not isinstance(tables, (list, tuple)):
        raise SpecFileError(f"TABLES 必须是 list，收到 {type(tables).__name__}")

    return LoadedSpec(
        path=absolute,
        tables=list(tables),
        seed=str(getattr(module, "SEED", "v4")),
        version=str(getattr(module, "SPEC_VERSION", "1")),
        title=str(getattr(module, "TITLE", os.path.splitext(os.path.basename(absolute))[0])),
        namespace=vars(module),
    )
