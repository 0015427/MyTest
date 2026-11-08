"""命令行入口：

    python -m MySQLScriptV4 plan   spec.py
    python -m MySQLScriptV4 run    spec.py --dsn sqlite:demo.db --wipe --verify
    python -m MySQLScriptV4 verify spec.py --dsn mysql://root:pw@localhost:3306/demo

终端只输出 ASCII 标签，避免 Windows 代码页问题；中文说明放文档里。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, Optional, Sequence

from . import __version__
from .ddl import DdlError
from .dsn import DsnError, parse_dsn
from .engine import RunConfig, run as engine_run
from .loader import SpecFileError, load_spec
from .sink import SinkFactory
from .spec import Plan, Problem, Spec, SpecError, Table

__all__ = ["main"]

COMMANDS = ("plan", "run", "verify")


# ---------------------------------------------------------------------------
# 输出辅助
# ---------------------------------------------------------------------------


def _human_count(value: Optional[int]) -> str:
    return "-" if value is None else f"{value:,}"


def _human_bytes(value: Optional[int]) -> str:
    if value is None:
        return "-"
    if value >= 1024 ** 3:
        return f"{value / 1024 ** 3:.1f} GB"
    if value >= 1024 ** 2:
        return f"{value / 1024 ** 2:.1f} MB"
    if value >= 1024:
        return f"{value / 1024:.1f} KB"
    return f"{value} B"


def _short(value: Any, width: int = 48) -> str:
    if isinstance(value, datetime):
        text = value.isoformat(sep=" ")
    elif isinstance(value, date):
        text = value.isoformat()
    else:
        text = str(value)
    text = text.replace("\n", " ")
    return text if len(text) <= width else text[: width - 3] + "..."


def _per_parent_text(table: Table) -> str:
    pp = table.per_parent
    if table.parent is None:
        return "-"
    if isinstance(pp, int) and not isinstance(pp, bool):
        return str(pp)
    if isinstance(pp, (tuple, list)):
        return f"{pp[0]}..{pp[1]}"
    return "fn " + getattr(pp, "__name__", "callable")


def _print_problems(problems: Sequence[Problem], show_warnings: bool = True) -> None:
    for problem in problems:
        if problem.level == "warning" and not show_warnings:
            continue
        print(problem)


def _print_plan_table(plan: Plan) -> None:
    print(f"spec   : {plan.spec.title}")
    tasks_word = "task" if plan.tasks == 1 else "tasks"
    print(
        f"root   : {plan.spec.root.name} {plan.root_rows:,} rows "
        f"-> {plan.tasks:,} {tasks_word} x {plan.task_rows:,}"
    )
    print(
        f"total  : {_human_count(plan.total_rows)} rows, "
        f"est. {_human_bytes(plan.total_bytes)} on disk (indexes excluded)"
    )
    print("")
    header = f"{'table':<20}{'parent':<16}{'rows':>14}{'per-parent':>12}{'batch':>8}{'row B':>8}"
    print(header)
    print("-" * len(header))
    for table in plan.order:
        print(
            f"{table.name:<20}"
            f"{(table.parent.name if table.parent else '-'):<16}"
            f"{_human_count(plan.rows_by_table.get(table.name)):>14}"
            f"{_per_parent_text(table):>12}"
            f"{plan.batch_rows[table.name]:>8}"
            f"{plan.bytes_by_table[table.name]:>8}"
        )


def _print_samples(plan: Plan, explain: bool) -> None:
    for table in plan.order:
        rows = plan.samples.get(table.name) or []
        print(f"\nsample {table.name} ({len(rows)} rows):")
        for row in rows:
            print("  " + "  ".join(f"{name}={_short(value)}" for name, value in row.items()))
        if explain:
            print("  rules:")
            for column, rule, is_fk in table.columns:
                if is_fk:
                    parent = table.parent
                    source = (
                        f"<auto: {parent.name}.{parent.info.primary_key}>"
                        if parent and parent.info.primary_key
                        else "<auto: parent pk>"
                    )
                    print(f"    {column.name:<20} {column.sql_type:<16} {source}")
                    continue
                from .rules import describe, source_at

                text = describe(rule)
                line = source_at(rule)
                print(f"    {column.name:<20} {column.sql_type:<16} {text}")
                if line and line not in text:
                    print(f"    {'':<20} {'':<16} | {line}")


def _plan_payload(plan: Plan, path: str) -> Dict[str, Any]:
    return {
        "ok": plan.ok,
        "spec": path,
        "title": plan.spec.title,
        "root": plan.spec.root.name,
        "root_rows": plan.root_rows,
        "task_rows": plan.task_rows,
        "tasks": plan.tasks,
        "total_rows": plan.total_rows,
        "estimated_bytes": plan.total_bytes,
        "problems": [p.as_dict() for p in plan.problems],
        "tables": [
            {
                "name": table.name,
                "parent": table.parent.name if table.parent else None,
                "fk": table.fk,
                "columns": list(table.column_names),
                "rows": plan.rows_by_table.get(table.name),
                "per_parent": _per_parent_text(table),
                "batch_rows": plan.batch_rows[table.name],
                "estimated_row_bytes": plan.bytes_by_table[table.name],
            }
            for table in plan.order
        ],
        "samples": plan.samples,
    }


# ---------------------------------------------------------------------------
# 命令
# ---------------------------------------------------------------------------


def _build_spec(args) -> Spec:
    loaded = load_spec(args.spec)
    return Spec(
        loaded.tables,
        seed=args.seed if getattr(args, "seed", None) else loaded.seed,
        version=loaded.version,
        title=loaded.title,
    )


def cmd_plan(args) -> int:
    spec = _build_spec(args)
    plan = spec.plan(
        rows=args.rows,
        task_rows=args.task_rows,
        packet_bytes=int(args.packet_mb * 1024 * 1024),
        sample=max(1, args.sample),
    )
    if args.json:
        print(json.dumps(_plan_payload(plan, args.spec), ensure_ascii=False, indent=2, default=str))
        return 0 if plan.ok else 2
    _print_plan_table(plan)
    if plan.problems:
        print("\nchecks:")
        _print_problems(plan.problems, show_warnings=not args.no_warnings)
    if not plan.errors:
        _print_samples(plan, args.explain)
    print(
        f"\nresult: {len(plan.errors)} error(s), "
        f"{len(plan.warnings)} warning(s)"
        + ("" if plan.ok else "  -> fix errors before run")
    )
    return 0 if plan.ok else 2


def cmd_run(args) -> int:
    if args.dry_run:
        dsn_text = None
    else:
        if not args.dsn:
            print("[error] run 需要 --dsn（或显式 --dry-run 只生成不落库）", file=sys.stderr)
            return 2
        dsn_text = args.dsn
    spec = _build_spec(args)
    for problem in spec.problems:
        if problem.level == "warning" and not args.no_warnings:
            print(problem)
    config = RunConfig(
        rows=args.rows,
        task_rows=args.task_rows,
        workers=args.workers,
        seed=args.seed,
        retries=args.retries,
        stop_on_error=not args.keep_going,
        check=args.check,
        create_tables=not args.no_create,
        wipe=args.wipe,
        resume=args.resume,
        verify=args.verify,
        progress=not args.no_progress,
        packet_bytes=int(args.packet_mb * 1024 * 1024),
        max_batch_rows=args.max_batch,
    )
    result = engine_run(spec, dsn_text, config)

    print("\n=== run ===")
    print(f"backend : {result.backend}")
    print(f"run_id  : {result.run_id}")
    print(
        f"tasks   : {result.tasks_done:,} done, {result.tasks_skipped:,} skipped, "
        f"{len(result.failures):,} failed"
    )
    print(f"rows    : {result.rows_written:,} (root target {result.rows_expected_root:,})")
    rate = result.rows_written / result.elapsed if result.elapsed > 0 else 0
    print(f"elapsed : {result.elapsed:.2f}s  ({rate:,.0f} rows/s)")
    print(f"retries : {result.retries}   splits: {result.splits}")
    print("")
    header = f"{'table':<20}{'rows':>14}{'batches':>10}"
    print(header)
    print("-" * len(header))
    for table in spec.order:
        print(
            f"{table.name:<20}{result.rows_by_table.get(table.name, 0):>14,}"
            f"{result.batches_by_table.get(table.name, 0):>10,}"
        )
    if result.failures:
        print("\nfailures:")
        for task, error in result.failures[:20]:
            print(f"  task {task}: {_short(error, 300)}")
        if len(result.failures) > 20:
            print(f"  ... {len(result.failures) - 20} more")
    if result.tasks_done == 0 and result.tasks_skipped:
        print(f"\nnothing to do: {result.tasks_skipped} task(s) already committed")
    if result.checks:
        print("\nverify:")
        for check in result.checks:
            print(f"  {'PASS' if check.ok else 'FAIL'}  {check.name:<32}{check.detail}")
    elif args.verify and result.backend == "dry":
        print("\nverify: skipped (dry-run has no database)")
    print(f"\nresult: {'ok' if result.ok else 'FAILED'}")
    return 0 if result.ok else 1


def cmd_verify(args) -> int:
    if not args.dsn:
        print("[error] verify 需要 --dsn", file=sys.stderr)
        return 2
    spec = _build_spec(args)
    plan = spec.plan(rows=args.rows, task_rows=args.task_rows)
    if plan.errors:
        _print_problems(plan.problems)
        return 2
    target = parse_dsn(args.dsn)
    if target.kind == "sqlite" and not os.path.exists(target.path):
        print(f"[error] 数据库文件不存在：{target.path}", file=sys.stderr)
        return 2
    sink = SinkFactory(target).open()
    try:
        checks = sink.verify(plan)
    finally:
        sink.close()
    print(f"verify {target.label}  ({spec.title})")
    for table in plan.order:
        print(f"  rows {table.name:<20} {_human_count(plan.rows_by_table.get(table.name))}")
    print("")
    for check in checks:
        print(f"  {'PASS' if check.ok else 'FAIL'}  {check.name:<32}{check.detail}")
    ok = all(check.ok for check in checks)
    print(f"\nresult: {'ok' if ok else 'FAILED'}")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m MySQLScriptV4",
        description="根据建表语句 + 字段规则给数据库造数据",
    )
    parser.add_argument("--version", action="version", version=f"MySQLScriptV4 {__version__}")
    sub = parser.add_subparsers(dest="command")

    plan = sub.add_parser("plan", help="校验 spec 并打印计划/样例（不连库）")
    plan.add_argument("spec", help="spec 文件路径")
    plan.add_argument("--rows", type=int, default=None, help="覆盖根表行数（冒烟测试用）")
    plan.add_argument("--task-rows", type=int, default=10_000)
    plan.add_argument("--packet-mb", type=float, default=4.0)
    plan.add_argument("--sample", type=int, default=3, help="每张表打印几行样例")
    plan.add_argument("--explain", action="store_true", help="显示每列的规则与源码行")
    plan.add_argument("--json", action="store_true", help="机器可读输出（给 AI 修错用）")
    plan.add_argument("--no-warnings", action="store_true")
    plan.add_argument("--seed", default=None)
    plan.set_defaults(func=cmd_plan)

    run = sub.add_parser("run", help="生成并写入数据库")
    run.add_argument("spec")
    run.add_argument("--dsn", default=None, help="sqlite:path / mysql://user:pass@host:3306/db")
    run.add_argument("--dry-run", action="store_true", help="只生成不落库（等同 --dsn 为空）")
    run.add_argument("--rows", type=int, default=None)
    run.add_argument("--task-rows", type=int, default=10_000)
    run.add_argument("--workers", type=int, default=0)
    run.add_argument("--seed", default=None)
    run.add_argument("--retries", type=int, default=3)
    run.add_argument("--check", choices=("first", "all", "none"), default="first")
    run.add_argument("--wipe", action="store_true", help="运行前清空目标表")
    run.add_argument("--resume", action="store_true", help="跳过已提交的任务")
    run.add_argument("--verify", action="store_true", help="运行后校验行数/孤儿外键/重复主键")
    run.add_argument("--keep-going", action="store_true", help="某任务失败后继续跑其他任务")
    run.add_argument("--no-create", action="store_true", help="不执行 CREATE TABLE")
    run.add_argument("--no-progress", action="store_true")
    run.add_argument("--no-warnings", action="store_true")
    run.add_argument("--packet-mb", type=float, default=4.0, help="单条 INSERT 目标大小")
    run.add_argument("--max-batch", type=int, default=5_000)
    run.set_defaults(func=cmd_run)

    verify = sub.add_parser("verify", help="只做一致性校验")
    verify.add_argument("spec")
    verify.add_argument("--dsn", required=True)
    verify.add_argument("--rows", type=int, default=None)
    verify.add_argument("--task-rows", type=int, default=10_000)
    verify.add_argument("--seed", default=None)
    verify.set_defaults(func=cmd_verify)

    return parser


def _force_utf8_output() -> None:
    """输出被重定向时用 UTF-8，避免 Windows 代码页把中文写坏。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            if not stream.isatty() and (stream.encoding or "").lower() not in ("utf-8", "utf8"):
                stream.reconfigure(encoding="utf-8")
        except Exception:  # noqa: BLE001 - 老终端/特殊流不支持就算了
            pass


def main(argv: Optional[Sequence[str]] = None) -> int:
    _force_utf8_output()
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] not in COMMANDS and not argv[0].startswith("-"):
        argv = ["plan", *argv]
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 2
    try:
        return int(args.func(args))
    except SpecError as exc:
        print("[error] spec 校验没有通过：", file=sys.stderr)
        for problem in exc.problems:
            print(problem, file=sys.stderr)
        return 2
    except (DdlError, SpecFileError, DsnError, ValueError, RuntimeError, OSError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - 数据库/驱动异常也要有可读输出
        print(f"[error] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
