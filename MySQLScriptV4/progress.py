"""终端进度：TTY 下原地刷新，非 TTY 按 10% 打印一行。"""
from __future__ import annotations

import sys
import time
from typing import Optional, TextIO


def _human_seconds(seconds: float) -> str:
    if seconds < 0 or seconds != seconds:  # NaN
        return "--"
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


class Progress:
    def __init__(
        self,
        tasks: int,
        rows: Optional[int],
        enabled: bool = True,
        stream: Optional[TextIO] = None,
    ) -> None:
        self.tasks = max(1, tasks)
        self.rows = rows
        self.stream = stream or sys.stdout
        self.enabled = enabled and self.stream is not None
        self._tty = bool(getattr(self.stream, "isatty", lambda: False)())
        self._started = time.perf_counter()
        self._last_render = 0.0
        self._last_plain = -1
        self._dirty = False

    def start(self) -> None:
        if not self.enabled:
            return
        if not self._tty:
            print(
                f"start: {self.tasks:,} tasks"
                + (f", {self.rows:,} rows" if self.rows else ""),
                file=self.stream,
                flush=True,
            )

    def update(
        self,
        done_tasks: int,
        done_rows: int,
        failures: int = 0,
        force: bool = False,
    ) -> None:
        if not self.enabled:
            return
        now = time.perf_counter()
        if not force and now - self._last_render < 0.2:
            return
        self._last_render = now
        elapsed = max(1e-9, now - self._started)
        rate = done_rows / elapsed
        percent = min(100.0, done_tasks * 100.0 / self.tasks)
        eta = (self.tasks - done_tasks) * elapsed / max(1, done_tasks)

        if self._tty:
            width = 20
            filled = int(width * done_tasks / self.tasks)
            bar = "=" * filled + ">" if filled < width else "=" * width
            bar = bar.ljust(width)
            line = (
                f"\r[{bar}] {percent:5.1f}%  {done_tasks:,}/{self.tasks:,} tasks  "
                f"{done_rows:,} rows  {elapsed:5.1f}s  {rate:,.0f} rows/s  "
                f"ETA {_human_seconds(eta)}"
            )
            if failures:
                line += f"  failed {failures}"
            self.stream.write(line + " " * 4)
            self._dirty = True
        else:
            step = int(percent / 10)
            if step > self._last_plain or force:
                self._last_plain = step
                print(
                    f"{percent:5.1f}%  {done_tasks:,}/{self.tasks:,} tasks  "
                    f"{done_rows:,} rows  {elapsed:5.1f}s  {rate:,.0f} rows/s  "
                    f"ETA {_human_seconds(eta)}"
                    + (f"  failed {failures}" if failures else ""),
                    file=self.stream,
                    flush=True,
                )
                self._dirty = False
        self.stream.flush()

    def finish(self) -> None:
        if not self.enabled:
            return
        if self._tty and self._dirty:
            self.stream.write("\n")
            self.stream.flush()
            self._dirty = False
