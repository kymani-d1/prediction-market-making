from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def process_memory_snapshot(
    *,
    proc_status_path: Path = Path("/proc/self/status"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> dict[str, Any]:
    """Return bounded, best-effort process and cgroup memory diagnostics."""
    snapshot: dict[str, Any] = {"pid": os.getpid()}
    try:
        status = proc_status_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        status = ""
    for line in status.splitlines():
        key, separator, value = line.partition(":")
        if not separator or key not in {"VmRSS", "VmHWM"}:
            continue
        parts = value.strip().split()
        if parts and parts[0].isdigit():
            snapshot[
                "process_rss_bytes" if key == "VmRSS" else "process_peak_rss_bytes"
            ] = int(parts[0]) * 1024

    for filename, metric in (
        ("memory.current", "cgroup_memory_current_bytes"),
        ("memory.peak", "cgroup_memory_peak_bytes"),
        ("memory.max", "cgroup_memory_limit_bytes"),
    ):
        try:
            raw = (cgroup_root / filename).read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            continue
        if raw.isdigit():
            snapshot[metric] = int(raw)
        elif raw:
            snapshot[metric] = raw

    events: dict[str, int] = {}
    for filename in ("memory.events", "memory.events.local"):
        try:
            raw_events = (cgroup_root / filename).read_text(
                encoding="utf-8"
            )
        except (OSError, UnicodeError):
            continue
        prefix = "local_" if filename.endswith(".local") else ""
        for line in raw_events.splitlines():
            name, separator, raw_value = line.partition(" ")
            if separator and raw_value.isdigit():
                events[f"{prefix}{name}"] = int(raw_value)
    if events:
        snapshot["cgroup_memory_events"] = events
    return snapshot
