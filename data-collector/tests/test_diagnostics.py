from __future__ import annotations

from pathlib import Path

from prediction_collector.common.diagnostics import process_memory_snapshot


def test_process_memory_snapshot_reads_proc_and_cgroup_files(
    workspace_tmp_path: Path,
) -> None:
    proc_status = workspace_tmp_path / "status"
    proc_status.write_text(
        "Name:\tpython\nVmHWM:\t4096 kB\nVmRSS:\t3072 kB\n",
        encoding="utf-8",
    )
    cgroup = workspace_tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "memory.current").write_text("123456\n", encoding="utf-8")
    (cgroup / "memory.peak").write_text("234567\n", encoding="utf-8")
    (cgroup / "memory.max").write_text("max\n", encoding="utf-8")
    (cgroup / "memory.events").write_text(
        "low 1\noom 2\noom_kill 3\n", encoding="utf-8"
    )

    snapshot = process_memory_snapshot(
        proc_status_path=proc_status,
        cgroup_root=cgroup,
    )

    assert snapshot["process_rss_bytes"] == 3072 * 1024
    assert snapshot["process_peak_rss_bytes"] == 4096 * 1024
    assert snapshot["cgroup_memory_current_bytes"] == 123456
    assert snapshot["cgroup_memory_peak_bytes"] == 234567
    assert snapshot["cgroup_memory_limit_bytes"] == "max"
    assert snapshot["cgroup_memory_events"]["oom"] == 2
    assert snapshot["cgroup_memory_events"]["oom_kill"] == 3
