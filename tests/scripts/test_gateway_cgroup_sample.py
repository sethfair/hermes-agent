"""Read-only sampler preserves separate memory and cumulative-I/O leaders."""
from scripts.gateway_cgroup_sample import sample


def test_sample_ranks_memory_and_io_separately_and_handles_disappearing_process(tmp_path):
    cgroup, proc = tmp_path / "cgroup", tmp_path / "proc"
    cgroup.mkdir()
    proc.mkdir()
    (cgroup / "cgroup.procs").write_text("11\n13\n")
    child = cgroup / "child"
    child.mkdir()
    (child / "cgroup.procs").write_text("12\n11\n14\n")
    for pid, rss, io in [(11, 900, "read_bytes: 1\nwrite_bytes: 2\n"),
                         (12, 100, "read_bytes: 10000\nwrite_bytes: 20000\n"),
                         (14, 200, None)]:
        path = proc / str(pid)
        path.mkdir()
        (path / "status").write_text(f"VmRSS:\t{rss} kB\n")
        (path / "comm").write_text("worker\n")
        if io is not None:
            (path / "io").write_text(io)
    result = sample(cgroup, proc, top=3)
    assert [p["pid"] for p in result["top_rss"]] == [11, 14, 12]
    assert [p["pid"] for p in result["top_io_cumulative"]] == [12, 11]
    assert result["top_rss"][1]["read_bytes"] is None
    assert result["top_io_cumulative"][0]["write_bytes"] == 20000
    assert result["processes_sampled"] == 3
