#!/usr/bin/env python3
"""One read-only cgroup-v2 sample; stdout is suitable for a user timer's journal.

I/O fields are lifetime counters, NOT rates. No argv, environments or file contents
are collected. Sampling races with exit/migration; unavailable fields remain null.
"""
import argparse
from datetime import datetime, timezone, UTC
import json
from pathlib import Path
import subprocess


def read_text(path):
    """Missing processes and restricted procfs entries are expected."""
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return ""


def counters(text):
    """Parse procfs name/value counters, retaining missing values as unavailable."""
    values = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        value = rest.split()
        if value and value[0].isdigit():
            values[key] = int(value[0])
    return values


def sample(cgroup, proc, top=5):
    """Collect root and descendant processes, deduplicating migration overlap."""
    pids = set()
    for members in [cgroup / "cgroup.procs", *cgroup.rglob("cgroup.procs")]:
        pids.update(int(pid) for pid in read_text(members).split() if pid.isdigit())
    processes = []
    for pid in sorted(pids):
        path = proc / str(pid)
        status = read_text(path / "status")
        if not status:
            continue
        io = counters(read_text(path / "io"))
        processes.append({
            "pid": pid,
            "name": read_text(path / "comm").strip(),
            "rss_kb": counters(status).get("VmRSS"),
            "read_bytes": io.get("read_bytes"),
            "write_bytes": io.get("write_bytes"),
        })
    rss = [p for p in processes if p["rss_kb"] is not None]
    io = [p for p in processes if p["read_bytes"] is not None and p["write_bytes"] is not None]
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "processes_sampled": len(processes),
        "memory_current_bytes": read_text(cgroup / "memory.current").strip() or None,
        "memory_pressure": read_text(cgroup / "memory.pressure").strip() or None,
        "io_pressure": read_text(cgroup / "io.pressure").strip() or None,
        "top_rss": sorted(rss, key=lambda p: p["rss_kb"], reverse=True)[:top],
        "top_io_cumulative": sorted(io, key=lambda p: p["read_bytes"] + p["write_bytes"], reverse=True)[:top],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", default="hermes-gateway.service")
    parser.add_argument("--top", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.top <= 100:
        parser.error("--top must be between 1 and 100")
    result = subprocess.run(
        ["systemctl", "--user", "show", "--property=ControlGroup", "--value", args.unit],
        capture_output=True, text=True, timeout=10, check=True,
    )
    group = result.stdout.strip()
    if not group.startswith("/") or group == "/" or ".." in Path(group).parts:
        parser.error("unit has no safe, non-root ControlGroup")
    cgroup = Path("/sys/fs/cgroup") / group.lstrip("/")
    if not (cgroup / "cgroup.procs").is_file():
        parser.error("unit has no readable cgroup-v2 process list")
    print(json.dumps({"unit": args.unit, "cgroup": group, **sample(cgroup, Path("/proc"), args.top)}))


if __name__ == "__main__":
    main()
