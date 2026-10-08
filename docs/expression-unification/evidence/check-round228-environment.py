#!/usr/bin/env python3
"""Summarize round228 environment snapshots and perf-stat sidecars."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

SNAPSHOT = re.compile(r"SNAPSHOT label=(\S+) utc=(\S+)")
PRESSURE = re.compile(r"some avg10=([0-9.]+)")


def read_snapshots(path: Path) -> dict[str, dict[str, object]]:
    snapshots: dict[str, dict[str, object]] = {}
    current: dict[str, object] | None = None
    for line in path.read_text().splitlines():
        match = SNAPSHOT.match(line)
        if match:
            current = {"utc": match.group(2), "proc_stat": {}}
            snapshots[match.group(1)] = current
            continue
        if current is None:
            continue
        if line.startswith("loadavg "):
            current["load1"] = float(line.split()[1])
        elif line.startswith("cpu_pressure "):
            current["cpu_pressure_avg10"] = float(PRESSURE.search(line).group(1))
        elif line.startswith("memory_pressure "):
            current["memory_pressure_avg10"] = float(PRESSURE.search(line).group(1))
        elif line.startswith("io_pressure "):
            current["io_pressure_avg10"] = float(PRESSURE.search(line).group(1))
        elif line.startswith("proc_stat "):
            parts = line.split()
            current["proc_stat"][parts[1]] = [int(value) for value in parts[2:]]
        elif line.startswith("cpu_frequency "):
            pieces = dict(item.split("=", 1) for item in line.split()[1:])
            current[f"cpu{pieces['cpu']}_khz"] = int(pieces["khz"])
        elif line.startswith("k10temp "):
            pieces = dict(item.split("=", 1) for item in line.split()[1:])
            current[f"temp{pieces['sensor']}_millidegree"] = int(pieces["millidegree"])
    return snapshots


def sibling_nonidle_percent(before: list[int], after: list[int]) -> float:
    delta = [right - left for left, right in zip(before, after)]
    total = sum(delta)
    idle = sum(delta[index] for index in [3, 4] if index < len(delta))
    return 0.0 if total == 0 else 100.0 * (total - idle) / total


def read_perf(path: Path) -> dict[str, float]:
    values: dict[str, float] = {}
    with path.open(newline="") as handle:
        for row in csv.reader(line for line in handle if not line.startswith("#")):
            if len(row) < 3 or not row[0] or row[0].startswith("<"):
                continue
            try:
                value = float(row[0])
            except ValueError:
                continue
            values[row[2].removesuffix(":u")] = value
    return values


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out", type=Path)
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()
    snapshots = read_snapshots(args.out / "environment.log")
    perf_paths = sorted((args.out / "perf").glob("*.csv"))
    expected_stems = {
        f"{suite}-{revision}-{run}"
        for suite in ["width-one", "batch"]
        for revision in ["current", "frozen"]
        for run in range(1, 6)
    }
    actual_stems = {path.stem for path in perf_paths}
    if actual_stems != expected_stems:
        raise ValueError(
            f"perf sidecars are incomplete: missing={sorted(expected_stems - actual_stems)} "
            f"extra={sorted(actual_stems - expected_stems)}"
        )
    expected_snapshots = {
        f"{stem}-{stage}" for stem in expected_stems for stage in ["before", "after"]
    } | {"session-start", "session-end"}
    if set(snapshots) != expected_snapshots:
        raise ValueError("environment snapshots do not match the exact 20-process session")

    processes = []
    for perf_path in perf_paths:
        stem = perf_path.stem
        before = snapshots[f"{stem}-before"]
        after = snapshots[f"{stem}-after"]
        perf = read_perf(perf_path)
        task_seconds = perf["task-clock"] / 1000.0
        processes.append(
            {
                "process": stem,
                "load1_before": before["load1"],
                "load1_after": after["load1"],
                "cpu_pressure_avg10_max": max(
                    before["cpu_pressure_avg10"], after["cpu_pressure_avg10"]
                ),
                "temperature_c_max": max(
                    before["temp1_millidegree"], after["temp1_millidegree"]
                )
                / 1000.0,
                "cpu2_frequency_mhz_range": [
                    before["cpu2_khz"] / 1000.0,
                    after["cpu2_khz"] / 1000.0,
                ],
                "cpu14_nonidle_percent": sibling_nonidle_percent(
                    before["proc_stat"]["cpu14"], after["proc_stat"]["cpu14"]
                ),
                "context_switches_per_second": perf["context-switches"] / task_seconds,
                "cpu_migrations": perf["cpu-migrations"],
                "major_faults": perf["major-faults"],
                "cycles_per_ref_cycle": perf["cycles"] / perf["ref-cycles"],
            }
        )
    summary = {
        "processes": processes,
        "max_load1": max(item["load1_before"] for item in processes),
        "max_cpu_pressure_avg10": max(
            item["cpu_pressure_avg10_max"] for item in processes
        ),
        "max_temperature_c": max(item["temperature_c_max"] for item in processes),
        "max_cpu14_nonidle_percent": max(
            item["cpu14_nonidle_percent"] for item in processes
        ),
        "max_context_switches_per_second": max(
            item["context_switches_per_second"] for item in processes
        ),
        "total_cpu_migrations": sum(item["cpu_migrations"] for item in processes),
        "total_major_faults": sum(item["major_faults"] for item in processes),
        "cycles_per_ref_cycle_range": [
            min(item["cycles_per_ref_cycle"] for item in processes),
            max(item["cycles_per_ref_cycle"] for item in processes),
        ],
    }
    args.json.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key: value for key, value in summary.items() if key != "processes"}, indent=2))


if __name__ == "__main__":
    main()
