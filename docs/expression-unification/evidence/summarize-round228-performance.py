#!/usr/bin/env python3
"""Summarize round228 paired process medians and bootstrap ratio intervals."""

from __future__ import annotations

import argparse
import json
import random
import re
import statistics
from collections import defaultdict
from pathlib import Path

RUN = re.compile(r"PERF_PROCESS_RUN suite=(\S+) revision=(\S+) run=(\d+)")
WIDTH = re.compile(
    r"EXPR_LANE_PERF tier=(\w+) mode=(\w+) name=(\w+) sample=(\d+) .*ns_per_eval=([0-9.]+)"
)
BATCH = re.compile(
    r"EXPR_BATCH_PERF mode=(\w+) route=(\w+) name=(\w+) scenario=(\w+) "
    r"physical_rows=(\d+) selected_rows=(\d+) sample=(\d+) .*"
    r"ns_per_batch=([0-9.]+) ns_per_row=([0-9.]+)"
)
CALIBRATION = re.compile(
    r"EXPR_(?:LANE|BATCH)_CALIBRATION stage=(\w+) .*ns_per_iter=([0-9.]+)"
)


def bootstrap_interval(values: list[float], seed: int = 2_282_026) -> tuple[float, float]:
    rng = random.Random(seed)
    estimates = []
    for _ in range(10_000):
        sample = [values[rng.randrange(len(values))] for _ in values]
        estimates.append(statistics.median(sample))
    estimates.sort()
    return estimates[249], estimates[9749]


def ratio_summary(current: list[float], frozen: list[float]) -> dict[str, object]:
    if len(current) != len(frozen) or not current:
        raise ValueError(f"unpaired process medians: {len(current)} current, {len(frozen)} frozen")
    ratios = [left / right for left, right in zip(current, frozen)]
    low, high = bootstrap_interval(ratios)
    return {
        "pairs": len(ratios),
        "current_process_medians": current,
        "frozen_process_medians": frozen,
        "paired_ratios": ratios,
        "current_median": statistics.median(current),
        "frozen_median": statistics.median(frozen),
        "paired_ratio_median": statistics.median(ratios),
        "paired_ratio_bootstrap_95": [low, high],
        "paired_ratio_mad": statistics.median(
            abs(value - statistics.median(ratios)) for value in ratios
        ),
    }


def parse_logs(out: Path) -> dict[str, object]:
    width_values: dict[tuple[object, ...], list[float]] = defaultdict(list)
    batch_values: dict[tuple[object, ...], list[float]] = defaultdict(list)
    calibration: dict[tuple[object, ...], list[float]] = defaultdict(list)

    for suite, filename in [("width-one", "width-one.log"), ("batch", "batch.log")]:
        revision = None
        run = None
        for line in (out / filename).read_text().splitlines():
            match = RUN.search(line)
            if match:
                assert match.group(1) == suite
                revision, run = match.group(2), int(match.group(3))
                continue
            match = CALIBRATION.search(line)
            if match and revision is not None:
                calibration[(suite, revision, run, match.group(1))].append(
                    float(match.group(2))
                )
                continue
            if suite == "width-one":
                match = WIDTH.search(line)
                if match:
                    tier, mode, name = match.group(1), match.group(2), match.group(3)
                    width_values[(revision, run, tier, mode, name)].append(
                        float(match.group(5))
                    )
            else:
                match = BATCH.search(line)
                if match:
                    mode, route, name, scenario = match.group(1, 2, 3, 4)
                    physical, selected = int(match.group(5)), int(match.group(6))
                    batch_values[
                        (revision, run, mode, route, name, scenario, physical, selected)
                    ].append(float(match.group(9)))

    expected_runs = list(range(1, 6))
    if len(width_values) != 180 or any(len(values) != 9 for values in width_values.values()):
        raise ValueError("width-one log is incomplete: expected 180 cells with 9 samples each")
    if len(batch_values) != 1_550 or any(len(values) != 7 for values in batch_values.values()):
        raise ValueError("batch log is incomplete: expected 1,550 process cells with 7 samples each")
    if len(calibration) != 40 or any(len(values) != 1 for values in calibration.values()):
        raise ValueError("calibration log is incomplete: expected 40 single observations")

    width_process = {key: statistics.median(values) for key, values in width_values.items()}
    batch_process = {key: statistics.median(values) for key, values in batch_values.items()}
    runs = sorted({key[1] for key in width_process})
    if runs != expected_runs or sorted({key[1] for key in batch_process}) != expected_runs:
        raise ValueError(f"unexpected process runs: {runs}")

    width = []
    width_cells = sorted({(key[2], key[4]) for key in width_process})
    for tier, name in width_cells:
        frozen = [width_process[("frozen", run, tier, "frozen_native", name)] for run in runs]
        for mode in ["one_shot", "lane_cache"]:
            current = [width_process[("current", run, tier, mode, name)] for run in runs]
            width.append(
                {
                    "tier": tier,
                    "name": name,
                    "comparison": f"{mode}/frozen_native",
                    **ratio_summary(current, frozen),
                }
            )
        one_shot = [width_process[("current", run, tier, "one_shot", name)] for run in runs]
        lane = [width_process[("current", run, tier, "lane_cache", name)] for run in runs]
        width.append(
            {
                "tier": tier,
                "name": name,
                "comparison": "lane_cache/one_shot",
                **ratio_summary(lane, one_shot),
            }
        )

    batch = []
    batch_cells = sorted(
        {
            (key[4], key[5], key[6], key[7], key[3])
            for key in batch_process
            if key[0] == "current"
        }
    )
    for name, scenario, physical, selected, route in batch_cells:
        current = [
            batch_process[
                (
                    "current",
                    run,
                    "current_production",
                    route,
                    name,
                    scenario,
                    physical,
                    selected,
                )
            ]
            for run in runs
        ]
        frozen = [
            batch_process[
                (
                    "frozen",
                    run,
                    "frozen_production",
                    "frozen_native",
                    name,
                    scenario,
                    physical,
                    selected,
                )
            ]
            for run in runs
        ]
        batch.append(
            {
                "name": name,
                "scenario": scenario,
                "physical_rows": physical,
                "selected_rows": selected,
                "current_route": route,
                **ratio_summary(current, frozen),
            }
        )

    calibration_summary = []
    for (suite, revision, run, stage), values in sorted(calibration.items()):
        calibration_summary.append(
            {
                "suite": suite,
                "revision": revision,
                "run": run,
                "stage": stage,
                "ns_per_iter": statistics.median(values),
            }
        )

    return {
        "process_median_is_independent_unit": True,
        "bootstrap_seed": 2_282_026,
        "bootstrap_resamples": 10_000,
        "width_one": width,
        "batch": batch,
        "calibration": calibration_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out", type=Path, help="directory produced by run-round228-performance.sh")
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()
    summary = parse_logs(args.out)
    args.json.write_text(json.dumps(summary, indent=2) + "\n")
    print(
        f"width comparisons={len(summary['width_one'])} batch cells={len(summary['batch'])} "
        f"calibrations={len(summary['calibration'])}"
    )


if __name__ == "__main__":
    main()
