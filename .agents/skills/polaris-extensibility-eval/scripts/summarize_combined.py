#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Combine multiple grid sub-runs into one comprehensive report.

Usage: summarize_combined.py <out_dir> <sub_run_dir> [<sub_run_dir>...]
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from summarize_grid import (
    load_cell, parse_cell_id, fmt, fmt_pct, categorize_delta
)


def arm_order(a: str) -> tuple:
    if a == "base": return (0, a)
    if a.startswith("A"): return (1, a)
    if a.startswith("B"): return (2, a)
    return (9, a)


def main(out_dir: str, sub_dirs: list[str]):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    cells = []
    for sd in sub_dirs:
        cells_dir = Path(sd) / "cells"
        if not cells_dir.is_dir():
            continue
        for d in sorted(cells_dir.iterdir()):
            if d.is_dir():
                cells.append(load_cell(d))

    by_key = {(c["task"], c["arm"], c["model"]): c for c in cells}
    tasks = sorted({c["task"] for c in cells})
    arms = sorted({c["arm"] for c in cells}, key=arm_order)
    models = sorted({c["model"] for c in cells},
                    key=lambda m: {"haiku": 0, "sonnet": 1, "opus": 2}.get(m, 9))

    md = []
    md.append("# Polaris extensibility-eval — comprehensive grid summary")
    md.append("")
    total_cost = sum((c.get("cost_usd") or 0) for c in cells)
    total_dur = sum((c.get("duration_seconds") or 0) for c in cells)
    counts = {"PASS": 0, "FAIL": 0, "ERROR": 0}
    for c in cells:
        counts[c["verdict"]] = counts.get(c["verdict"], 0) + 1
    md.append(f"- Cells: {len(cells)}  (PASS: {counts.get('PASS',0)}, FAIL: {counts.get('FAIL',0)}, ERROR: {counts.get('ERROR',0)})")
    md.append(f"- Tasks: {', '.join(tasks)}")
    md.append(f"- Arms (in order): {', '.join(arms)}")
    md.append(f"- Models: {', '.join(models)}")
    md.append(f"- Total cost: ${total_cost:.2f}")
    md.append(f"- Total wall: {int(total_dur//60)}m {int(total_dur%60)}s")
    md.append("")

    # All cells, grouped per (task, arm, model)
    md.append("## Full cell matrix")
    md.append("")
    md.append("| Task | Arm | Model | Verdict | Wall (s) | Cost ($) | Tokens (in→out) | Turns |")
    md.append("|------|-----|-------|---------|----------|----------|-----------------|-------|")
    for task in tasks:
        for arm in arms:
            for model in models:
                c = by_key.get((task, arm, model))
                if not c:
                    md.append(f"| {task} | {arm} | {model} | — | — | — | — | — |")
                    continue
                toks = f"{c.get('input_tokens','—')}→{c.get('output_tokens','—')}"
                md.append(f"| {task} | {arm} | {model} | {c['verdict']} "
                          f"| {fmt(c.get('duration_seconds'))} | {fmt(c.get('cost_usd'),p=4)} "
                          f"| {toks} | {fmt(c.get('num_turns'))} |")
    md.append("")

    # All-arm pivot: base as anchor, show every other arm's delta
    if "base" in arms:
        for after_arm in [a for a in arms if a != "base"]:
            md.append(f"## A/B per (task, model): base vs {after_arm}")
            md.append("")
            md.append("| Task | Model | base | "
                      f"{after_arm} | Δ wall | Δ cost | Δ turns | Category |")
            md.append("|------|-------|------|------|--------|--------|---------|----------|")
            for task in tasks:
                for model in models:
                    b = by_key.get((task, "base", model))
                    a = by_key.get((task, after_arm, model))
                    if not b or not a:
                        continue
                    cat = categorize_delta(b, a)
                    md.append(
                        f"| {task} | {model} "
                        f"| {b['verdict']} ({fmt(b.get('duration_seconds'))}s "
                        f"${fmt(b.get('cost_usd'),p=3)}, {fmt(b.get('num_turns'))}t) "
                        f"| {a['verdict']} ({fmt(a.get('duration_seconds'))}s "
                        f"${fmt(a.get('cost_usd'),p=3)}, {fmt(a.get('num_turns'))}t) "
                        f"| {fmt_pct(b.get('duration_seconds'), a.get('duration_seconds'))} "
                        f"| {fmt_pct(b.get('cost_usd'), a.get('cost_usd'))} "
                        f"| {fmt_pct(b.get('num_turns'), a.get('num_turns'))} "
                        f"| {cat} |")
            md.append("")

    # Per-model: how does the change help model X across the arms?
    md.append("## Per-model spread across arms")
    md.append("")
    for task in tasks:
        md.append(f"### Task {task}")
        md.append("")
        md.append("| Model | " + " | ".join(arms) + " |")
        md.append("|-------|" + "|".join("------" for _ in arms) + "|")
        for model in models:
            row = [model]
            for arm in arms:
                c = by_key.get((task, arm, model))
                if not c:
                    row.append("—")
                else:
                    row.append(
                        f"{c['verdict']} ({fmt(c.get('duration_seconds'))}s "
                        f"${fmt(c.get('cost_usd'),p=3)} {fmt(c.get('num_turns'))}t)"
                    )
            md.append("| " + " | ".join(row) + " |")
        md.append("")

    # Cost-per-model totals
    md.append("## Cost spend by model")
    md.append("")
    md.append("| Model | Cells | Total cost | Avg cost | Avg wall | Avg turns |")
    md.append("|-------|-------|------------|----------|----------|-----------|")
    for model in models:
        mcells = [c for c in cells if c["model"] == model]
        n = len(mcells)
        cost = sum((c.get("cost_usd") or 0) for c in mcells)
        wall = sum((c.get("duration_seconds") or 0) for c in mcells)
        turns = sum((c.get("num_turns") or 0) for c in mcells)
        avg_cost = cost / n if n else 0
        avg_wall = wall / n if n else 0
        avg_turns = turns / n if n else 0
        md.append(f"| {model} | {n} | ${cost:.2f} | ${avg_cost:.3f} | {avg_wall:.0f}s | {avg_turns:.1f} |")
    md.append("")

    out_md = out_path / "report.md"
    out_md.write_text("\n".join(md))
    out_json = out_path / "report.json"
    out_json.write_text(json.dumps({
        "cells": cells,
        "summary": counts,
        "total_cost_usd": total_cost,
        "total_wall_seconds": total_dur,
    }, indent=2))
    print(f"wrote {out_md}")
    print(f"wrote {out_json}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
