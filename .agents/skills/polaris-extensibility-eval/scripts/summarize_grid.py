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
"""Summarize a polaris-extensibility-eval grid run.

Reads run_dir/cells/<cell_id>/{verdict.txt, usage.json, timing.json,
worker.diff} and emits report.md + report.json with multi-axis tables.
"""
import json
import re
import sys
from pathlib import Path


def parse_cell_id(cell_id: str):
    """T-event-typed-attribute__base__haiku -> (task, arm, model_tag)."""
    parts = cell_id.split("__")
    if len(parts) != 3:
        return cell_id, "?", "?"
    return tuple(parts)


def load_cell(cell_dir: Path):
    cell_id = cell_dir.name
    task, arm, model = parse_cell_id(cell_id)
    out = {
        "cell_id": cell_id,
        "task": task,
        "arm": arm,
        "model": model,
        "verdict": (cell_dir / "verdict.txt").read_text().strip()
            if (cell_dir / "verdict.txt").exists() else "ERROR",
        "verifier_exit": int((cell_dir / "verifier_exit.txt").read_text().strip())
            if (cell_dir / "verifier_exit.txt").exists() else None,
        "worker_exit": int((cell_dir / "exit_code.txt").read_text().strip())
            if (cell_dir / "exit_code.txt").exists() else None,
    }
    timing = {}
    if (cell_dir / "timing.json").exists():
        timing = json.loads((cell_dir / "timing.json").read_text())
    usage = {}
    if (cell_dir / "usage.json").exists():
        try:
            usage = json.loads((cell_dir / "usage.json").read_text())
        except json.JSONDecodeError:
            usage = {}
    out["duration_seconds"] = timing.get("duration_seconds")
    out["cost_usd"] = usage.get("cost_usd")
    out["input_tokens"] = usage.get("input_tokens")
    out["output_tokens"] = usage.get("output_tokens")
    out["cache_read_input_tokens"] = usage.get("cache_read_input_tokens")
    out["cache_creation_input_tokens"] = usage.get("cache_creation_input_tokens")
    out["num_turns"] = usage.get("num_turns")
    out["stop_reason"] = usage.get("stop_reason")
    # Worker diff: count lines added/removed.
    diff_path = cell_dir / "worker.diff"
    if diff_path.exists():
        diff = diff_path.read_text()
        out["diff_lines_added"] = sum(
            1 for l in diff.splitlines() if l.startswith("+") and not l.startswith("+++"))
        out["diff_lines_removed"] = sum(
            1 for l in diff.splitlines() if l.startswith("-") and not l.startswith("---"))
        out["diff_files"] = sorted({
            l[6:] for l in diff.splitlines() if l.startswith("+++ b/")
        })
    return out


def categorize_delta(before: dict, after: dict, cost_threshold: float = 0.15):
    if before.get("verdict") == "ERROR" or after.get("verdict") == "ERROR":
        return "inconclusive_error"
    bv, av = before["verdict"], after["verdict"]
    if bv == "FAIL" and av == "PASS":
        return "improvement"
    if bv == "PASS" and av == "FAIL":
        return "regression"
    if bv == "FAIL" and av == "FAIL":
        return "inconclusive_both_fail"
    # PASS -> PASS: check cost delta
    bc = before.get("cost_usd") or 0
    ac = after.get("cost_usd") or 0
    bt = (before.get("input_tokens") or 0) + (before.get("output_tokens") or 0)
    at_ = (after.get("input_tokens") or 0) + (after.get("output_tokens") or 0)
    bd = before.get("duration_seconds") or 0
    ad = after.get("duration_seconds") or 0

    def pct(b, a):
        if not b:
            return None
        return (a - b) / b

    cost_pct = pct(bc, ac)
    tok_pct = pct(bt, at_)
    dur_pct = pct(bd, ad)
    metrics = [m for m in (cost_pct, tok_pct, dur_pct) if m is not None]
    if metrics:
        if max(metrics) > cost_threshold and all(m >= 0 for m in metrics if abs(m) > 0.05):
            return "soft_regression_by_cost"
        if min(metrics) < -cost_threshold and all(m <= 0 for m in metrics if abs(m) > 0.05):
            return "soft_improvement_by_cost"
    return "neutral"


def fmt(v, w=8, p=2):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{p}f}"
    return str(v)


def fmt_pct(b, a):
    if not b or a is None:
        return "—"
    pct = (a - b) / b * 100
    return f"{pct:+.1f}%"


def main(run_dir: str):
    run_path = Path(run_dir)
    cells_dir = run_path / "cells"
    if not cells_dir.is_dir():
        print(f"no cells dir: {cells_dir}", file=sys.stderr)
        sys.exit(1)
    cells = [load_cell(d) for d in sorted(cells_dir.iterdir()) if d.is_dir()]

    # Index by (task, arm, model)
    by_key = {(c["task"], c["arm"], c["model"]): c for c in cells}

    tasks = sorted({c["task"] for c in cells})
    # Order arms semantically: "base" first, then alphanum.
    def arm_order(a: str) -> tuple:
        if a == "base":
            return (0, a)
        if a.startswith("A"):
            return (1, a)
        if a.startswith("B"):
            return (2, a)
        return (9, a)
    arms = sorted({c["arm"] for c in cells}, key=arm_order)
    models = sorted({c["model"] for c in cells},
                    key=lambda m: {"haiku": 0, "sonnet": 1, "opus": 2}.get(m, 9))

    md = []
    md.append(f"# Polaris extensibility-eval grid run: {run_path.name}")
    md.append("")
    md.append(f"- Cells: {len(cells)}")
    md.append(f"- Tasks: {', '.join(tasks)}")
    md.append(f"- Arms: {', '.join(arms)}")
    md.append(f"- Models: {', '.join(models)}")
    total_cost = sum((c.get("cost_usd") or 0) for c in cells)
    total_dur = sum((c.get("duration_seconds") or 0) for c in cells)
    md.append(f"- Total cost: ${total_cost:.2f}")
    md.append(f"- Total wall: {total_dur//60:.0f}m {total_dur%60:.0f}s")
    md.append("")

    # Table 1: per-cell raw
    md.append("## Per-cell results (raw)")
    md.append("")
    md.append("| Task | Arm | Model | Verdict | Wall (s) | Cost ($) | Tokens (in→out) | Turns | Diff +/- |")
    md.append("|------|-----|-------|---------|----------|----------|------------------|-------|----------|")
    for c in cells:
        toks = f"{c.get('input_tokens', '—')}→{c.get('output_tokens', '—')}"
        diff = f"+{c.get('diff_lines_added','—')}/-{c.get('diff_lines_removed','—')}"
        md.append(f"| {c['task']} | {c['arm']} | {c['model']} | {c['verdict']} "
                  f"| {fmt(c.get('duration_seconds'))} "
                  f"| {fmt(c.get('cost_usd'),p=4)} | {toks} "
                  f"| {fmt(c.get('num_turns'))} | {diff} |")
    md.append("")

    # Table 2: A/B per (task, model). Use the lexicographic last arm
    # as "after"; assumes arm naming like "base" < "A2" < "B".
    if len(arms) >= 2:
        before_arm = arms[0]
        after_arms = arms[1:]
        for after_arm in after_arms:
            md.append(f"## A/B per (task, model): {before_arm} vs {after_arm}")
            md.append("")
            md.append("| Task | Model | Before | After | Δ wall | Δ cost | Δ tokens | Category |")
            md.append("|------|-------|--------|-------|--------|--------|----------|----------|")
            for task in tasks:
                for model in models:
                    b = by_key.get((task, before_arm, model))
                    a = by_key.get((task, after_arm, model))
                    if not b or not a:
                        continue
                    cat = categorize_delta(b, a)
                    bt = (b.get('input_tokens') or 0) + (b.get('output_tokens') or 0)
                    at_ = (a.get('input_tokens') or 0) + (a.get('output_tokens') or 0)
                    md.append(
                        f"| {task} | {model} "
                        f"| {b['verdict']} ({fmt(b.get('duration_seconds'))}s, ${fmt(b.get('cost_usd'),p=3)}) "
                        f"| {a['verdict']} ({fmt(a.get('duration_seconds'))}s, ${fmt(a.get('cost_usd'),p=3)}) "
                        f"| {fmt_pct(b.get('duration_seconds'), a.get('duration_seconds'))} "
                        f"| {fmt_pct(b.get('cost_usd'), a.get('cost_usd'))} "
                        f"| {fmt_pct(bt, at_)} "
                        f"| {cat} |")
            md.append("")

    # Table 3: model-spread per arm — does the change help small models more?
    md.append("## Model spread by arm")
    md.append("")
    md.append("| Task | Arm | Haiku | Sonnet | Opus |")
    md.append("|------|-----|-------|--------|------|")
    for task in tasks:
        for arm in arms:
            row = [task, arm]
            for model in ("haiku", "sonnet", "opus"):
                c = by_key.get((task, arm, model))
                if not c:
                    row.append("—")
                else:
                    row.append(f"{c['verdict']} ({fmt(c.get('duration_seconds'))}s, ${fmt(c.get('cost_usd'),p=3)}, {fmt(c.get('num_turns'))}t)")
            md.append("| " + " | ".join(row) + " |")
    md.append("")

    # Summary counts
    md.append("## Summary counts")
    md.append("")
    counts = {"PASS": 0, "FAIL": 0, "ERROR": 0}
    for c in cells:
        counts[c["verdict"]] = counts.get(c["verdict"], 0) + 1
    for v, n in counts.items():
        md.append(f"- {v}: {n}")
    md.append("")

    (run_path / "report.md").write_text("\n".join(md))
    (run_path / "report.json").write_text(json.dumps({
        "run_id": run_path.name,
        "cells": cells,
        "summary": counts,
        "total_cost_usd": total_cost,
        "total_wall_seconds": total_dur,
    }, indent=2))
    print(f"wrote {run_path/'report.md'}")
    print(f"wrote {run_path/'report.json'}")


if __name__ == "__main__":
    main(sys.argv[1])
