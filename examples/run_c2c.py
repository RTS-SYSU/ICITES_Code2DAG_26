#!/usr/bin/env python3
"""Build the C2c (Condition + independent Lock, no static Loop) example.

Run from the repository root:
    python3 examples/run_c2c.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))

from _native import build_native
from Code2DAG.pipeline.composition import run_composition_blocks, select_rule

SOURCE = ROOT / "source_files" / "composition_c2c" / "benchmark.c"


def main() -> int:
    text = SOURCE.read_text()
    rule = select_rule(text)
    if rule != "composition_c2c":
        raise SystemExit(f"unexpected rule: {rule}")
    with tempfile.TemporaryDirectory(prefix="code2dag_c2c_") as tmp:
        base, pipeline = build_native(SOURCE, Path(tmp))
        run_composition_blocks(base_dir=base, base_name=SOURCE.stem, source_file=SOURCE)
        block = pipeline / "blocks" / "level2" / rule
        segments = json.loads((block / "segments.json").read_text())
        dag = json.loads((block / "dag_seg.json").read_text())
    groups = segments.get("lock_groups", [])
    summary = {
        "rule": rule,
        "source": str(SOURCE.relative_to(ROOT)),
        "analysis_only": segments.get("capabilities", {}).get("analysis_only"),
        "nodes": len(dag["nodes"]),
        "edges": len(dag["edges"]),
        "condition_regions": len(segments.get("condition_regions", [])),
        "lock_groups": [
            {
                "group_id": g["group_id"],
                "mutex_id": g["mutex_id"],
                "member_segment_ids": g["member_segment_ids"],
            }
            for g in groups
        ],
        "create_edges": sum(1 for e in dag["edges"] if e.get("kind") == "create"),
        "join_edges": sum(1 for e in dag["edges"] if e.get("kind") == "join"),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
