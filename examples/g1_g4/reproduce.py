#!/usr/bin/env python3
"""Reproduce the G1-G4 structural mapping used by the extended-DAG examples.

Run from the repository root:
    python3 examples/g1_g4/reproduce.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT / "examples"))

from _native import build_native
from Code2DAG.pipeline.composition import run_composition_blocks, select_rule

SOURCES = Path(__file__).resolve().parent / "sources"


def main() -> int:
    summaries = []
    for source in sorted(SOURCES.glob("*.c")):
        text = source.read_text()
        rule = select_rule(text)
        with tempfile.TemporaryDirectory(prefix=f"code2dag_{source.stem}_") as tmp:
            base, pipeline = build_native(source, Path(tmp))
            run_composition_blocks(base_dir=base, base_name=source.stem, source_file=source)
            block = pipeline / "blocks" / "level2" / rule
            segments = json.loads((block / "segments.json").read_text())
            dag = json.loads((block / "dag_seg.json").read_text())
            struct = json.loads((block / "dag_struct.json").read_text())
        groups = segments.get("lock_groups", [])
        summaries.append(
            {
                "program": source.stem,
                "rule": rule,
                "nodes": len(dag["nodes"]),
                "edges": len(dag["edges"]),
                "struct_nodes": len(struct["nodes"]),
                "struct_edges": len(struct["edges"]),
                "condition_regions": len(segments.get("condition_regions", [])),
                "create_edges": sum(1 for e in dag["edges"] if e.get("kind") == "create"),
                "join_edges": sum(1 for e in dag["edges"] if e.get("kind") == "join"),
                "lock_groups": [
                    {
                        "group_id": g["group_id"],
                        "mutex_id": g["mutex_id"],
                        "members": [m["function"] for m in g["members"]],
                    }
                    for g in groups
                ],
            }
        )
    print(json.dumps(summaries, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
