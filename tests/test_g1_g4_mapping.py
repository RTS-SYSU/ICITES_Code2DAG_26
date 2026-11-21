"""Minimal G1-G4 regression: rule, graph size, Condition/join/mutex mapping."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.composition import run_composition_blocks, select_rule
from Code2DAG.tests import test_composition_c1 as c1


ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "examples" / "g1_g4" / "sources"
EXPECTED = {
    "G1": dict(rule="conditional_branches", nodes=19, edges=25, snodes=21, sedges=27,
               regions=1, locks=0, creates=6, joins=7),
    "G2": dict(rule="conditional_branches", nodes=22, edges=29, snodes=24, sedges=31,
               regions=1, locks=0, creates=7, joins=8),
    "G3": dict(rule="conditional_branches", nodes=26, edges=36, snodes=36, sedges=46,
               regions=5, locks=0, creates=6, joins=11),
    "G4": dict(rule="composition_c2c", nodes=31, edges=42, snodes=41, sedges=52,
               regions=5, locks=2, creates=7, joins=12),
}


def reachable(edges, source, target):
    todo = [source]
    seen = set()
    while todo:
        node = todo.pop()
        if node == target:
            return True
        if node not in seen:
            seen.add(node)
            todo.extend(e["dst"] for e in edges if e["src"] == node)
    return False


class G1G4MappingTests(unittest.TestCase):
    def test_g1_g4(self) -> None:
        helper = c1.CompositionC1Tests()
        for name, expected in EXPECTED.items():
            with self.subTest(program=name):
                source = SOURCES / f"{name}.c"
                before = source.read_bytes()
                self.assertEqual(expected["rule"], select_rule(source.read_text()))
                with tempfile.TemporaryDirectory(prefix=f"gx_{name}_") as tmp:
                    base, pipeline = helper.native(Path(tmp), source)
                    run_composition_blocks(base_dir=base, base_name=name, source_file=source)
                    block = pipeline / "blocks" / "level2" / expected["rule"]
                    s = json.loads((block / "segments.json").read_text())
                    d = json.loads((block / "dag_seg.json").read_text())
                    st = json.loads((block / "segments_struct.json").read_text())
                    sd = json.loads((block / "dag_struct.json").read_text())

                self.assertEqual(expected["nodes"], len(d["nodes"]))
                self.assertEqual(expected["edges"], len(d["edges"]))
                self.assertEqual(expected["snodes"], len(sd["nodes"]))
                self.assertEqual(expected["sedges"], len(sd["edges"]))
                self.assertEqual(expected["regions"], len(s.get("condition_regions", [])))
                self.assertEqual(expected["locks"], len(s.get("lock_groups", [])))
                self.assertEqual(expected["creates"], sum(1 for e in d["edges"] if e["kind"] == "create"))
                self.assertEqual(expected["joins"], sum(1 for e in d["edges"] if e["kind"] == "join"))
                self.assertFalse(any(e["src"] == e["dst"] for e in d["edges"]))
                self.assertEqual(set(d["nodes"]), {r["seg_id"] for r in s["segments"]})
                self.assertEqual(set(sd["nodes"]), {r["seg_id"] for r in st["segments"]})
                topo_sort(d["nodes"], [(e["src"], e["dst"]) for e in d["edges"]])
                topo_sort(sd["nodes"], [(e["src"], e["dst"]) for e in sd["edges"]])

                regions = {r["condition_id"]: r for r in s["condition_regions"]}
                for region in regions.values():
                    self.assertTrue(region.get("branches"))
                    if region.get("parent_id") is not None:
                        self.assertIn(region["parent_id"], regions)
                for row in s["segments"]:
                    for member in row.get("condition_memberships", []) or []:
                        self.assertIn(member["exclusive_group"], regions)
                        self.assertIn(
                            member["branch_id"],
                            {b["branch_id"] for b in regions[member["exclusive_group"]]["branches"]},
                        )

                # Then/else nodes in one cycle-free condition never reach each other.
                conditional = [r for r in s["segments"] if r.get("condition_memberships")]
                for i, left in enumerate(conditional):
                    for right in conditional[i + 1:]:
                        lm = {m["exclusive_group"]: m["branch_id"] for m in left["condition_memberships"]}
                        rm = {m["exclusive_group"]: m["branch_id"] for m in right["condition_memberships"]}
                        if any(k in rm and lm[k] != rm[k] for k in lm):
                            self.assertFalse(reachable(d["edges"], left["seg_id"], right["seg_id"]))
                            self.assertFalse(reachable(d["edges"], right["seg_id"], left["seg_id"]))

                if name == "G4":
                    groups = s["lock_groups"]
                    self.assertEqual({"m1"}, {g["mutex_id"] for g in groups})
                    self.assertEqual({"task_5", "task_7"},
                                     {g["members"][0]["function"] for g in groups})
                    grouped = s["mappings"]["grouped_node_to_member_segments"]
                    for group in groups:
                        self.assertTrue(group["member_segment_ids"])
                        self.assertIn(group["group_id"], set(d["nodes"]))
                        self.assertEqual(group["member_segment_ids"], grouped[group["group_id"]])
                    roots = [r for r in regions.values() if r.get("parent_id") is None]
                    self.assertEqual(3, len(roots))
                else:
                    self.assertEqual([], s.get("lock_groups", []))

                self.assertEqual(before, source.read_bytes())


if __name__ == "__main__":
    unittest.main()
