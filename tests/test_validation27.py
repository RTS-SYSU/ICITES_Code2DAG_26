"""Structural regression for the fixed 27-program C1/C2/C3/C4/C5 corpus.

The test runs the public pipeline from the checked-in C sources.  It does not
re-execute runtime traces and does not compare against experiment records; it
checks rule selection, graph/mapping consistency, and the declared Condition
and Loop metadata.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.composition import run_composition_blocks, select_rule
from Code2DAG.tests import test_composition_c1 as c1


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "tests" / "validation27"
PROGRAMS = json.loads((CORPUS / "programs.json").read_text())["programs"]
CONTROL = {"condition_entry", "condition_exit", "loop_entry", "loop_exit"}


class Validation27Tests(unittest.TestCase):
    def test_all_27_programs(self) -> None:
        helper = c1.CompositionC1Tests()
        for prog in PROGRAMS:
            with self.subTest(program=prog["name"]):
                source = CORPUS / "programs" / prog["file"]
                before = source.read_bytes()
                text = source.read_text()
                self.assertEqual(prog["rule"], select_rule(text))

                with tempfile.TemporaryDirectory(prefix="v27_") as tmp:
                    base, pipeline = helper.native(Path(tmp), source)
                    run_composition_blocks(
                        base_dir=base, base_name=source.stem, source_file=source
                    )
                    block = pipeline / "blocks" / "level2" / prog["rule"]
                    sched_segments = json.loads((block / "segments.json").read_text())
                    struct_segments = json.loads((block / "segments_struct.json").read_text())
                    sched_dag = json.loads((block / "dag_seg.json").read_text())
                    struct_dag = json.loads((block / "dag_struct.json").read_text())

                self.assertTrue(sched_segments.get("capabilities", {}).get("scheduler_compatible"))
                sched_nodes = [row["seg_id"] for row in sched_segments["segments"]]
                self.assertEqual(len(sched_nodes), len(set(sched_nodes)))
                self.assertEqual(set(sched_nodes), set(sched_dag["nodes"]))
                struct_nodes = {row["seg_id"] for row in struct_segments["segments"]}
                self.assertTrue({e["src"] for e in sched_dag["edges"]} <= set(sched_nodes))
                self.assertTrue({e["dst"] for e in sched_dag["edges"]} <= set(sched_nodes))
                self.assertTrue({e["src"] for e in struct_dag["edges"]} <= struct_nodes)
                self.assertTrue({e["dst"] for e in struct_dag["edges"]} <= struct_nodes)
                self.assertFalse(any(e["src"] == e["dst"] for e in sched_dag["edges"]))
                topo_sort(sched_nodes, [(e["src"], e["dst"]) for e in sched_dag["edges"]])
                topo_sort(sorted(struct_nodes), [(e["src"], e["dst"]) for e in struct_dag["edges"]])

                regions = {r["condition_id"]: r for r in sched_segments.get("condition_regions", [])}
                self.assertGreaterEqual(len(regions), len(prog.get("conditions", [])))
                for region in regions.values():
                    branches = {b["branch_id"] for b in region.get("branches", [])}
                    self.assertTrue(branches)
                    parent = region.get("parent_id")
                    if parent is not None:
                        self.assertIn(parent, regions)
                for row in sched_segments["segments"] + struct_segments["segments"]:
                    seen = {}
                    for member in row.get("condition_memberships", []) or []:
                        group = member["exclusive_group"]
                        branch = member["branch_id"]
                        self.assertIn(group, regions)
                        self.assertIn(branch, {b["branch_id"] for b in regions[group]["branches"]})
                        self.assertNotIn(group, seen)
                        seen[group] = branch
                    if row["kind"] in CONTROL:
                        continue

                loops = sched_segments.get("loops", [])
                self.assertGreaterEqual(len(loops), len(prog.get("loops", [])))
                loop_ids = {loop["loop_id"] for loop in loops}
                for loop in loops:
                    self.assertIsInstance(loop.get("iteration_count"), int)
                    self.assertGreaterEqual(loop["iteration_count"], 0)
                    if loop.get("parent_loop_id") is not None:
                        self.assertIn(loop["parent_loop_id"], loop_ids)

                mappings = sched_segments.get("mappings", {})
                for field, values in mappings.items():
                    if not isinstance(values, dict):
                        continue
                    if field.endswith("_to_execution_segments"):
                        allowed = set(sched_nodes)
                        if field == "template_to_execution_segments":
                            allowed |= {
                                t["template_id"]
                                for key in ("templates", "source_component_templates")
                                for t in sched_segments.get(key, [])
                            }
                        for target in values.values():
                            target = [target] if isinstance(target, str) else target
                            self.assertTrue(set(target) <= allowed, (prog["name"], field))
                    if field.endswith("_to_structural_nodes"):
                        for target in values.values():
                            target = [target] if isinstance(target, str) else target
                            self.assertTrue(set(target) <= struct_nodes, (prog["name"], field))
                self.assertEqual(before, source.read_bytes())


if __name__ == "__main__":
    unittest.main()
