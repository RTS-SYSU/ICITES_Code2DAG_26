from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from Code2DAG.level1.time_analysis_level1 import Segment, instrument_source
from Code2DAG.pipeline.condition_model import analyze_conditionals, build_condition_views
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.errors import StageError
from Code2DAG.pipeline.instrument_levelx import instrument_prio_all_segments_by_start_line
from Code2DAG.pipeline.runner import _write_rule_outputs, default_rule_for_level
from Code2DAG.pipeline.rules.base import RuleOutput
from Code2DAG.pipeline.rules_registry import list_rules
from Code2DAG.pipeline.timing import run_timing


SIMPLE_SOURCE = """int main(void) {
    setup();
    if (flag) {
        fast();
    } else {
        slow();
    }
    finish();
}
"""


def _base_payload(source: str = SIMPLE_SOURCE):
    line_count = len(source.splitlines())
    segments = {
        "base_name": "sample",
        "node_locations": {
            "main/setup1": {"function": "main", "line": 2},
            "main/fast2": {"function": "main", "line": 4},
            "main/slow3": {"function": "main", "line": 6},
            "main/finish4": {"function": "main", "line": 8},
        },
        "segments": [
            {
                "seg_id": "SEG:main#001@2-8",
                "function": "main",
                "kind": "compute",
                "start_line": 2,
                "end_line": min(8, line_count - 1),
            }
        ],
    }
    dag = {"base_name": "sample", "nodes": ["SEG:main#001@2-8"], "edges": []}
    return segments, dag


class ConditionalAnalysisTests(unittest.TestCase):
    def test_detects_paired_if_else_and_branch_ranges(self) -> None:
        analysis = analyze_conditionals(SIMPLE_SOURCE, {"main": (2, 8)})
        self.assertEqual([], analysis.diagnostics)
        self.assertEqual(1, len(analysis.regions))
        region = analysis.regions[0]
        self.assertEqual((3, 7), (region.entry_line, region.exit_line))
        self.assertEqual([("then", 4, 4), ("else", 6, 6)], [(b.branch_id, b.start_line, b.end_line) for b in region.branches])

    def test_nested_condition_is_owned_by_the_containing_branch(self) -> None:
        source = """int main(void) {
    if (outer) {
        if (inner) {
            a();
        } else {
            b();
        }
    } else {
        c();
    }
}
"""
        analysis = analyze_conditionals(source, {"main": (2, 10)})
        self.assertEqual(2, len(analysis.regions))
        outer, inner = analysis.regions
        self.assertIsNone(outer.parent_id)
        self.assertEqual(outer.condition_id, inner.parent_id)
        self.assertEqual("then", inner.parent_branch_id)


class ConditionalViewTests(unittest.TestCase):
    def test_structural_and_schedule_views_keep_branches_separate(self) -> None:
        base_segments, base_dag = _base_payload()
        struct_segments, struct_dag, sched_segments, sched_dag, meta = build_condition_views(
            base_name="sample",
            source_text=SIMPLE_SOURCE,
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )

        kinds = [s["kind"] for s in struct_segments["segments"]]
        self.assertEqual(1, kinds.count("condition_entry"))
        self.assertEqual(1, kinds.count("condition_exit"))
        branch_edges = [e for e in struct_dag["edges"] if e["kind"] == "condition_branch"]
        self.assertEqual({"then", "else"}, {e["branch_id"] for e in branch_edges})
        self.assertEqual(1, len({e["exclusive_group"] for e in branch_edges}))
        topo_sort(struct_dag["nodes"], [(e["src"], e["dst"]) for e in struct_dag["edges"]])
        branch_nodes = {
            str(segment["branch_id"]).rsplit(":", 1)[-1]: segment["seg_id"]
            for segment in struct_segments["segments"]
            if segment.get("branch_id")
        }
        edge_pairs = {(e["src"], e["dst"]) for e in struct_dag["edges"]}
        self.assertNotIn((branch_nodes["then"], branch_nodes["else"]), edge_pairs)
        self.assertNotIn((branch_nodes["else"], branch_nodes["then"]), edge_pairs)

        self.assertNotIn("conditional_atomic", {s["kind"] for s in sched_segments["segments"]})
        self.assertNotIn("condition_entry", {s["kind"] for s in sched_segments["segments"]})
        self.assertNotIn("condition_exit", {s["kind"] for s in sched_segments["segments"]})
        scheduled_branches = {
            membership["branch_id"]: segment["seg_id"]
            for segment in sched_segments["segments"]
            for membership in segment.get("condition_memberships", [])
        }
        self.assertEqual({"then", "else"}, set(scheduled_branches))
        schedule_branch_edges = [e for e in sched_dag["edges"] if e["kind"] == "condition_branch"]
        self.assertEqual({"then", "else"}, {e["branch_id"] for e in schedule_branch_edges})
        self.assertEqual(1, len(sched_dag["exclusive_groups"]))
        topo_sort(sched_dag["nodes"], [(e["src"], e["dst"]) for e in sched_dag["edges"]])
        mapping = sched_segments["mappings"]["original_node_to_execution_segment"]
        self.assertEqual(scheduled_branches["then"], mapping["main/fast2"])
        self.assertEqual(scheduled_branches["else"], mapping["main/slow3"])
        self.assertNotEqual(mapping["main/fast2"], mapping["main/slow3"])
        self.assertTrue(meta["scheduler_compatible"])

        timing = {
            "weights": {
                segment_id: {"avg_ns": 1}
                for segment_id in sched_dag["nodes"]
            }
        }
        schedule = get_algo("FIFO").compute(
            dag_json=sched_dag,
            segments_json=sched_segments,
            timing_json=timing,
        )
        self.assertEqual(set(sched_dag["nodes"]), set(schedule["priorities"]))
        self.assertIn(scheduled_branches["then"], schedule["priorities"])
        self.assertIn(scheduled_branches["else"], schedule["priorities"])

    def test_early_control_transfer_blocks_existing_scheduler(self) -> None:
        source = """int main(void) {
    if (flag)
        return 1;
    finish();
}
"""
        base_segments = {
            "segments": [
                {"seg_id": "S", "function": "main", "kind": "compute", "start_line": 2, "end_line": 4}
            ]
        }
        base_dag = {"nodes": ["S"], "edges": []}
        _ss, _sd, sched_segments, _dd, meta = build_condition_views(
            base_name="early",
            source_text=source,
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )
        self.assertFalse(meta["scheduler_compatible"])
        self.assertFalse(sched_segments["capabilities"]["scheduler_compatible"])
        self.assertIn("return/break/continue/goto", meta["blocked_reasons"][0])

    def test_same_line_branches_are_rejected_for_independent_instrumentation(self) -> None:
        source = """int main(void) {
    if (flag) { fast(); } else { slow(); }
    finish();
}
"""
        base_segments = {
            "segments": [
                {"seg_id": "S", "function": "main", "kind": "compute", "start_line": 2, "end_line": 3}
            ]
        }
        base_dag = {"nodes": ["S"], "edges": []}
        _ss, _sd, sched_segments, _dd, meta = build_condition_views(
            base_name="same_line",
            source_text=source,
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )
        self.assertFalse(meta["scheduler_compatible"])
        self.assertFalse(sched_segments["capabilities"]["scheduler_compatible"])
        self.assertIn("share source lines", meta["blocked_reasons"][0])

    def test_branch_code_sharing_control_boundary_lines_is_rejected(self) -> None:
        bodies = [
            "    if (flag) { fast();\n    } else {\n        slow();\n    }\n",
            "    if (flag) {\n        fast();\n    } else { slow();\n    }\n",
            "    if (flag) {\n        fast(); } else {\n        slow();\n    }\n",
            "    if (flag) {\n        fast();\n    } else {\n        slow(); }\n",
            "    if (\n        flag\n    ) { fast();\n    } else {\n        slow();\n    }\n",
        ]
        for body in bodies:
            with self.subTest(body=body):
                source = "int main(void) {\n" + body + "}\n"
                base_segments = {"segments": [{"seg_id": "BASE", "function": "main", "kind": "compute",
                    "start_line": 2, "end_line": len(source.splitlines()) - 1}]}
                st, _sd, ss, _dd, meta = build_condition_views(base_name="unsafe_line", source_text=source,
                    base_segments_json=base_segments, base_dag_json={"nodes": ["BASE"], "edges": []})
                self.assertFalse(meta["scheduler_compatible"])
                self.assertFalse(ss["capabilities"]["scheduler_compatible"])
                self.assertTrue(any("control boundary" in reason for reason in meta["blocked_reasons"]))
                self.assertEqual(1, sum(s["kind"] == "condition_entry" for s in st["segments"]))
                self.assertEqual(1, sum(s["kind"] == "condition_exit" for s in st["segments"]))

    def test_cross_thread_dependency_is_preserved_with_separate_branch_nodes(self) -> None:
        source = SIMPLE_SOURCE + "\nvoid worker(void) {\n    work();\n}\n"
        base_segments, _base_dag = _base_payload(source)
        base_segments["segments"] = [
            {"seg_id": "SETUP", "function": "main", "kind": "compute", "start_line": 2, "end_line": 2},
            {"seg_id": "FAST", "function": "main", "kind": "compute", "start_line": 4, "end_line": 4},
            {"seg_id": "SLOW", "function": "main", "kind": "compute", "start_line": 6, "end_line": 6},
            {"seg_id": "FINISH", "function": "main", "kind": "compute", "start_line": 8, "end_line": 8},
            {"seg_id": "WORKER", "function": "worker", "kind": "compute", "start_line": 11, "end_line": 11},
        ]
        base_dag = {
            "nodes": ["SETUP", "FAST", "SLOW", "FINISH", "WORKER"],
            "edges": [{"src": "FAST", "dst": "WORKER", "kind": "create"}],
        }
        _ss, _sd, sched_segments, sched_dag, meta = build_condition_views(
            base_name="cross",
            source_text=source,
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )
        self.assertTrue(meta["scheduler_compatible"])
        self.assertEqual([], sched_segments["capabilities"]["blocked_reasons"])
        self.assertIn("create", {edge["kind"] for edge in sched_dag["edges"]})

    def test_condition_at_thread_boundaries_preserves_all_dependency_endpoints(self) -> None:
        source = """void worker(void) {
    if (flag) {
        fast();
    } else {
        slow();
    }
}
int main(void) {
    spawn();
    join();
}
"""
        base_segments = {"segments": [
            {"seg_id": "WORKER", "function": "worker", "kind": "compute", "start_line": 2, "end_line": 6},
            {"seg_id": "CREATE", "function": "main", "kind": "compute", "start_line": 9, "end_line": 9},
            {"seg_id": "JOIN", "function": "main", "kind": "compute", "start_line": 10, "end_line": 10},
        ]}
        for incoming, outgoing in [("create", "join"), ("sem_dep", "sem_dep")]:
            with self.subTest(incoming=incoming, outgoing=outgoing):
                base_dag = {"nodes": ["WORKER", "CREATE", "JOIN"], "edges": [
                    {"src": "CREATE", "dst": "WORKER", "kind": incoming},
                    {"src": "WORKER", "dst": "JOIN", "kind": outgoing},
                    {"src": "CREATE", "dst": "JOIN", "kind": "intra"},
                ]}
                st, sd, ss, dd, meta = build_condition_views(
                    base_name="boundary", source_text=source,
                    base_segments_json=base_segments, base_dag_json=base_dag,
                )
                self.assertTrue(meta["scheduler_compatible"])
                by_line = {s["start_line"]: s["seg_id"] for s in ss["segments"]}
                entry = next(s["seg_id"] for s in st["segments"] if s["kind"] == "condition_entry")
                exit_node = next(s["seg_id"] for s in st["segments"] if s["kind"] == "condition_exit")
                structural = {(e["src"], e["dst"], e["kind"]) for e in sd["edges"]}
                self.assertIn((by_line[9], entry, incoming), structural)
                self.assertIn((exit_node, by_line[10], outgoing), structural)
                for line, branch in [(3, "then"), (5, "else")]:
                    for src, dst, kind in [(by_line[9], by_line[line], incoming), (by_line[line], by_line[10], outgoing)]:
                        edge = next(e for e in dd["edges"] if (e["src"], e["dst"], e["kind"]) == (src, dst, kind))
                        self.assertEqual(branch, edge["branch_id"])
                        self.assertEqual("COND:worker#001", edge["exclusive_group"])
                        self.assertTrue(edge["condition_transitions"])
                pairs = {(e["src"], e["dst"]) for e in dd["edges"]}
                self.assertNotIn((by_line[3], by_line[5]), pairs)
                self.assertNotIn((by_line[5], by_line[3]), pairs)
                topo_sort(dd["nodes"], [(e["src"], e["dst"]) for e in dd["edges"]])
                for algo in ["FIFO", "LPF", "heft", "t_level", "zhao2020"]:
                    schedule = get_algo(algo).compute(dag_json=dd, segments_json=ss,
                        timing_json={"weights": {n: {"avg_ns": 1} for n in dd["nodes"]}})
                    self.assertEqual(set(dd["nodes"]), set(schedule["priorities"]))

    def test_branch_timing_probes_are_inserted_inside_each_branch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "sample.c"
            output = Path(tmp) / "instrumented.c"
            source.write_text(SIMPLE_SOURCE, encoding="utf-8")
            segments = [
                Segment("BR:main#then@4", "main", "compute", 4, 4),
                Segment("BR:main#else@6", "main", "compute", 6, 6),
            ]
            warnings, skipped = instrument_source(source, segments, output)
            rendered = output.read_text(encoding="utf-8")
        self.assertEqual([], warnings)
        self.assertEqual([], skipped)
        self.assertLess(rendered.index("if (flag)"), rendered.index('SEG_BEGIN("BR:main#then@4")'))
        self.assertLess(rendered.index('SEG_END("BR:main#then@4")'), rendered.index("    } else {"))
        self.assertLess(rendered.index("    } else {"), rendered.index('SEG_BEGIN("BR:main#else@6")'))
        self.assertLess(rendered.index('SEG_END("BR:main#else@6")'), rendered.index("    finish();"))


class PipelineIntegrationTests(unittest.TestCase):
    def test_single_if_else_end_to_end_preserves_selected_branch(self) -> None:
        gcc = shutil.which("gcc")
        if gcc is None:
            self.skipTest("gcc is required for the end-to-end condition test")

        source_text = """#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    int flag = argc > 1 ? atoi(argv[1]) : 0;
    if (flag) {
        puts("then");
    } else {
        puts("else");
    }
    return 0;
}
"""
        base_segments = {
            "base_name": "condition_e2e",
            "node_locations": {
                "main/then": {"function": "main", "line": 7},
                "main/else": {"function": "main", "line": 9},
            },
            "segments": [
                {
                    "seg_id": "BASE",
                    "function": "main",
                    "kind": "compute",
                    "start_line": 5,
                    "end_line": 11,
                }
            ],
        }
        base_dag = {"base_name": "condition_e2e", "nodes": ["BASE"], "edges": []}
        _struct_segments, _struct_dag, sched_segments, sched_dag, meta = build_condition_views(
            base_name="condition_e2e",
            source_text=source_text,
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )
        self.assertTrue(meta["scheduler_compatible"])

        mapping = sched_segments["mappings"]["original_node_to_execution_segment"]
        then_id = mapping["main/then"]
        else_id = mapping["main/else"]
        self.assertNotEqual(then_id, else_id)

        timing = {
            "weights": {
                segment_id: {"avg_ns": 1}
                for segment_id in sched_dag["nodes"]
            }
        }
        schedule = get_algo("FIFO").compute(
            dag_json=sched_dag,
            segments_json=sched_segments,
            timing_json=timing,
        )
        self.assertIn(then_id, schedule["priorities"])
        self.assertIn(else_id, schedule["priorities"])

        def compile_source(path: Path, output: Path) -> None:
            proc = subprocess.run(
                [gcc, "-std=c11", str(path), "-o", str(output)],
                cwd=str(path.parent),
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, proc.returncode, proc.stderr)

        def run_binary(path: Path, flag: str) -> List[str]:
            proc = subprocess.run([str(path), flag], capture_output=True, text=True)
            self.assertEqual(0, proc.returncode, proc.stderr)
            return [line.strip() for line in proc.stdout.splitlines() if line.strip()]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "condition.c"
            source.write_text(source_text, encoding="utf-8")

            original = root / "original.exe"
            compile_source(source, original)
            original_then = run_binary(original, "1")
            original_else = run_binary(original, "0")

            branch_segments = [
                Segment(str(segment["seg_id"]), "main", str(segment["kind"]), int(segment["start_line"]), int(segment["end_line"]))
                for segment in sched_segments["segments"]
                if str(segment["seg_id"]) in {then_id, else_id}
            ]
            (root / "segtrace.h").write_text(
                '#include <stdio.h>\n#define SEG_BEGIN(ID) printf("BEGIN:%s\\n", (ID))\n#define SEG_END(ID) printf("END:%s\\n", (ID))\n',
                encoding="utf-8",
            )
            timed_source = root / "condition_timed.c"
            warnings, skipped = instrument_source(source, branch_segments, timed_source)
            self.assertEqual([], warnings)
            self.assertEqual([], skipped)
            timed = root / "timed.exe"
            compile_source(timed_source, timed)
            timed_then = run_binary(timed, "1")
            timed_else = run_binary(timed, "0")
            self.assertTrue(any(then_id in line for line in timed_then))
            self.assertFalse(any(else_id in line for line in timed_then))
            self.assertTrue(any(else_id in line for line in timed_else))
            self.assertFalse(any(then_id in line for line in timed_else))
            self.assertEqual(original_then, [line for line in timed_then if not line.startswith(("BEGIN:", "END:"))])
            self.assertEqual(original_else, [line for line in timed_else if not line.startswith(("BEGIN:", "END:"))])

            segments_path = root / "segments.json"
            segments_path.write_text(json.dumps(sched_segments), encoding="utf-8")
            (root / "prio_runtime.h").write_text(
                '#include <stdio.h>\nstatic inline void l1_set_thread_prio_fifo(int value) { printf("PRIO:%d\\n", value); }\n',
                encoding="utf-8",
            )
            priority_source = root / "condition_priority.c"
            priorities = {then_id: 11, else_id: 22}
            instrument_prio_all_segments_by_start_line(
                source,
                segments_json=segments_path,
                priorities=priorities,
                out_c=priority_source,
            )
            prioritized = root / "priority.exe"
            compile_source(priority_source, prioritized)
            priority_then = run_binary(prioritized, "1")
            priority_else = run_binary(prioritized, "0")
            self.assertIn("PRIO:11", priority_then)
            self.assertNotIn("PRIO:22", priority_then)
            self.assertIn("PRIO:22", priority_else)
            self.assertNotIn("PRIO:11", priority_else)
            self.assertEqual(original_then, [line for line in priority_then if not line.startswith("PRIO:")])
            self.assertEqual(original_else, [line for line in priority_else if not line.startswith("PRIO:")])

    def test_native_generation_preserves_same_line_semaphore_dependencies(self) -> None:
        gcc = shutil.which("gcc-13")
        if gcc is None:
            self.skipTest("Ubuntu GCC 13 is required for the native regression")
        from Code2DAG.pipeline.runner import run_collector, run_blocks
        from Code2DAG.generation.exporters import CircleTxtExporter

        repo = Path(__file__).resolve().parents[2]
        source = repo / "Code2DAG/source_files/dag5_condition_regression/dag5_condition_regression.c"
        name = source.stem
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            commands = [
                [gcc, "-O0", "-g", "-fdump-rtl-expand", "-c", str(source), "-o", str(base / (name + ".o"))],
            ]
            proc = subprocess.run(commands[0], cwd=repo, capture_output=True, text=True)
            self.assertEqual(0, proc.returncode, proc.stderr)
            expand = next(base.glob("*.expand"))
            proc = subprocess.run(
                ["python3", "-m", "Code2DAG.generation.legacy", str(expand),
                 "--source-file", str(source), "--output-base", str(base), "--force"],
                cwd=repo, capture_output=True, text=True,
            )
            self.assertEqual(0, proc.returncode, proc.stderr)
            result = base / "intermediate_results" / name
            shutil.copy2(result / "config_files" / (name + ".dot"), result / "dag_generation/dag.dot")
            run_collector(base_dir=base, base_name=name, source_file=source)
            run_blocks(base_dir=base, base_name=name, level="level2", rule_name="conditional_branches", source_file=source)
            blocks = result / "pipeline/blocks/level2/conditional_branches"
            dag = json.loads((blocks / "dag_sched.json").read_text())
            segments = json.loads((blocks / "segments_sched.json").read_text())
            for kind, count in [("sem_dep", 8), ("create", 17), ("join", 17)]:
                self.assertEqual(count, sum(edge["kind"] == kind for edge in dag["edges"]), kind)
            members = [item for item in segments["segments"] if item.get("condition_memberships")]
            self.assertEqual(2, len(members))
            self.assertEqual({"then", "else"}, {item["condition_memberships"][0]["branch_id"] for item in members})
            exporter = CircleTxtExporter({}, expand, source)
            self.assertIsNone(exporter._extract_source_arg_name("sem_post", 194))
            self.assertEqual("sem_left", exporter._extract_source_arg_name("sem_post", 194, 95))

    def test_rule_is_opt_in_and_default_is_unchanged(self) -> None:
        self.assertIn("conditional_branches", list_rules("level2"))
        self.assertEqual("effective_line_merge", default_rule_for_level("level2"))

    def test_writer_keeps_structural_and_scheduler_views(self) -> None:
        segments_struct = {"segments": [{"seg_id": "CE", "function": "main", "kind": "condition_entry", "start_line": 2, "end_line": 2}]}
        dag_struct = {"nodes": ["CE"], "edges": []}
        segments_sched = {"segments": [{"seg_id": "BR", "function": "main", "kind": "compute", "start_line": 3, "end_line": 3}]}
        dag_sched = {"nodes": ["BR"], "edges": []}
        output = RuleOutput(segments_struct, dag_struct, segments_sched, dag_sched, {"separate_views": True})
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            _write_rule_outputs(
                base_dir=base,
                base_name="sample",
                level="level2",
                rule_name="conditional_branches",
                output=output,
                dot_renderer=lambda _segments, _dag: "digraph G {}\n",
            )
            root = base / "intermediate_results" / "sample" / "pipeline" / "blocks" / "level2" / "conditional_branches"
            self.assertTrue((root / "segments_struct.json").exists())
            self.assertTrue((root / "dag_struct.json").exists())
            self.assertTrue((root / "segments_sched.json").exists())
            self.assertTrue((root / "dag_sched.json").exists())
            self.assertIn('"BR"', (root / "segments.json").read_text(encoding="utf-8"))

    def test_all_pipeline_stages_refuse_control_boundary_line_instrumentation(self) -> None:
        from Code2DAG.pipeline.schedule import run_schedule
        from Code2DAG.pipeline.instrument import run_instrument
        source_text = """int main(void) {
    if (flag) { fast();
    } else {
        slow();
    }
}
"""
        st, sd, ss, dd, meta = build_condition_views(base_name="unsafe_line", source_text=source_text,
            base_segments_json={"segments": [{"seg_id": "BASE", "function": "main", "kind": "compute", "start_line": 2, "end_line": 5}]},
            base_dag_json={"nodes": ["BASE"], "edges": []})
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            source = base / "unsafe.c"
            source.write_text(source_text)
            pipeline = base / "intermediate_results/unsafe_line/pipeline"
            blocks = pipeline / "blocks/level2/conditional_branches"
            blocks.mkdir(parents=True)
            segments_path = blocks / "segments.json"
            segments_path.write_text(json.dumps(ss))
            (blocks / "dag_seg.json").write_text(json.dumps(dd))
            (pipeline / "block_info.json").write_text(json.dumps({"source_file": str(source)}))
            timing = pipeline / "timing/level2/conditional_branches/timing.json"
            timing.parent.mkdir(parents=True)
            timing.write_text('{"weights":{}}')
            schedule = pipeline / "schedule/level2/conditional_branches/FIFO/schedule.json"
            schedule.parent.mkdir(parents=True)
            schedule.write_text('{"priorities":{}}')
            args = dict(base_dir=base, base_name="unsafe_line", level="level2", rule_name="conditional_branches")
            with self.assertRaisesRegex(StageError, "control boundary"):
                run_schedule(**args, algo_name="FIFO")
            with self.assertRaisesRegex(StageError, "control boundary"):
                run_instrument(**args, algo_name="FIFO", instrument_mode="generic")
            with self.assertRaisesRegex(StageError, "control boundary"):
                run_timing(**args, repeats=1)
            output = base / "must_not_be_written.c"
            with self.assertRaisesRegex(ValueError, "control boundary"):
                instrument_prio_all_segments_by_start_line(source, segments_json=segments_path,
                    priorities={s["seg_id"]: 10 for s in ss["segments"]}, out_c=output)
            self.assertFalse(output.exists())

    def test_timing_refuses_an_unsafe_condition_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            source = base / "sample.c"
            source.write_text("int main(void) { return 0; }\n", encoding="utf-8")
            pipeline = base / "intermediate_results" / "sample" / "pipeline"
            blocks = pipeline / "blocks" / "level2" / "conditional_branches"
            blocks.mkdir(parents=True)
            (pipeline / "block_info.json").write_text(
                '{"source_file": "' + str(source).replace("\\", "\\\\") + '"}',
                encoding="utf-8",
            )
            (blocks / "segments.json").write_text(
                '{"capabilities":{"scheduler_compatible":false,"blocked_reasons":["unsafe branch"]},"segments":[]}',
                encoding="utf-8",
            )
            (blocks / "dag_seg.json").write_text('{"nodes":[],"edges":[]}', encoding="utf-8")
            with self.assertRaisesRegex(StageError, "not timing-compatible"):
                run_timing(
                    base_dir=base,
                    base_name="sample",
                    level="level2",
                    rule_name="conditional_branches",
                    repeats=1,
                )


if __name__ == "__main__":
    unittest.main()
