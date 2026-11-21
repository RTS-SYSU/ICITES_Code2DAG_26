from __future__ import annotations

import copy
import subprocess
from pathlib import Path
from typing import Dict, Optional

from .collector import collect_block_info
from .errors import StageError
from .instrument import run_instrument
from .io_utils import mark_failed, mark_running, mark_success, read_json, write_json
from .naming import validate_name
from .rules.base import RuleOutput
from .rules_registry import get_rule, list_rules
from .schedule import run_schedule
from .timing import run_timing


def _render_dot(dot_text: str, dot_path: Path, png_path: Path) -> None:
    dot_path.parent.mkdir(parents=True, exist_ok=True)
    dot_path.write_text(dot_text, encoding="utf-8")
    try:
        subprocess.run(["dot", "-Tpng", str(dot_path), "-o", str(png_path)], check=True, capture_output=True)
    except Exception:
        # Keep dot even if graphviz is missing.
        pass


def run_collector(*, base_dir: Path, base_name: str, source_file: Path) -> Dict:
    return collect_block_info(base_dir=base_dir, base_name=base_name, source_file=source_file)


def _absorb_internal_sem_witnesses(output: RuleOutput) -> None:
    """Remove sem_dep self-loops from published views and record the witness.

    A same-segment semaphore relation is an internal dependency of that merged
    segment.  It is not a scheduling edge and must not be published as a
    self-loop.  Keeping the record in mappings preserves provenance.
    """
    seen_dags = set()
    for view, dag, segments in (
        ("struct", output.struct_dag, output.struct_segments),
        ("sched", output.sched_dag, output.sched_segments),
    ):
        if not isinstance(dag, dict) or id(dag) in seen_dags:
            continue
        seen_dags.add(id(dag))
        edges = dag.get("edges")
        if not isinstance(edges, list):
            continue
        kept = []
        absorbed = []
        for edge in edges:
            if (
                isinstance(edge, dict)
                and edge.get("kind") == "sem_dep"
                and edge.get("src") == edge.get("dst")
            ):
                absorbed.append({"edge": copy.deepcopy(edge), "view": view})
            else:
                kept.append(edge)
        if not absorbed:
            continue
        dag["edges"] = kept
        if not isinstance(segments, dict):
            continue
        mappings = segments.get("mappings")
        if not isinstance(mappings, dict):
            mappings = {}
            segments["mappings"] = mappings
        existing = mappings.get("absorbed_internal_sem_dependencies")
        if not isinstance(existing, list):
            existing = []
            mappings["absorbed_internal_sem_dependencies"] = existing
        existing.extend(absorbed)


def _validate_final_graphs_acyclic(output: RuleOutput) -> None:
    """Reject a rule result that still has a cycle after witness absorption.

    Base Level-2 graphs may legitimately contain a sem_dep self-loop while a
    determinate same-segment relation is still an internal witness.  This
    check runs only on the final struct/sched views, after that witness has
    been absorbed, so it does not turn every intermediate base cycle into an
    error.
    """
    from .algo.common import topo_sort

    seen = set()
    for view, dag in (("struct", output.struct_dag), ("sched", output.sched_dag)):
        if not isinstance(dag, dict) or id(dag) in seen:
            continue
        seen.add(id(dag))
        nodes = dag.get("nodes")
        edges = dag.get("edges")
        if not isinstance(nodes, list) or not isinstance(edges, list):
            continue
        pairs = [
            (str(edge["src"]), str(edge["dst"]))
            for edge in edges
            if isinstance(edge, dict)
            and isinstance(edge.get("src"), str)
            and isinstance(edge.get("dst"), str)
        ]
        try:
            topo_sort([str(node) for node in nodes], pairs)
        except Exception as exc:
            raise StageError(f"final {view} graph is not acyclic: {exc}") from exc


def _write_rule_outputs(*, base_dir: Path, base_name: str, level: str, rule_name: str, output: RuleOutput, dot_renderer) -> Dict:
    out_root = base_dir / "intermediate_results" / base_name / "pipeline" / "blocks" / level / rule_name
    rule_meta_path = out_root / "rule_meta.json"
    mark_running(rule_meta_path, step="blocks", extra={"level": level, "rule_name": rule_name})
    try:
        out_root.mkdir(parents=True, exist_ok=True)

        # Keep the historical filenames as scheduling-view aliases.  Conditional
        # rules additionally expose a structural view that contains synthetic
        # control nodes and must not be consumed by ordinary DAG schedulers.
        write_json(out_root / "segments_struct.json", output.struct_segments)
        write_json(out_root / "dag_struct.json", output.struct_dag)
        write_json(out_root / "segments_sched.json", output.sched_segments)
        write_json(out_root / "dag_sched.json", output.sched_dag)

        segments = dict(output.sched_segments or output.struct_segments)
        dag = dict(output.sched_dag or output.struct_dag)
        segments["view"] = "single"
        write_json(out_root / "segments.json", segments)
        write_json(out_root / "dag_seg.json", dag)

        dot_text = dot_renderer(segments, dag)
        _render_dot(dot_text, out_root / "dag_seg.dot", out_root / "dag_seg.png")
        if output.struct_segments != output.sched_segments or output.struct_dag != output.sched_dag:
            struct_dot = dot_renderer(output.struct_segments, output.struct_dag)
            _render_dot(struct_dot, out_root / "dag_struct.dot", out_root / "dag_struct.png")

        payload = dict(output.meta)
        payload.update({"status": "success", "level": level, "rule_name": rule_name})
        write_json(rule_meta_path, payload)
        mark_success(rule_meta_path, step="blocks", extra={"level": level, "rule_name": rule_name})
        return payload
    except Exception as exc:
        mark_failed(rule_meta_path, step="blocks", error=str(exc), extra={"level": level, "rule_name": rule_name})
        raise


def run_blocks(*, base_dir: Path, base_name: str, level: str, rule_name: str, source_file: Optional[Path] = None) -> Dict:
    level = validate_name(level, "level")
    if level not in {"level1", "level2", "level3"}:
        raise ValidationError("level must be one of: level1, level2, level3")
    rule_name = validate_name(rule_name, "rule_name")

    pipeline_root = base_dir / "intermediate_results" / base_name / "pipeline"
    block_info_path = pipeline_root / "block_info.json"
    if not block_info_path.exists():
        raise StageError("missing block_info.json; run collector first")
    block_info = read_json(block_info_path)
    if source_file is None:
        source_file = Path(str(block_info["source_file"])).resolve()

    rule = get_rule(level, rule_name)
    output = rule.build(base_dir=base_dir, base_name=base_name, source_file=source_file, block_info=block_info)
    _absorb_internal_sem_witnesses(output)
    _validate_final_graphs_acyclic(output)
    dot_renderer = getattr(rule, "to_dot", None)
    if dot_renderer is None:
        raise StageError(f"rule '{rule_name}' missing to_dot renderer")
    return _write_rule_outputs(
        base_dir=base_dir,
        base_name=base_name,
        level=level,
        rule_name=rule_name,
        output=output,
        dot_renderer=dot_renderer,
    )


def run_timing_stage(*, base_dir: Path, base_name: str, level: str, rule_name: str, repeats: Optional[int] = None) -> Dict:
    validate_name(level, "level")
    validate_name(rule_name, "rule_name")
    return run_timing(base_dir=base_dir, base_name=base_name, level=level, rule_name=rule_name, repeats=repeats)


def run_schedule_stage(*, base_dir: Path, base_name: str, level: str, rule_name: str, algo_name: str) -> Dict:
    validate_name(level, "level")
    validate_name(rule_name, "rule_name")
    validate_name(algo_name, "algo_name")
    return run_schedule(base_dir=base_dir, base_name=base_name, level=level, rule_name=rule_name, algo_name=algo_name)


def run_instrument_stage(
    *,
    base_dir: Path,
    base_name: str,
    level: str,
    rule_name: str,
    algo_name: str,
    instrument_mode: str = "auto",
    segment_trace: bool = False,
) -> Dict:
    validate_name(level, "level")
    validate_name(rule_name, "rule_name")
    validate_name(algo_name, "algo_name")
    validate_name(instrument_mode, "instrument_mode")
    return run_instrument(
        base_dir=base_dir,
        base_name=base_name,
        level=level,
        rule_name=rule_name,
        algo_name=algo_name,
        instrument_mode=instrument_mode,
        segment_trace=segment_trace,
    )


def default_rule_for_level(level: str) -> Optional[str]:
    preferred = {
        "level1": "stage1_create_join",
        "level2": "effective_line_merge",
        "level3": "placeholder_level3",
    }.get(level)
    rules = list_rules(level)
    if not rules:
        return None
    if preferred in rules:
        return preferred
    return sorted(rules.keys())[0]
