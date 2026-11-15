from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict

from .algo_registry import get_algo
from .errors import StageError
from .io_utils import mark_failed, mark_running, mark_success, read_json, write_json


def run_schedule(*, base_dir: Path, base_name: str, level: str, rule_name: str, algo_name: str) -> Dict:
    pipeline_root = base_dir / "intermediate_results" / base_name / "pipeline"
    out_root = pipeline_root / "schedule" / level / rule_name / algo_name
    meta_path = out_root / "schedule_meta.json"
    mark_running(meta_path, step="schedule", extra={"algo_name": algo_name})

    try:
        dag_json_path = pipeline_root / "blocks" / level / rule_name / "dag_seg.json"
        segments_json_path = pipeline_root / "blocks" / level / rule_name / "segments.json"
        timing_json_path = pipeline_root / "timing" / level / rule_name / "timing.json"
        if not dag_json_path.exists():
            raise StageError(f"missing dag: {dag_json_path}")
        if not segments_json_path.exists():
            raise StageError(f"missing segments: {segments_json_path}")
        if not timing_json_path.exists():
            raise StageError(f"missing timing: {timing_json_path}")

        dag_json = read_json(dag_json_path)
        segments_json = read_json(segments_json_path)
        timing_json = read_json(timing_json_path)
        capabilities = segments_json.get("capabilities", {})
        if isinstance(capabilities, dict) and capabilities.get("scheduler_compatible") is False:
            reasons = capabilities.get("blocked_reasons", [])
            raise StageError(f"condition model is not scheduler-compatible in phase 1: {reasons}")

        if algo_name in {"condition_pareto", "condition_union"}:
            expected = hashlib.sha256(segments_json_path.read_bytes()).hexdigest()
            if timing_json.get("segments_sha256") != expected:
                raise StageError("conditional profiles do not match current segments; rerun condition-timing")

        algo = get_algo(algo_name)
        schedule_json = algo.compute(dag_json=dag_json, segments_json=segments_json, timing_json=timing_json)
        algo.validate(schedule_json)

        out_root.mkdir(parents=True, exist_ok=True)
        write_json(out_root / "schedule.json", schedule_json)
        # Remove stale annotated graph outputs from older versions.
        for stale_name in ("dag_seg_annotated.dot", "dag_seg_annotated.png"):
            stale_path = out_root / stale_name
            if stale_path.exists():
                stale_path.unlink()
        mark_success(
            meta_path,
            step="schedule",
            extra={"algo_name": algo_name, "priority_count": len(schedule_json.get("priorities", {})), "priority_range": "1..99"},
        )
        return schedule_json
    except Exception as exc:
        mark_failed(meta_path, step="schedule", error=str(exc), extra={"algo_name": algo_name})
        raise
