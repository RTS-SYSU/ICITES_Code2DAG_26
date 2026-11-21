from __future__ import annotations

from pathlib import Path
from typing import Dict

from ...level2 import segment_dag_level2
from ..condition_model import build_condition_views, render_condition_dot
from .base import RuleOutput
from .level2_effective_line_rule import _build_merge_post_wait


class ConditionalBranchesRule:
    """Build explicit mutually exclusive branches for structure and scheduling."""

    def rule_id(self) -> str:
        return "conditional_branches"

    def level(self) -> str:
        return "level2"

    def build(self, *, base_dir: Path, base_name: str, source_file: Path, block_info: Dict) -> RuleOutput:
        _ = block_info
        _build_merge_post_wait(base_dir, base_name)
        base_segments, base_dag = segment_dag_level2.build_level2_segments_and_dag(
            base_dir=base_dir,
            base_name=base_name,
            source_file=source_file,
        )
        struct_segments, struct_dag, sched_segments, sched_dag, meta = build_condition_views(
            base_name=base_name,
            source_text=source_file.read_text(encoding="utf-8", errors="replace"),
            base_segments_json=base_segments,
            base_dag_json=base_dag,
        )
        meta.update(
            {
                "rule_name": self.rule_id(),
                "level": self.level(),
                "notes": (
                    "Phase-1 conditional model. Structural output has paired entry/exit nodes and "
                    "mutually exclusive branch edges. Existing schedulers receive separate executable branch nodes."
                ),
            }
        )
        return RuleOutput(struct_segments, struct_dag, sched_segments, sched_dag, meta)

    @staticmethod
    def to_dot(segments_json: Dict, dag_json: Dict) -> str:
        return render_condition_dot(segments_json, dag_json)
