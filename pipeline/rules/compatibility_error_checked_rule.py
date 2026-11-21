"""Compatibility rule for error-checked pthread lock/unlock wrappers."""
from __future__ import annotations

from pathlib import Path

from ...level2.segment_dag_level2 import build_level2_segments_and_dag, _to_dot
from ..compatibility_error_checked_model import RULE, build_error_checked_views
from .base import RuleOutput
from .level2_effective_line_rule import _build_merge_post_wait


class CompatibilityErrorCheckedSyncRule:
    def rule_id(self) -> str:
        return RULE

    def level(self) -> str:
        return "level2"

    def build(self, *, base_dir: Path, base_name: str, source_file: Path, block_info: dict) -> RuleOutput:
        _ = block_info
        _build_merge_post_wait(base_dir, base_name)
        segments, dag = build_level2_segments_and_dag(
            base_dir=base_dir, base_name=base_name, source_file=source_file
        )
        views = build_error_checked_views(
            base_name=base_name,
            source_text=source_file.read_bytes().decode("utf-8"),
            base_segments_json=segments,
            base_dag_json=dag,
        )
        return RuleOutput(*views)

    @staticmethod
    def to_dot(segments_json, dag_json):
        return _to_dot(segments_json, dag_json)
