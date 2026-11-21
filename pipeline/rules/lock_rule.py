from pathlib import Path

from ...level2.segment_dag_level2 import build_level2_segments_and_dag, _to_dot
from ..lock_model import build_lock_views
from .base import RuleOutput
from .level2_effective_line_rule import _build_merge_post_wait


class LockGroupedRule:
    def rule_id(self):
        return 'lock_grouped'

    def level(self):
        return 'level2'

    def build(self, *, base_dir: Path, base_name: str, source_file: Path, block_info: dict):
        _build_merge_post_wait(base_dir, base_name)
        segments, dag = build_level2_segments_and_dag(base_dir=base_dir,base_name=base_name,source_file=source_file)
        views = build_lock_views(base_name=base_name,source_text=source_file.read_bytes().decode('utf-8'),base_segments_json=segments,base_dag_json=dag)
        return RuleOutput(*views)

    @staticmethod
    def to_dot(segments_json, dag_json):
        return _to_dot(segments_json,dag_json)
