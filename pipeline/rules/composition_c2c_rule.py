"""C2c: Condition + independent Lock, no static Loop."""
from ...level2.segment_dag_level2 import build_level2_segments_and_dag, _to_dot
from ..composition_c2c_model import RULE, build_condition_lock_views
from .level2_effective_line_rule import _build_merge_post_wait
from .base import RuleOutput


class CompositionC2cRule:
    def rule_id(self): return RULE
    def level(self): return 'level2'

    def build(self, *, base_dir, base_name, source_file, block_info):
        text=source_file.read_bytes().decode('utf-8')
        _build_merge_post_wait(base_dir,base_name)
        s,d=build_level2_segments_and_dag(base_dir=base_dir,base_name=base_name,source_file=source_file)
        return RuleOutput(*build_condition_lock_views(base_name=base_name,source_text=text,
                                                       base_segments_json=s,base_dag_json=d))

    @staticmethod
    def to_dot(segments_json,dag_json): return _to_dot(segments_json,dag_json)
