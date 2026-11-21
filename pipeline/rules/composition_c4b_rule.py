"""C4b opt-in plugin; use the composition facade for publisher-level delegation."""
from ...level2.segment_dag_level2 import build_level2_segments_and_dag, _to_dot
from ..composition import select_rule
from ..composition_c4b_model import RULE, build_loop_in_lock_views as build_composition_views
from .level2_effective_line_rule import _build_merge_post_wait
from .base import RuleOutput


class CompositionC4bRule:
    def rule_id(self): return RULE
    def level(self): return 'level2'

    def build(self, *, base_dir, base_name, source_file, block_info):
        text=source_file.read_bytes().decode('utf-8')
        rule=select_rule(text)
        if rule != RULE and rule not in ('composition_c5e','composition_c5f'):
            from ..rules_registry import get_rule
            return get_rule('level2',rule).build(base_dir=base_dir,base_name=base_name,source_file=source_file,block_info=block_info)
        _build_merge_post_wait(base_dir,base_name)
        s,d=build_level2_segments_and_dag(base_dir=base_dir,base_name=base_name,source_file=source_file)
        return RuleOutput(*build_composition_views(base_name=base_name,source_text=text,base_segments_json=s,base_dag_json=d))

    @staticmethod
    def to_dot(segments_json,dag_json): return _to_dot(segments_json,dag_json)
