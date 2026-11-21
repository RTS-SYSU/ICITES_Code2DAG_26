"""Optional perfect-nest rule; all old forms delegate without serialization changes."""
import hashlib
from ...level2.segment_dag_level2 import build_level2_segments_and_dag, _to_dot
from ..loop_nested_model import analyze_nested_loops, build_loop_nested_views
from .loop_pair_rule import LoopStaticPairRule
from .level2_effective_line_rule import _build_merge_post_wait
from .base import RuleOutput


class LoopStaticNestedRule:
    def rule_id(self): return 'loop_static_nested'
    def level(self): return 'level2'

    def build(self, *, base_dir, base_name, source_file, block_info):
        text = source_file.read_bytes().decode('utf-8')
        if analyze_nested_loops(text) is None:
            return LoopStaticPairRule().build(base_dir=base_dir, base_name=base_name, source_file=source_file, block_info=block_info)
        _build_merge_post_wait(base_dir, base_name)
        s,d = build_level2_segments_and_dag(base_dir=base_dir, base_name=base_name, source_file=source_file)
        views = build_loop_nested_views(base_name=base_name, source_text=text, base_segments_json=s, base_dag_json=d)
        for payload in (views[0],views[2]): payload['source_sha256'] = hashlib.sha256(source_file.read_bytes()).hexdigest()
        return RuleOutput(*views)

    @staticmethod
    def to_dot(segments_json, dag_json): return _to_dot(segments_json, dag_json)
