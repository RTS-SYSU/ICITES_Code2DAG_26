"""Optional sequential pair rule; delegate zero/single-loop inputs byte-for-byte."""
import hashlib
from ...level2.segment_dag_level2 import build_level2_segments_and_dag,_to_dot
from ..loop_pair_model import analyze_loop_pair,build_loop_pair_views
from .base import RuleOutput
from .loop_rule import LoopStaticRule
from .level2_effective_line_rule import _build_merge_post_wait


class LoopStaticPairRule:
    def rule_id(self):return 'loop_static_pair'
    def level(self):return 'level2'

    def build(self,*,base_dir,base_name,source_file,block_info):
        text=source_file.read_bytes().decode('utf-8')
        if analyze_loop_pair(text) is None:
            return LoopStaticRule().build(base_dir=base_dir,base_name=base_name,source_file=source_file,block_info=block_info)
        _build_merge_post_wait(base_dir,base_name)
        s,d=build_level2_segments_and_dag(base_dir=base_dir,base_name=base_name,source_file=source_file)
        views=build_loop_pair_views(base_name=base_name,source_text=text,base_segments_json=s,base_dag_json=d)
        for payload in (views[0],views[2]):payload['source_sha256']=hashlib.sha256(source_file.read_bytes()).hexdigest()
        return RuleOutput(*views)

    @staticmethod
    def to_dot(segments_json,dag_json):return _to_dot(segments_json,dag_json)
