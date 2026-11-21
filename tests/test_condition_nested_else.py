"""Symmetric closure: one inner if/else inside outer else, no else-if."""
from pathlib import Path
import shutil
import tempfile
import unittest
from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.tests.test_condition_nested import (ROOT, ALGOS, call_lines,
    validate_structure, projected_structure, native_acceptance)

SOURCE_ELSE = ROOT / 'Code2DAG/source_files/condition_nested_else/condition_nested_else.c'

class NestedElseTests(unittest.TestCase):
    def test_else_parent_memberships_and_projected_merges(self):
        source = SOURCE_ELSE.read_text()
        lines = source.splitlines()
        start = next(i for i,l in enumerate(lines,1) if '(void)unused;' in l)
        end = next(i for i,l in enumerate(lines,1) if 'return NULL;' in l)
        main = next(i for i,l in enumerate(lines,1) if 'puts("main:start")' in l)
        payload = {'segments': [
            {'seg_id':'W','function':'worker','kind':'compute','start_line':start,'end_line':end},
            {'seg_id':'M','function':'main','kind':'compute','start_line':main,'end_line':len(lines)-1}],
            'node_locations': {label:{'function':'worker','line':line} for label,line in call_lines(source,'else').items()}}
        st,sd,ss,dd,_ = build_condition_views(base_name='nested_else',source_text=source,
            base_segments_json=payload,base_dag_json={'nodes':['W','M'],'edges':[]})
        result = validate_structure(source,st,sd,ss,dd,'else')
        for flags in [(False,True),(False,False),(True,False),(True,True)]:
            projected_structure(st,sd,result,flags,'else')
        for label,sid in result['labels'].items():
            self.assertEqual(sid,ss['mappings']['original_node_to_execution_segment'][label])
        for algo in ALGOS:
            schedule=get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json={'weights':{n:{'avg_ns':1} for n in dd['nodes']}})
            self.assertEqual(set(dd['nodes']),set(schedule['priorities']))

    @unittest.skipUnless(shutil.which('gcc-13'),'Ubuntu GCC13 required')
    def test_native_false_paths_and_true_inner_invariance(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary=native_acceptance(Path(tmp)/'acceptance',source_file=SOURCE_ELSE,child_branch='else')
            self.assertTrue(summary['passed'])
            self.assertEqual(summary['paths']['T0']['stdout'],summary['paths']['T1']['stdout'])

if __name__=='__main__':unittest.main()
