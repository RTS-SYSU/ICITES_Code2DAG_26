"""A single Condition target can coexist with multiple ordinary lock regions."""
import unittest

from Code2DAG.tests import test_composition_c2b as c2b
from Code2DAG.pipeline.composition_c2b_model import build_lock_path_views, MAX_LOCK_REGIONS
from Code2DAG.pipeline.composition_model import CompositionError


class ExtraRegionsTests(unittest.TestCase):
    def build(self,count):
        text = (c2b.SOURCES/'common.c').read_text()
        extra = '    pthread_mutex_lock(&m);\n    result += 1;\n    pthread_mutex_unlock(&m);\n'
        text = text.replace('    return NULL;',extra*(count-1)+'    return NULL;',1)
        raw,dag = c2b.CompositionC2bTests().fixture('common',text)[1:]
        return build_lock_path_views(base_name='fixture',source_text=text,
                                    base_segments_json=raw,base_dag_json=dag)

    def test_four_regions_keep_distinct_source_members_and_one_condition(self):
        st,sd,ss,dd,meta = self.build(4)
        group = ss['lock_groups'][0]
        self.assertEqual(4,len(group['members']))
        self.assertEqual(4,len({m['member_segment_id'] for m in group['members']}))
        self.assertEqual(1,len(ss['condition_regions']))
        self.assertEqual(2,len(group['candidate_segment_ids']))
        # Ordinary sections stay in the graph, in source order, without being
        # absorbed into either of the target section's mutually exclusive paths.
        ordinary = [r for r in ss['segments'] if r['kind']=='mutex_cs']
        self.assertEqual(3,len(ordinary))
        targets = ss['mappings']['original_segment_to_execution_segments']
        for member in group['members'][1:]:
            self.assertEqual(1,len(targets[member['member_segment_id']]))

    def test_eight_regions_are_bounded_and_ninth_is_rejected(self):
        self.assertEqual(MAX_LOCK_REGIONS,len(self.build(MAX_LOCK_REGIONS)[2]['lock_groups'][0]['members']))
        with self.assertRaisesRegex(CompositionError,'at most eight'):
            self.build(MAX_LOCK_REGIONS+1)


if __name__ == '__main__':
    unittest.main()
