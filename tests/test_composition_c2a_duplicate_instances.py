"""C2a must keep one source function and both generated execution instances."""
import unittest

from Code2DAG.pipeline.composition_c2a_model import build_branch_lock_views
from Code2DAG.pipeline.algo.common import topo_sort


SOURCE = """#include <pthread.h>
#include <stdio.h>

static int shared = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
    int mode = *(int *)arg;
    if (mode) {
        pthread_mutex_lock(&lock);
        shared += 1;
        pthread_mutex_unlock(&lock);
    } else {
        shared += 2;
    }
    return NULL;
}

int main(void) {
    pthread_t t1, t2;
    int one = 1;
    pthread_create(&t1, NULL, worker, &one);
    pthread_create(&t2, NULL, worker, &one);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    printf("shared=%d\\n", shared);
    return 0;
}
"""

SEGMENTS = [
    dict(seg_id='SEG:main#001@20-22', function='main', kind='compute', start_line=20, end_line=22),
    dict(seg_id='SEG:main#002@23-23', function='main', kind='compute', start_line=23, end_line=23),
    dict(seg_id='SEG:main#003@24-24', function='main', kind='compute', start_line=24, end_line=24),
    dict(seg_id='SEG:main#004@25-26', function='main', kind='compute', start_line=25, end_line=26),
    dict(seg_id='MU:worker#001@8-15', function='worker', kind='mutex_cs', start_line=8, end_line=15),
    dict(seg_id='SEG:worker@instance1#001@8-9', function='worker@instance1', kind='compute', start_line=8, end_line=9),
    dict(seg_id='MU:worker@instance1#002@10-12', function='worker@instance1', kind='mutex_cs', start_line=10, end_line=12),
    dict(seg_id='SEG:worker@instance1#003@13-15', function='worker@instance1', kind='compute', start_line=13, end_line=15),
]

EDGES = [
    dict(src='SEG:main#001@20-22', dst='SEG:main#002@23-23', kind='intra'),
    dict(src='SEG:main#002@23-23', dst='SEG:main#003@24-24', kind='intra'),
    dict(src='SEG:main#003@24-24', dst='SEG:main#004@25-26', kind='intra'),
    dict(src='SEG:worker@instance1#001@8-9', dst='MU:worker@instance1#002@10-12', kind='intra'),
    dict(src='MU:worker@instance1#002@10-12', dst='SEG:worker@instance1#003@13-15', kind='intra'),
    dict(src='SEG:main#001@20-22', dst='MU:worker#001@8-15', kind='create'),
    dict(src='SEG:main#002@23-23', dst='SEG:worker@instance1#001@8-9', kind='create'),
    dict(src='MU:worker#001@8-15', dst='SEG:main#003@24-24', kind='join'),
    dict(src='MU:worker#001@8-15', dst='SEG:main#004@25-26', kind='join'),
]


class DuplicateInstanceC2aTests(unittest.TestCase):
    def build(self):
        raw = dict(base_name='two_instances', source_file='<memory>',
                   segments=SEGMENTS,
                   thread_entry_functions=['main', 'worker', 'worker@instance1'],
                   node_locations={}, node_to_segment={}, mappings={})
        dag = dict(nodes=[row['seg_id'] for row in SEGMENTS], edges=EDGES)
        return build_branch_lock_views(base_name='two_instances', source_text=SOURCE,
                                       base_segments_json=raw, base_dag_json=dag)

    def test_both_instances_and_one_mutex_are_preserved(self):
        _struct, _struct_dag, sched, sched_dag, meta = self.build()
        self.assertTrue(meta['scheduler_compatible'])
        groups = {group['group_id']: group for group in sched['lock_groups']}
        self.assertEqual(
            {'LOCKGROUP:lock@COND:worker#001:then',
             'LOCKGROUP:lock@COND:worker@instance1#001:then'},
            set(groups))
        for group in groups.values():
            self.assertEqual('lock', group['mutex_id'])
            self.assertEqual(1, len(group['member_segment_ids']))
        conditions = {(region['function'], region['entry_line'])
                      for region in sched['condition_regions']}
        self.assertEqual({('worker', 9), ('worker@instance1', 9)}, conditions)
        positions = sched['mappings']['source_position_to_execution_segments']
        for fn in ('worker', 'worker@instance1'):
            for line in (10, 11, 12):
                key = f'LOOPSRC:{fn}:line:{line}'
                self.assertEqual(1, len(positions[key]), key)
                self.assertIn(positions[key][0], groups, key)
        topo_sort(sched_dag['nodes'], [(e['src'], e['dst']) for e in sched_dag['edges']])


if __name__ == '__main__':
    unittest.main()
