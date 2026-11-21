"""Both @instanceN clones of one worker must get the same exact lock segment."""
import json
import tempfile
import unittest
from pathlib import Path

from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
from Code2DAG.pipeline.composition_c2a_model import build_branch_lock_views
from Code2DAG.pipeline.algo.common import topo_sort


SOURCE = """#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static int shared = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

struct worker_arg {
    int id;
    int mode;
};

void *worker(void *arg) {
    struct worker_arg *a = (struct worker_arg *)arg;
    if (a->mode) {
        pthread_mutex_lock(&lock);
        shared += 1;
        pthread_mutex_unlock(&lock);
        printf("worker%d tid=%lu branch=then shared=%d\\n",
               a->id, (unsigned long)pthread_self(), shared);
    } else {
        printf("worker%d tid=%lu branch=else shared=%d\\n",
               a->id, (unsigned long)pthread_self(), shared);
    }
    return NULL;
}

int main(int argc, char **argv) {
    int mode = atoi(argv[1]);
    struct worker_arg a1 = {1, mode};
    struct worker_arg a2 = {2, mode};
    pthread_t t1, t2;
    pthread_create(&t1, NULL, worker, &a1);
    pthread_create(&t2, NULL, worker, &a2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    printf("shared=%d mode=%d\\n", shared, mode);
    return 0;
}
"""

WORKER_META = {
    'worker/if/pthread_mutex_lock1': {'file': '<memory>', 'line': 16, 'col': 9, 'extern': 0},
    'worker/if/pthread_mutex_unlock2': {'file': '<memory>', 'line': 18, 'col': 9, 'extern': 0},
    'worker/if/pthread_self3': {'file': '<memory>', 'line': 20, 'col': 38, 'extern': 0},
    'worker/if/printf4': {'file': '<memory>', 'line': 19, 'col': 9, 'extern': 0},
    'worker/pthread_self5': {'file': '<memory>', 'line': 23, 'col': 38, 'extern': 0},
    'worker/printf6': {'file': '<memory>', 'line': 22, 'col': 9, 'extern': 0},
}
MAIN_META = {
    'main/atoi1': {'file': '<memory>', 'line': 29, 'col': 16, 'extern': 0},
    'main/pthread_create2': {'file': '<memory>', 'line': 33, 'col': 5, 'extern': 0},
    'main/pthread_create3': {'file': '<memory>', 'line': 34, 'col': 5, 'extern': 0},
    'main/pthread_join4': {'file': '<memory>', 'line': 35, 'col': 5, 'extern': 0},
    'main/pthread_join5': {'file': '<memory>', 'line': 36, 'col': 5, 'extern': 0},
    'main/printf6': {'file': '<memory>', 'line': 37, 'col': 5, 'extern': 0},
    'main/if/__stack_chk_fail7': {'file': '<memory>', 'line': 39, 'col': 1, 'extern': 0},
}
FUNCTIONS_FULL = {
    'worker': {
        'mycalls': list(WORKER_META),
        'myinfo': {'tail': 'worker/printf6'},
        'mycalls_meta': WORKER_META,
    },
    'main': {
        'mycalls': ['main/atoi1', 'main/pthread_create2', 'worker',
                    'main/pthread_create3', 'worker@instance1',
                    'main/pthread_join4', 'main/pthread_join5',
                    'main/printf6', 'main/if/__stack_chk_fail7'],
        'myinfo': {
            'tail': 'main/if/__stack_chk_fail7',
            'pthread_create': 'worker',
            '__create_queue__': ['worker'],
            'main/pthread_join4': 't1',
            'main/pthread_join5': 't2',
            't1': 'worker',
            't2': 'worker',
        },
        'mycalls_meta': MAIN_META,
    },
    'worker@instance1': {
        'mycalls': list(WORKER_META),
        'myinfo': {'tail': 'worker/printf6'},
        'mycalls_meta': WORKER_META,
    },
}
RANGES = {
    'functions': [
        {'name': 'worker', 'base_name': 'worker', 'start_line': 13,
         'body_start_line': 13, 'first_stmt_line': 14, 'end_line': 26,
         'last_stmt_line': 25, 'last_return_line': 25,
         'level1_start_line': 14, 'level1_end_line': 25},
        {'name': 'main', 'base_name': 'main', 'start_line': 28,
         'body_start_line': 28, 'first_stmt_line': 29, 'end_line': 39,
         'last_stmt_line': 38, 'last_return_line': 38,
         'level1_start_line': 29, 'level1_end_line': 38},
        {'name': 'worker@instance1', 'base_name': 'worker', 'start_line': 13,
         'body_start_line': 13, 'first_stmt_line': 14, 'end_line': 26,
         'last_stmt_line': 25, 'last_return_line': 25,
         'level1_start_line': 14, 'level1_end_line': 25},
    ]
}
INTERNAL = {'worker': WORKER_META, 'main': MAIN_META, 'worker@instance1': WORKER_META}


class BranchSegmentTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        native = self.root / 'native'
        gen = native / 'intermediate_results' / 'branch_case' / 'dag_generation'
        (gen / 'debug').mkdir(parents=True)
        (native / 'intermediate_results' / 'branch_case' / 'config_files').mkdir(parents=True)
        self.source = self.root / 'branch_case.c'
        self.source.write_text(SOURCE)
        (gen / 'functions_full.json').write_text(json.dumps(FUNCTIONS_FULL))
        (gen / 'functions_ranges.json').write_text(json.dumps(RANGES))
        (gen / 'debug' / 'mycalls_meta_internal.json').write_text(json.dumps(INTERNAL))
        (gen / 'dag.dot').write_text('strict digraph callgraph {}')
        (native / 'intermediate_results' / 'branch_case' / 'config_files' / 'circle.txt').write_text('')
        self.base_dir = native

    def tearDown(self):
        self._tmp.cleanup()

    def base(self):
        return build_level2_segments_and_dag(
            base_dir=self.base_dir, base_name='branch_case', source_file=self.source)

    def final(self):
        segments, dag = self.base()
        raw = dict(segments)
        raw['mappings'] = {}
        return build_branch_lock_views(
            base_name='branch_case', source_text=SOURCE,
            base_segments_json=raw, base_dag_json=dag), segments, dag

    def test_both_instances_get_exact_16_18_lock_segment(self):
        segments, _dag = self.base()
        worker = [(r['seg_id'], r['function'], r['kind'], r['start_line'], r['end_line'])
                  for r in segments['segments'] if r['function'] == 'worker']
        instance = [(r['seg_id'], r['function'], r['kind'], r['start_line'], r['end_line'])
                    for r in segments['segments'] if r['function'] == 'worker@instance1']
        self.assertEqual([
            ('SEG:worker#001@14-15', 'worker', 'compute', 14, 15),
            ('MU:worker#002@16-18', 'worker', 'mutex_cs', 16, 18),
            ('SEG:worker#003@19-24', 'worker', 'compute', 19, 24),
        ], worker)
        self.assertEqual([
            ('SEG:worker@instance1#001@14-15', 'worker@instance1', 'compute', 14, 15),
            ('MU:worker@instance1#002@16-18', 'worker@instance1', 'mutex_cs', 16, 18),
            ('SEG:worker@instance1#003@19-24', 'worker@instance1', 'compute', 19, 24),
        ], instance)

    def test_c2a_final_graph_keeps_memberships_mutex_joins_and_mappings(self):
        (_struct, struct_dag, sched, sched_dag, meta), _segments, _dag = self.final()
        self.assertTrue(meta['scheduler_compatible'])
        groups = {g['group_id']: g for g in sched['lock_groups']}
        self.assertEqual({
            'LOCKGROUP:lock@COND:worker#001:then',
            'LOCKGROUP:lock@COND:worker@instance1#001:then',
        }, set(groups))
        self.assertTrue(all(g['mutex_id'] == 'lock' for g in groups.values()))
        self.assertEqual(['BR:worker#004@16-18'], groups[
            'LOCKGROUP:lock@COND:worker#001:then']['member_segment_ids'])
        self.assertEqual(['BR:worker@instance1#004@16-18'], groups[
            'LOCKGROUP:lock@COND:worker@instance1#001:then']['member_segment_ids'])

        joins = {(e['src'], e['dst']) for e in sched_dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({
            ('BR:worker#005@19-20', 'SEG:main#003@35-35'),
            ('BR:worker#006@22-23', 'SEG:main#003@35-35'),
            ('BR:worker@instance1#005@19-20', 'SEG:main#004@36-37'),
            ('BR:worker@instance1#006@22-23', 'SEG:main#004@36-37'),
        }, joins)

        positions = sched['mappings']['source_position_to_execution_segments']
        for fn, group, then_row, else_row in (
                ('worker', 'LOCKGROUP:lock@COND:worker#001:then',
                 'BR:worker#005@19-20', 'BR:worker#006@22-23'),
                ('worker@instance1', 'LOCKGROUP:lock@COND:worker@instance1#001:then',
                 'BR:worker@instance1#005@19-20', 'BR:worker@instance1#006@22-23')):
            for line in (16, 17, 18):
                self.assertEqual([group], positions[f'LOOPSRC:{fn}:line:{line}'])
            for line in (19, 20):
                self.assertEqual([then_row], positions[f'LOOPSRC:{fn}:line:{line}'])
            for line in (22, 23):
                self.assertEqual([else_row], positions[f'LOOPSRC:{fn}:line:{line}'])

        def owner(node):
            for prefix in ('worker@instance1', 'worker'):
                if node.startswith(('BR:' + prefix, 'CE:' + prefix, 'CX:' + prefix,
                                    'LOCKGROUP:lock@COND:' + prefix)):
                    return prefix
            return None

        cross = [e for e in sched_dag['edges']
                 if owner(e['src']) and owner(e['dst']) and owner(e['src']) != owner(e['dst'])]
        self.assertEqual([], cross)
        topo_sort(struct_dag['nodes'], [(e['src'], e['dst']) for e in struct_dag['edges']])
        topo_sort(sched_dag['nodes'], [(e['src'], e['dst']) for e in sched_dag['edges']])


if __name__ == '__main__':
    unittest.main()
