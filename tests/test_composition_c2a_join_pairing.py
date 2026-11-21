"""Base-stage join pairing and C2a end-to-end reachability for @instanceN."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
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

WORKER_META = {
    'worker/if/pthread_mutex_lock1': {'file': '<memory>', 'line': 10, 'col': 9, 'extern': 0},
    'worker/if/pthread_mutex_unlock2': {'file': '<memory>', 'line': 12, 'col': 9, 'extern': 0},
}
MAIN_META = {
    'main/pthread_create1': {'file': '<memory>', 'line': 22, 'col': 5, 'extern': 0},
    'main/pthread_create2': {'file': '<memory>', 'line': 23, 'col': 5, 'extern': 0},
    'main/pthread_join3': {'file': '<memory>', 'line': 24, 'col': 5, 'extern': 0},
    'main/pthread_join4': {'file': '<memory>', 'line': 25, 'col': 5, 'extern': 0},
    'main/printf5': {'file': '<memory>', 'line': 26, 'col': 5, 'extern': 0},
    'main/if/__stack_chk_fail6': {'file': '<memory>', 'line': 28, 'col': 1, 'extern': 0},
}
FUNCTIONS_FULL = {
    'worker': {
        'mycalls': ['worker/if/pthread_mutex_lock1', 'worker/if/pthread_mutex_unlock2'],
        'myinfo': {'tail': 'worker/if/pthread_mutex_unlock2'},
        'mycalls_meta': WORKER_META,
    },
    'main': {
        'mycalls': ['main/pthread_create1', 'worker', 'main/pthread_create2',
                    'worker@instance1', 'main/pthread_join3', 'main/pthread_join4',
                    'main/printf5', 'main/if/__stack_chk_fail6'],
        'myinfo': {
            'tail': 'main/if/__stack_chk_fail6',
            'pthread_create': 'worker',
            '__create_queue__': ['worker'],
            'main/pthread_join3': 't1',
            'main/pthread_join4': 't2',
            't1': 'worker',
            't2': 'worker',
        },
        'mycalls_meta': MAIN_META,
    },
    'worker@instance1': {
        'mycalls': ['worker/if/pthread_mutex_lock1', 'worker/if/pthread_mutex_unlock2'],
        'myinfo': {'tail': 'worker/if/pthread_mutex_unlock2'},
        'mycalls_meta': WORKER_META,
    },
}
FUNCTIONS_RANGES = {
    'functions': [
        {'name': 'worker', 'base_name': 'worker', 'start_line': 7,
         'body_start_line': 7, 'first_stmt_line': 8, 'end_line': 17,
         'last_stmt_line': 16, 'last_return_line': 16,
         'level1_start_line': 8, 'level1_end_line': 16},
        {'name': 'main', 'base_name': 'main', 'start_line': 19,
         'body_start_line': 19, 'first_stmt_line': 20, 'end_line': 28,
         'last_stmt_line': 27, 'last_return_line': 27,
         'level1_start_line': 20, 'level1_end_line': 27},
        {'name': 'worker@instance1', 'base_name': 'worker', 'start_line': 7,
         'body_start_line': 7, 'first_stmt_line': 8, 'end_line': 17,
         'last_stmt_line': 16, 'last_return_line': 16,
         'level1_start_line': 8, 'level1_end_line': 16},
    ]
}
INTERNAL_META = {'worker': WORKER_META, 'main': MAIN_META, 'worker@instance1': WORKER_META}

SINGLE_SOURCE = """#include <pthread.h>

static int shared = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
    pthread_mutex_lock(&lock);
    shared += 1;
    pthread_mutex_unlock(&lock);
    return NULL;
}

int main(void) {
    pthread_t t1, mystery;
    pthread_create(&t1, NULL, worker, NULL);
    pthread_join(mystery, NULL);
    return 0;
}
"""
SINGLE_WORKER_META = {
    'worker/pthread_mutex_lock1': {'file': '<memory>', 'line': 7, 'col': 5, 'extern': 0},
    'worker/pthread_mutex_unlock2': {'file': '<memory>', 'line': 9, 'col': 5, 'extern': 0},
}
SINGLE_MAIN_META = {
    'main/pthread_create1': {'file': '<memory>', 'line': 15, 'col': 5, 'extern': 0},
    'main/pthread_join2': {'file': '<memory>', 'line': 16, 'col': 5, 'extern': 0},
}
SINGLE_FUNCTIONS_FULL = {
    'worker': {
        'mycalls': ['worker/pthread_mutex_lock1', 'worker/pthread_mutex_unlock2'],
        'myinfo': {'tail': 'worker/pthread_mutex_unlock2'},
        'mycalls_meta': SINGLE_WORKER_META,
    },
    'main': {
        'mycalls': ['main/pthread_create1', 'worker', 'main/pthread_join2'],
        'myinfo': {
            'tail': 'main/pthread_join2',
            'pthread_create': 'worker',
            '__create_queue__': ['worker'],
            'main/pthread_join2': 'mystery',
            'mystery': 'worker',
        },
        'mycalls_meta': SINGLE_MAIN_META,
    },
}
SINGLE_FUNCTIONS_RANGES = {
    'functions': [
        {'name': 'worker', 'base_name': 'worker', 'start_line': 6,
         'body_start_line': 6, 'first_stmt_line': 7, 'end_line': 11,
         'last_stmt_line': 10, 'last_return_line': 10,
         'level1_start_line': 7, 'level1_end_line': 10},
        {'name': 'main', 'base_name': 'main', 'start_line': 13,
         'body_start_line': 13, 'first_stmt_line': 14, 'end_line': 18,
         'last_stmt_line': 17, 'last_return_line': 17,
         'level1_start_line': 14, 'level1_end_line': 17},
    ]
}
SINGLE_INTERNAL_META = {'worker': SINGLE_WORKER_META, 'main': SINGLE_MAIN_META}


class JoinPairingTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        native = self.root / 'native'
        gen = native / 'intermediate_results' / 'two_instances' / 'dag_generation'
        (gen / 'debug').mkdir(parents=True)
        (native / 'intermediate_results' / 'two_instances' / 'config_files').mkdir(parents=True)
        self.gen = gen
        self.native = native
        self.source = self.root / 'two_instances.c'
        self.base_dir = native
        self.base_name = 'two_instances'
        self.configure(SOURCE, FUNCTIONS_FULL, FUNCTIONS_RANGES, INTERNAL_META)

    def configure(self, source, functions_full, functions_ranges, internal_meta, name='two_instances'):
        self.source.write_text(source)
        self.gen = self.native / 'intermediate_results' / name / 'dag_generation'
        (self.gen / 'debug').mkdir(parents=True, exist_ok=True)
        (self.native / 'intermediate_results' / name / 'config_files').mkdir(parents=True, exist_ok=True)
        (self.gen / 'functions_full.json').write_text(json.dumps(functions_full))
        (self.gen / 'functions_ranges.json').write_text(json.dumps(functions_ranges))
        (self.gen / 'debug' / 'mycalls_meta_internal.json').write_text(json.dumps(internal_meta))
        (self.gen / 'dag.dot').write_text('strict digraph callgraph {}')
        (self.native / 'intermediate_results' / name / 'config_files' / 'circle.txt').write_text('')
        self.source_text = source
        self.base_name = name

    def tearDown(self):
        self._tmp.cleanup()

    def base_graph(self):
        return build_level2_segments_and_dag(
            base_dir=self.base_dir, base_name=self.base_name, source_file=self.source)

    def final_views(self):
        segments, dag = self.base_graph()
        raw = dict(segments)
        raw['mappings'] = {}
        return build_branch_lock_views(
            base_name=self.base_name, source_text=self.source_text,
            base_segments_json=raw, base_dag_json=dag), segments, dag

    def test_base_join_edges_pair_by_create_handle(self):
        _segments, dag = self.base_graph()
        create = {(e['src'], e['dst']) for e in dag['edges'] if e['kind'] == 'create'}
        joins = {(e['src'], e['dst']) for e in dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({
            ('SEG:main#001@20-22', 'SEG:worker#001@8-9'),
            ('SEG:main#002@23-23', 'SEG:worker@instance1#001@8-9'),
        }, create)
        self.assertEqual({
            ('SEG:worker#003@13-15', 'SEG:main#003@24-24'),
            ('SEG:worker@instance1#003@13-15', 'SEG:main#004@25-26'),
        }, joins)
        self.assertNotIn(('SEG:worker#003@13-15', 'SEG:main#004@25-26'), joins)

    def test_end_to_end_reachability_branch_exit_and_mappings(self):
        (struct, struct_dag, sched, sched_dag, meta), _segments, _dag = self.final_views()
        self.assertTrue(meta['scheduler_compatible'])

        groups = {g['group_id']: g for g in sched['lock_groups']}
        self.assertEqual({
            'LOCKGROUP:lock@COND:worker#001:then',
            'LOCKGROUP:lock@COND:worker@instance1#001:then',
        }, set(groups))
        self.assertTrue(all(g['mutex_id'] == 'lock' for g in groups.values()))

        direct_join = {(e['src'], e['dst']) for e in sched_dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({
            ('LOCKGROUP:lock@COND:worker#001:then', 'SEG:main#003@24-24'),
            ('BR:worker#005@14-14', 'SEG:main#003@24-24'),
            ('LOCKGROUP:lock@COND:worker@instance1#001:then', 'SEG:main#004@25-26'),
            ('BR:worker@instance1#005@14-14', 'SEG:main#004@25-26'),
        }, direct_join)

        adj = {node: [] for node in sched_dag['nodes']}
        for edge in sched_dag['edges']:
            adj[edge['src']].append(edge['dst'])

        def reachable(start, target):
            pending, seen = [start], set()
            while pending:
                node = pending.pop()
                if node == target:
                    return True
                if node not in seen:
                    seen.add(node)
                    pending.extend(adj.get(node, []))
            return False

        self.assertTrue(reachable('BR:worker#001@8-8', 'SEG:main#003@24-24'))
        self.assertTrue(reachable('BR:worker@instance1#001@8-8', 'SEG:main#004@25-26'))
        self.assertFalse(reachable('BR:worker@instance1#001@8-8', 'SEG:main#003@24-24'))

        positions = sched['mappings']['source_position_to_execution_segments']
        for fn, group in (('worker', 'LOCKGROUP:lock@COND:worker#001:then'),
                          ('worker@instance1', 'LOCKGROUP:lock@COND:worker@instance1#001:then')):
            for line in (10, 11, 12):
                self.assertEqual([group], positions[f'LOOPSRC:{fn}:line:{line}'])
            else_row = 'BR:worker#005@14-14' if fn == 'worker' else 'BR:worker@instance1#005@14-14'
            self.assertEqual([else_row], positions[f'LOOPSRC:{fn}:line:14'])

        topo_sort(struct_dag['nodes'], [(e['src'], e['dst']) for e in struct_dag['edges']])
        topo_sort(sched_dag['nodes'], [(e['src'], e['dst']) for e in sched_dag['edges']])

    def test_reversed_join_order_pairs_by_handle_not_source_order(self):
        source = SOURCE.replace(
            "    pthread_join(t1, NULL);\n    pthread_join(t2, NULL);",
            "    pthread_join(t2, NULL);\n    pthread_join(t1, NULL);")
        functions_full = copy.deepcopy(FUNCTIONS_FULL)
        functions_full['main']['myinfo']['main/pthread_join3'] = 't2'
        functions_full['main']['myinfo']['main/pthread_join4'] = 't1'
        self.configure(source, functions_full, FUNCTIONS_RANGES, INTERNAL_META)

        _segments, dag = self.base_graph()
        joins = {(e['src'], e['dst']) for e in dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({
            ('SEG:worker@instance1#003@13-15', 'SEG:main#003@24-24'),
            ('SEG:worker#003@13-15', 'SEG:main#004@25-26'),
        }, joins)

        (_struct, _struct_dag, sched, sched_dag, _meta), _segments, _dag = self.final_views()
        direct = {(e['src'], e['dst']) for e in sched_dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({
            ('LOCKGROUP:lock@COND:worker@instance1#001:then', 'SEG:main#003@24-24'),
            ('BR:worker@instance1#005@14-14', 'SEG:main#003@24-24'),
            ('LOCKGROUP:lock@COND:worker#001:then', 'SEG:main#004@25-26'),
            ('BR:worker#005@14-14', 'SEG:main#004@25-26'),
        }, direct)
        self.assertNotIn(
            ('LOCKGROUP:lock@COND:worker#001:then', 'SEG:main#003@24-24'), direct)

    def test_unresolved_join_handle_with_multiple_instances_raises(self):
        source = SOURCE.replace('pthread_t t1, t2;', 'pthread_t t1, t2, mystery;').replace(
            "    pthread_join(t2, NULL);", "    pthread_join(mystery, NULL);")
        functions_full = copy.deepcopy(FUNCTIONS_FULL)
        functions_full['main']['myinfo']['main/pthread_join4'] = 'mystery'
        functions_full['main']['myinfo']['mystery'] = 'worker'
        self.configure(source, functions_full, FUNCTIONS_RANGES, INTERNAL_META)
        with self.assertRaisesRegex(
                ValueError, r"ambiguous join handle 'mystery' at line 25: "
                            r"multiple execution instances"):
            self.base_graph()

    def test_unresolved_join_handle_with_single_instance_keeps_fallback(self):
        self.configure(SINGLE_SOURCE, SINGLE_FUNCTIONS_FULL,
                       SINGLE_FUNCTIONS_RANGES, SINGLE_INTERNAL_META, name='single')
        segments, dag = self.base_graph()
        worker = {row['seg_id'] for row in segments['segments'] if row['function'] == 'worker'}
        joins = {(e['src'], e['dst']) for e in dag['edges'] if e['kind'] == 'join'}
        self.assertEqual({('MU:worker#001@7-9', 'SEG:main#002@16-16')}, joins)
        self.assertTrue(joins and all(src in worker for src, _dst in joins))

    def test_unresolved_join_without_base_mapping_and_multiple_instances_raises(self):
        source = SOURCE.replace('pthread_t t1, t2;', 'pthread_t t1, t2, mystery;').replace(
            "    pthread_join(t2, NULL);", "    pthread_join(mystery, NULL);")
        functions_full = copy.deepcopy(FUNCTIONS_FULL)
        functions_full['main']['myinfo']['main/pthread_join4'] = 'mystery'
        functions_full['main']['myinfo'].pop('mystery', None)
        self.configure(source, functions_full, FUNCTIONS_RANGES, INTERNAL_META)
        with self.assertRaisesRegex(
                ValueError, r"cannot resolve join handle 'mystery' at line 25: "
                            r"no base function mapping and multiple execution instances"):
            self.base_graph()

    def test_unresolved_join_without_any_mapping_single_instance_keeps_skip(self):
        functions_full = copy.deepcopy(SINGLE_FUNCTIONS_FULL)
        functions_full['main']['myinfo'].pop('mystery', None)
        self.configure(SINGLE_SOURCE, functions_full,
                       SINGLE_FUNCTIONS_RANGES, SINGLE_INTERNAL_META, name='single')
        _segments, dag = self.base_graph()
        self.assertEqual([], [e for e in dag['edges'] if e['kind'] == 'join'])


if __name__ == '__main__':
    unittest.main()
