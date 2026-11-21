"""Repeated C2b workers: separate executions, shared mutex, handle-bound joins."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.composition import select_rule, run_composition_blocks
from Code2DAG.pipeline.composition_c2b_model import build_lock_path_views
from Code2DAG.pipeline.composition_model import CompositionError
from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.tests import test_composition_c1 as c1


SOURCE = '''#include <pthread.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int mode = 1;
int value;
void *worker(void *arg) {
    pthread_mutex_lock(&m);
    value += 10;
    if (mode) {
        value += 1;
    } else {
        value += 2;
    }
    value += 100;
    pthread_mutex_unlock(&m);
    return NULL;
}
int main(void) {
    pthread_t a, b;
    pthread_create(&a, NULL, worker, NULL);
    pthread_create(&b, NULL, worker, NULL);
    pthread_join(b, NULL);
    pthread_join(a, NULL);
    printf("%d\\n", value);
    return 0;
}
'''


class RepeatedEntriesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory()
        cls.cache={}
        three=SOURCE.replace('pthread_t a, b;','pthread_t a, b, c;').replace(
            '    pthread_join(b, NULL);','    pthread_create(&c, NULL, worker, NULL);\n    pthread_join(c, NULL);\n    pthread_join(b, NULL);')
        nested=SOURCE.replace('        value += 1;','        if (mode == 2) {\n            value += 3;\n        } else {\n            value += 4;\n        }')
        extra=SOURCE.replace('    return NULL;','    pthread_mutex_lock(&m);\n    value += 1000;\n    pthread_mutex_unlock(&m);\n    return NULL;',1)
        for name,text in [('two',SOURCE),('three',three),('nested',nested),('extra',extra)]:
            root=Path(cls.temp.name)/name;root.mkdir()
            source=root/(name+'.c');source.write_text(text)
            before=hashlib.sha256(source.read_bytes()).hexdigest()
            if select_rule(text)!='composition_c2b':raise AssertionError('wrong rule')
            base,p=c1.CompositionC1Tests().native(root,source)
            raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
            run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            block=p/'blocks/level2/composition_c2b'
            views=[json.loads((block/(n+'.json')).read_text()) for n in
                   ('segments_struct','dag_struct','segments_sched','dag_sched')]
            if before!=hashlib.sha256(source.read_bytes()).hexdigest():raise AssertionError('source mutated')
            cls.cache[name]=(text,raw,dag,views)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_two_and_three_instances_keep_conditions_paths_mutex_and_no_false_order(self):
        for name,count in [('two',2),('three',3),('nested',2)]:
            with self.subTest(name=name):
                text,raw,dag,(st,sd,ss,dd)=self.cache[name]
                functions={'worker'}|{'worker@instance'+str(i) for i in range(1,count)}
                paths=[r for r in ss['segments'] if r['kind']=='lock_path_candidate']
                self.assertEqual(count*(3 if name=='nested' else 2),len(paths))
                self.assertEqual(functions,{r['function'] for r in paths})
                self.assertEqual(count,len(ss['lock_groups'][0]['members']))
                self.assertEqual({'m'},{r['mutex_id'] for r in paths})
                for fn in functions:
                    self.assertEqual(2 if name=='nested' else 1,
                                     sum(r['function']==fn for r in ss['condition_regions']))
                for graph in (sd,dd):
                    topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                for a in paths:
                    for b in paths:
                        if a is not b:self.assertFalse(c1.reachable(dd,a['seg_id'],b['seg_id']))

    def test_reversed_joins_follow_handles_and_creates_follow_instances(self):
        text,raw,dag,(st,sd,ss,dd)=self.cache['two']
        by_id={r['seg_id']:r for r in ss['segments']}
        create_lines={'worker':20,'worker@instance1':21}
        join_lines={'worker':23,'worker@instance1':22}
        for fn in create_lines:
            paths=[r['seg_id'] for r in ss['segments'] if r['function']==fn]
            for sid in paths:
                creates=[by_id[e['src']] for e in dd['edges'] if e['dst']==sid and e['kind']=='create']
                joins=[by_id[e['dst']] for e in dd['edges'] if e['src']==sid and e['kind']=='join']
                self.assertEqual(1,len(creates));self.assertEqual(1,len(joins))
                self.assertLessEqual(creates[0]['start_line'],create_lines[fn]);self.assertEqual(create_lines[fn],creates[0]['end_line'])
                self.assertEqual(join_lines[fn],joins[0]['start_line'])
        self.assertEqual(8,len(dd['nodes']));self.assertEqual(11,len(dd['edges']))

    def test_source_and_generation_maps_are_instance_aware(self):
        text,raw,dag,(st,sd,ss,dd)=self.cache['two'];by_id={r['seg_id']:r for r in ss['segments']}
        maps=ss['mappings'];instance_maps=maps['generation_node_to_execution_segments_by_instance']
        self.assertEqual({'worker','worker@instance1'},set(instance_maps))
        for fn,table in instance_maps.items():
            self.assertEqual({'worker/pthread_mutex_lock1','worker/pthread_mutex_unlock2'},set(table))
            for node,targets in table.items():
                self.assertEqual(2,len(targets));self.assertTrue(all(by_id[t]['function']==fn for t in targets))
                self.assertEqual(set(maps['generation_node_to_execution_segments'][node]),
                                 set().union(*(set(t[node]) for t in instance_maps.values())))
            for line,branch in [(10,'then'),(12,'else')]:
                targets=maps['source_position_to_execution_segments'][f'LOOPSRC:{fn}:line:{line}']
                self.assertEqual(1,len(targets));r=by_id[targets[0]]
                self.assertEqual(fn,r['function']);self.assertEqual(branch,r['condition_memberships'][0]['branch_id'])
            for line in (7,8,14,15):
                targets=maps['source_position_to_execution_segments'][f'LOOPSRC:{fn}:line:{line}']
                self.assertEqual(2,len(targets));self.assertTrue(all(by_id[t]['function']==fn for t in targets))

    def test_additional_straight_line_regions_keep_each_execution(self):
        _,_,_,(_,_,ss,dd)=self.cache['extra']
        self.assertEqual(4,len(ss['lock_groups'][0]['members']))
        ordinary=[r for r in ss['segments'] if r['kind']=='mutex_cs']
        self.assertEqual({'worker','worker@instance1'},{r['function'] for r in ordinary})
        self.assertTrue(all(r['generation_origin'] for r in ordinary))

    def test_cross_instance_dependency_is_projected_not_discarded_as_internal(self):
        text,raw,dag,_=copy.deepcopy(self.cache['two'])
        members=[r for r in raw['segments'] if r['kind']=='mutex_cs']
        dag['edges'].append(dict(src=members[0]['seg_id'],dst=members[1]['seg_id'],kind='custom',proof='keep'))
        frozen=copy.deepcopy((raw,dag))
        st,sd,ss,dd,_=build_lock_path_views(base_name='two',source_text=text,base_segments_json=raw,base_dag_json=dag)
        edges=[e for e in dd['edges'] if e['kind']=='custom']
        self.assertEqual(4,len(edges));self.assertTrue(all(e['proof']=='keep' for e in edges))
        self.assertEqual(frozen,(raw,dag))

    def test_missing_execution_or_create_edge_is_rejected(self):
        text,raw,dag,_=self.cache['two']
        missing=copy.deepcopy(raw);missing['thread_entry_functions'].remove('worker@instance1')
        with self.assertRaisesRegex(CompositionError,'instances do not match'):
            build_lock_path_views(base_name='two',source_text=text,base_segments_json=missing,base_dag_json=dag)
        edges=copy.deepcopy(dag);edges['edges']=[e for e in edges['edges'] if e['kind']!='create']
        with self.assertRaisesRegex(CompositionError,'create edges'):
            build_lock_path_views(base_name='two',source_text=text,base_segments_json=raw,base_dag_json=edges)
        from Code2DAG.pipeline.condition_model import analyze_conditionals
        wrong_owner=analyze_conditionals(text,{'main':(1,len(text.splitlines()))})
        with patch('Code2DAG.pipeline.composition_c2b_model.analyze_conditionals',return_value=wrong_owner):
            with self.assertRaisesRegex(CompositionError,'one matched lock/unlock'):
                build_lock_path_views(base_name='two',source_text=text,base_segments_json=raw,base_dag_json=dag)

    def test_reused_handle_and_loop_remain_rejected(self):
        with self.assertRaisesRegex(CompositionError,'distinct create handles'):
            select_rule(SOURCE.replace('pthread_create(&b,','pthread_create(&a,'))
        text,raw,dag,_=self.cache['two']
        text+='\nvoid loop(void) {\n    for (int i = 0; i < 2; i++) {\n        value += i;\n    }\n}\n'
        with self.assertRaisesRegex(CompositionError,'with Loop are unsupported'):
            build_lock_path_views(base_name='two',source_text=text,base_segments_json=raw,base_dag_json=dag)

    def test_missing_duplicate_and_unknown_join_handles_are_rejected(self):
        text,raw,dag,_=self.cache['two']
        for changed in [text.replace('    pthread_join(b, NULL);',''),
                        text.replace('pthread_join(b, NULL)','pthread_join(a, NULL)'),
                        text.replace('pthread_join(b, NULL)','pthread_join(unknown, NULL)')]:
            with self.subTest(source=changed), self.assertRaisesRegex(CompositionError,'one direct join per create handle'):
                build_lock_path_views(base_name='two',source_text=changed,base_segments_json=raw,base_dag_json=dag)


if __name__=='__main__':
    unittest.main()
