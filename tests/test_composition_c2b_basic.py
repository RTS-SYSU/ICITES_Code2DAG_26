"""Native source-to-graph tests for main locks and call-context Conditions."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests.test_composition_c2b_repeated_entries import SOURCE
from Code2DAG.pipeline.composition import select_rule, run_composition_blocks
from Code2DAG.pipeline.algo.common import topo_sort

MAIN_LOCK='''    pthread_mutex_lock(&m);
    value += 5;
    pthread_mutex_unlock(&m);
'''
HELPER='''static void inspect(void) {
    if (mode == 3) {
        value += 7;
    }
    return;
}
'''
MAIN_ONLY='''#include <pthread.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int value;
int main(void) {
    pthread_mutex_lock(&m);
    if (value) {
        value += 1;
    } else {
        value += 2;
    }
    pthread_mutex_unlock(&m);
    return 0;
}
'''


def variants():
    main=SOURCE.replace('    pthread_t a, b;\n','    pthread_t a, b;\n'+MAIN_LOCK)
    sequential=main.replace('    value += 100;','    if (mode == 2) {\n        value += 3;\n    }\n    value += 100;')
    helper=main.replace('void *worker',HELPER+'void *worker').replace('        value += 1;','        value += 1;\n        inspect();').replace('        value += 2;','        value += 2;\n        inspect();')
    start=SOURCE.index('    if (mode)');end=SOURCE.index('    value += 100;')
    helper_only=(SOURCE[:start]+'    inspect();\n'+SOURCE[end:]).replace('void *worker',HELPER+'void *worker')
    unused=helper_only.replace('static void inspect', 'static void untouched(void) {\n    value += 99;\n}\nstatic void inspect')
    return dict(main=main,empty_main=main.replace('    value += 5;\n',''),sequential=sequential,
                helper=helper,helper_only=helper_only,unused=unused,main_only=MAIN_ONLY)


class BasicC2bTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.cache={}
        for name,text in variants().items():
            root=Path(cls.temp.name)/name;root.mkdir();source=root/(name+'.c');source.write_text(text)
            frozen=hashlib.sha256(source.read_bytes()).hexdigest()
            if select_rule(text)!='composition_c2b':raise AssertionError('wrong automatic rule')
            base,p=c1.CompositionC1Tests().native(root,source)
            run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            block=p/'blocks/level2/composition_c2b'
            views=[json.loads((block/(n+'.json')).read_text()) for n in ('segments_struct','dag_struct','segments_sched','dag_sched')]
            if frozen!=hashlib.sha256(source.read_bytes()).hexdigest():raise AssertionError('source changed')
            cls.cache[name]=(text,views)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_native_main_empty_main_and_main_only_keep_mutex_and_acyclicity(self):
        for name in ('main','empty_main','main_only'):
            with self.subTest(name=name):
                _,(st,sd,ss,dd)=self.cache[name]
                regions=ss['lock_groups'][0]['members']
                self.assertEqual(1,sum(r['function']=='main' for r in regions))
                self.assertTrue(any(r['function']=='main' and r.get('mutex_id')=='m' for r in ss['segments']))
                for graph in (sd,dd):topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                if name=='main_only':
                    self.assertFalse(any(e['kind'] in ('create','join') for e in dd['edges']))
        from Code2DAG.pipeline.composition_c2b_timing import import_lock_path_timing
        with self.assertRaisesRegex(ValueError,'structural analysis only'):
            import_lock_path_timing(base_dir=Path(self.temp.name)/'main/native',base_name='main',input_timing=Path(self.temp.name)/'unused.json')

    def test_adjacent_main_unlock_and_create_keep_precise_handle_pairing(self):
        text,(st,sd,ss,dd)=self.cache['main'];rows={r['seg_id']:r for r in ss['segments']}
        lines=text.splitlines()
        for instance,handle in [('worker','a'),('worker@instance1','b')]:
            create=next(i for i,l in enumerate(lines,1) if 'pthread_create(&'+handle in l)
            join=next(i for i,l in enumerate(lines,1) if 'pthread_join('+handle in l)
            incoming=[e for e in dd['edges'] if e['kind']=='create' and rows[e['dst']]['function']==instance]
            outgoing=[e for e in dd['edges'] if e['kind']=='join' and rows[e['src']]['function']==instance]
            self.assertTrue(incoming and outgoing)
            self.assertTrue(all(rows[e['src']]['kind']=='compute' and 'mutex_id' not in rows[e['src']] for e in incoming))
            self.assertTrue(all(rows[e['src']]['start_line']==create==rows[e['src']]['end_line'] for e in incoming))
            self.assertTrue(all(rows[e['dst']]['start_line']==join for e in outgoing))

    def test_sequential_conditions_form_four_paths_per_worker(self):
        _,(st,sd,ss,dd)=self.cache['sequential']
        for fn in ('worker','worker@instance1'):
            self.assertEqual(2,sum(c['function']==fn for c in ss['condition_regions']))
            paths=[r for r in ss['segments'] if r['function']==fn]
            self.assertEqual(4,len(paths));self.assertTrue(all(len(r['condition_memberships'])==2 for r in paths))

    def test_shared_helper_is_instantiated_per_call_and_thread_with_parent_guard(self):
        _,(st,sd,ss,dd)=self.cache['helper'];ctx=ss['mappings']['helper_call_contexts']
        self.assertEqual(4,len(ctx));self.assertFalse(any(r['function']=='inspect' for r in st['segments']))
        for name,site in ctx.items():
            rows=[r for r in st['segments'] if r['function']==name]
            self.assertTrue(rows);self.assertTrue(site['condition_memberships'])
            outer=site['condition_memberships'][0]
            self.assertTrue(all(outer in r['condition_memberships'] for r in rows))
            cond=[c for c in ss['condition_regions'] if c['function']==name]
            self.assertEqual(1,len(cond));self.assertEqual(outer['exclusive_group'],cond[0]['parent_id'])
            self.assertTrue(all(r.get('mutex_id')=='m' for r in rows))
        owner={r['seg_id']:r for r in st['segments']}
        for edge in sd['edges']:
            a,b=owner[edge['src']],owner[edge['dst']]
            if a['execution_context']!=b['execution_context']:
                self.assertIn(edge['kind'],('create','join'))

    def test_leaf_helper_without_internal_calls_and_no_outer_condition(self):
        text,(st,sd,ss,dd)=self.cache['helper_only']
        self.assertEqual(2,len(ss['mappings']['helper_call_contexts']))
        self.assertEqual(2,len(ss['condition_regions']))
        self.assertTrue(all(c.get('source_function')=='inspect' for c in ss['condition_regions']))
        line=next(i for i,l in enumerate(text.splitlines(),1) if 'value += 7;' in l)
        key='LOOPSRC:inspect:line:'+str(line)
        self.assertTrue(ss['mappings']['source_position_to_execution_segments_by_source'][key])

    def test_unused_helper_is_not_an_execution_and_all_source_maps_are_valid(self):
        _,(st,sd,ss,dd)=self.cache['unused']
        self.assertFalse(any(r.get('source_function')=='untouched' for r in st['segments']))
        ids=set(dd['nodes'])
        for key in ('source_position_to_execution_segments','source_position_to_execution_segments_by_source',
                    'generation_node_to_execution_segments','original_segment_to_execution_segments',
                    'structural_node_to_execution_segments'):
            self.assertTrue(all(set(v)<=ids for v in ss['mappings'][key].values()))

    def reject_native(self,text,reason):
        with tempfile.TemporaryDirectory() as temp:
            source=Path(temp)/'reject.c';source.write_text(text)
            base,p=c1.CompositionC1Tests().native(temp,source)
            with self.assertRaisesRegex(ValueError,reason):run_composition_blocks(base_dir=base,base_name='reject',source_file=source)
            self.assertFalse((p/'blocks/level2/composition_c2b/segments.json').exists())

    def test_recursive_helper_mutex_alias_and_local_mutex_remain_rejected(self):
        recursive=variants()['helper'].replace('    return;','    inspect();\n    return;',1)
        self.reject_native(recursive,'recursive/nested')
        alias=variants()['main'].replace('    pthread_t a, b;','    pthread_t a, b;\n    void *alias = &m;')
        self.reject_native(alias,'aliases or extra uses')
        local=MAIN_ONLY.replace('pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;\n','').replace('int main(void) {','int main(void) {\n    pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;')
        self.reject_native(local,'must be global')
        indirect=MAIN_ONLY.replace('int main(void) {','void hook(void) { value += 4; }\nint main(void) {').replace(
            '    if (value) {','    void (*fp)(void) = hook;\n    fp();\n    if (value) {')
        self.reject_native(indirect,'indirect calls')

    def test_path_expansion_limit_fails_before_publication(self):
        extra=''.join('    if (mode == '+str(i)+') {\n        value += 1;\n    }\n' for i in range(5))
        text=variants()['main'].replace('    value += 100;',extra+'    value += 100;')
        self.reject_native(text,'path limit')


if __name__=='__main__':unittest.main()
