"""Path-specific critical sections: no worst-case merge or common-source loss."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests import test_composition_c2a as c2a
from Code2DAG.pipeline.composition_c2b_model import build_lock_path_views,RULE
from Code2DAG.pipeline.composition_c2b_timing import import_lock_path_timing
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort
ROOT=Path(__file__).resolve().parents[2]
SOURCES=ROOT/'Code2DAG/source_files/composition_c2b'
EXPECTED={'basic':[10,20],'common':[15,25],'no_else':[15,5],'else_if':[15,25,35],'nested_then':[19,29,35],'nested_else':[15,29,39],'other_lock':[22,32],'other_loop':[18,28]}


class CompositionC2bTests(unittest.TestCase):
    def fixture(self,name,text=None):
        source=SOURCES/(name+'.c');text=source.read_text() if text is None else text
        raw,dag=c2a.fixture(text,source);return text,raw,dag

    def test_candidate_count_membership_and_common_one_to_many(self):
        for name,values in EXPECTED.items():
            with self.subTest(name=name):
                text,raw,dag=self.fixture(name);before=copy.deepcopy((raw,dag))
                st,sd,s,d,_=build_lock_path_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
                self.assertEqual(before,(raw,dag));paths=[r for r in s['segments'] if r['kind']=='lock_path_candidate']
                self.assertEqual([e['edge'] for e in s['composition_edge_provenance']],dag['edges'])
                structural={r['seg_id']:r for r in st['segments']}
                for candidate in paths:
                    route=candidate['source_structural_nodes']
                    for a,b in zip(route,route[1:]):self.assertTrue(any(e['src']==a and e['dst']==b for e in sd['edges']))
                    for membership in candidate['condition_memberships']:
                        controls=[r for r in st['segments'] if r.get('condition_id')==membership['exclusive_group']]
                        self.assertEqual({'condition_entry','condition_exit'},{r['kind'] for r in controls})
                        self.assertTrue(all(r['seg_id'] in route for r in controls))
                    self.assertTrue(any(e['dst']==candidate['seg_id'] for e in d['edges']))
                    self.assertTrue(any(e['src']==candidate['seg_id'] for e in d['edges']))
                first=paths[0]
                self.assertEqual([r['seg_id'] for r in paths],s['mappings']['original_segment_to_execution_segments'][first['lock_group']['source_region']['member_segment_id']])
                for node,loc in raw['node_locations'].items():
                    if loc['function']=='worker' and loc['line'] in [first['start_line'],first['end_line']]:
                        self.assertEqual([r['seg_id'] for r in paths],s['mappings']['generation_node_to_execution_segments'][node])
                self.assertEqual(len(values),len(paths));self.assertTrue(all(r['condition_memberships'] for r in paths))
                self.assertEqual(1,len({r['lock_group']['group_id'] for r in paths}));self.assertEqual({'m'},{r['mutex_id'] for r in paths})
                for graph in [sd,d]:topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']]);self.assertEqual(len(graph['nodes']),len(set(graph['nodes'])))
                for a in paths:
                    for b in paths:
                        if a is not b:self.assertFalse(c1.reachable(d,a['seg_id'],b['seg_id']))
                for i,line in enumerate(text.splitlines(),1):
                    if 'COMMON_' in line:
                        key='LOOPSRC:worker:line:'+str(i)
                        self.assertEqual([r['seg_id'] for r in paths],s['mappings']['source_position_to_execution_segments'][key])
                for path in paths:
                    for sid in path['member_segment_ids']:
                        row=next(r for r in st['segments'] if r['seg_id']==sid)
                        self.assertTrue(all(m in path['condition_memberships'] for m in row.get('condition_memberships',[])))

    def test_native_independent_loop_minimal_reproduction(self):
        source=SOURCES/'other_loop.c';before=source.read_bytes()
        with tempfile.TemporaryDirectory() as tmp:
            base,_=c1.CompositionC1Tests().native(tmp,source)
            run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
        self.assertEqual(before,source.read_bytes())


    def test_formal_region_attributes_and_immutability(self):
        from Code2DAG.pipeline.condition_model import analyze_conditionals,ConditionalRegion
        from Code2DAG.pipeline.composition_c2b_model import _condition_region_spans
        for name in ['basic','no_else','else_if','nested_then','nested_else','other_loop']:
            text,raw,dag=self.fixture(name)
            ranges={fn:(min(r['start_line'] for r in raw['segments'] if r['function']==fn),max(r['end_line'] for r in raw['segments'] if r['function']==fn)) for fn in ['main','worker']}
            regions=analyze_conditionals(text,ranges).regions;before=copy.deepcopy(regions)
            with self.subTest(name=name):
                self.assertTrue(all(isinstance(r,ConditionalRegion) for r in regions))
                self.assertEqual([(r.start_offset,r.end_offset) for r in regions],_condition_region_spans(regions))
                self.assertEqual(before,regions)

    def test_invalid_region_types_reject_before_dag_publication(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from Code2DAG.pipeline.condition_model import Analysis
        from Code2DAG.pipeline.composition_model import CompositionError
        from Code2DAG.pipeline.runner import run_blocks
        from Code2DAG.pipeline.rules.composition_c2b_rule import CompositionC2bRule
        source=SOURCES/'other_loop.c';text,raw,dag=self.fixture('other_loop')
        for invalid in [{},dict(start_offset=0,end_offset=1),SimpleNamespace(start_offset=0),None,42,(0,1)]:
            with self.subTest(value=repr(invalid)),tempfile.TemporaryDirectory() as tmp:
                base=Path(tmp);p=base/'intermediate_results/fixture/pipeline';p.mkdir(parents=True);(p/'block_info.json').write_text(json.dumps({'source_file':str(source)}))
                with patch('Code2DAG.pipeline.composition_c2b_model.analyze_conditionals',return_value=Analysis([invalid])),patch('Code2DAG.pipeline.rules.composition_c2b_rule._build_merge_post_wait'),patch('Code2DAG.pipeline.rules.composition_c2b_rule.build_level2_segments_and_dag',return_value=(raw,dag)):
                    with self.assertRaisesRegex(CompositionError,'formal ConditionalRegion'):run_blocks(base_dir=base,base_name='fixture',level='level2',rule_name=RULE,source_file=source)
                self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_dispatch_matched_function_and_thread_regions(self):
        from Code2DAG.pipeline.composition import select_rule
        from Code2DAG.pipeline.composition_c2b_model import _source_matched_regions
        source=(SOURCES/'common.c').read_text()
        self.assertEqual(RULE,select_rule(source))
        self.assertEqual('composition_c1',select_rule((c1.SOURCES/'kcl.c').read_text()))
        self.assertEqual('composition_c1',select_rule((c1.SOURCES/'klc.c').read_text()))
        # Same mutex in two real thread entries must remain separate pairs.
        ranges,pairs=_source_matched_regions((c1.SOURCES/'kcl.c').read_text())
        self.assertEqual({'worker_a','worker_b'},{p['function'] for p in pairs})
        self.assertTrue(all(p['function']==p['thread_entry_function'] for p in pairs))
        self.assertFalse(any(p['lock_range'][0]<12 and 16<p['unlock_range'][0] for p in pairs))
        # Multiple regions: only one pair encloses the Condition.
        text=(SOURCES/'other_lock.c').read_text();ranges,pairs=_source_matched_regions(text)
        self.assertEqual(2,len(pairs));self.assertEqual(RULE,select_rule(text))
        for _ in range(20):
            self.assertEqual(RULE,select_rule(source));self.assertEqual('composition_c1',select_rule((c1.SOURCES/'kcl.c').read_text()))

    def test_dispatch_cross_thread_or_function_and_partial_boundary(self):
        from Code2DAG.pipeline.composition import select_rule
        from Code2DAG.pipeline.composition_model import CompositionError
        source=(SOURCES/'common.c').read_text()
        # A helper is not a pthread entry, even when called by that thread.
        helper=source.replace('void *worker(void *arg) {','void *helper(void *arg) {')
        with self.assertRaisesRegex(CompositionError,'thread function'):select_rule(helper)
        # Paired only by global order, never by a shared function or thread.
        cross=(c1.SOURCES/'kcl.c').read_text().replace('    pthread_mutex_unlock(&shared_mutex);','',1).replace('    pthread_mutex_lock(&shared_mutex);','',2)
        with self.assertRaises(CompositionError):select_rule(cross)
        # Boundary overlap cannot dispatch to C2b; it is invalid C2a source.
        partial=source.replace('    pthread_mutex_unlock(&m);','').replace('        result += 10; /* THEN */','        result += 10; /* THEN */\n    pthread_mutex_unlock(&m);')
        self.assertNotEqual(RULE,select_rule(partial))
        # The condition is in another function, outside every matched pair.
        elsewhere=(c1.SOURCES/'kcl.c').read_text();self.assertEqual('composition_c1',select_rule(elsewhere))

    def test_native_all_paths_component_timing_and_five_algorithms(self):
        import re
        expected_paths={'basic':[10,20],'common':[15,25],'no_else':[15,5],'else_if':[15,25,35],'nested_then':[19,29,35],'nested_else':[15,29,39],'other_lock':[15,25],'other_loop':[15,25]}
        for name,business in EXPECTED.items():
            source=SOURCES/(name+'.c');before=source.read_bytes();text=source.read_text();lines=text.splitlines();h=c1.CompositionC1Tests()
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
                b=p/'blocks/level2'/RULE;s=json.loads((b/'segments.json').read_text());d=json.loads((b/'dag_seg.json').read_text());struct=json.loads((b/'segments_struct.json').read_text())
                paths=[r for r in s['segments'] if r['kind']=='lock_path_candidate'];self.assertEqual(len(business),len(paths))
                weights={}
                for template in s['templates']:
                    amount=0
                    for line in lines[template['start_line']-1:template['end_line']]:
                        m=re.search(r'result\s*\+=\s*(\d+)\s*;',line)
                        if m:amount+=int(m.group(1))
                        elif re.search(r'result\s*\+=\s*i\s*;',line):amount+=11
                    weights[template['template_id']]=dict(total_ns=amount,count=1,avg_ns=amount,min_ns=amount,max_ns=amount)
                inp=Path(tmp)/'timing.json';inp.write_text(json.dumps({'weights':weights}));original_input=inp.read_bytes()
                result=import_lock_path_timing(base_dir=base,base_name=name,input_timing=inp)
                self.assertEqual(expected_paths[name],[result['weights'][r['seg_id']]['avg_ns'] for r in paths])
                for candidate in paths:
                    self.assertEqual(len(candidate['member_template_ids']),len(set(candidate['member_template_ids'])))
                    for field in ['total_ns','count','avg_ns','min_ns','max_ns']:
                        self.assertEqual(sum(weights[t][field] for t in candidate['member_template_ids']),result['weights'][candidate['seg_id']][field])
                for algo in c1.ALGOS:
                    scheduled=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)
                    self.assertEqual(set(d['nodes']),set(scheduled['priorities']))
                executable=Path(tmp)/'business';h.command(['gcc-13','-pthread','-std=c11',source,'-o',executable])
                for mode,value in enumerate(business):
                    run=h.command([executable,*(['x']*mode)]);self.assertEqual('RESULT:'+str(1100+value)+'\n',run.stdout)
                    selected=paths[mode];self.assertIn(selected['seg_id'],result['weights'])
                    for other in paths:
                        if other is not selected:self.assertFalse(c1.reachable(d,selected['seg_id'],other['seg_id']))
                # Shared generation lock/unlock sources and common positions are one-to-many.
                for node,targets in s['mappings']['generation_node_to_execution_segments'].items():
                    if '/pthread_mutex_' in node and all(r['seg_id'] in targets for r in paths):self.assertEqual([r['seg_id'] for r in paths],targets)
                for i,line in enumerate(lines,1):
                    if 'COMMON_' in line:self.assertEqual([r['seg_id'] for r in paths],s['mappings']['source_position_to_execution_segments']['LOOPSRC:worker:line:'+str(i)])
                output=p/'timing/level2'/RULE/'timing.json';good=output.read_bytes();sp=b/'segments.json';valid=sp.read_bytes()
                for damage in ['path','mutex','member','mapping','cross_template','missing_common','single_node']:
                    bad=json.loads(valid);candidate=next(r for r in bad['segments'] if r['kind']=='lock_path_candidate')
                    if damage=='path':candidate['path_id']='wrong'
                    elif damage=='mutex':candidate['mutex_id']='wrong'
                    elif damage=='member':candidate['member_segment_ids'].append('unknown')
                    elif damage=='mapping':bad['mappings']['original_segment_to_execution_segments'][candidate['source_segments'][0]]=[candidate['seg_id']]
                    elif damage=='cross_template':candidate['member_template_ids']=paths[1]['member_template_ids']
                    elif damage=='missing_common':candidate['member_template_ids']=candidate['member_template_ids'][1:]
                    else:bad['segments']=[r for r in bad['segments'] if r['kind']!='lock_path_candidate']+[candidate]
                    sp.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):import_lock_path_timing(base_dir=base,base_name=name,input_timing=inp)
                    self.assertEqual(good,output.read_bytes())
                sp.write_bytes(valid);self.assertEqual(result,import_lock_path_timing(base_dir=base,base_name=name,input_timing=inp));self.assertEqual(original_input,inp.read_bytes())
            self.assertEqual(before,source.read_bytes())

    def test_dispatch_same_thread_helper_and_second_matching_region(self):
        from Code2DAG.pipeline.composition import select_rule
        text=(c1.SOURCES/'kcl.c').read_text()
        conditional='    if (flag) {\n        value_a += 11; /* THEN */\n    } else {\n        value_a += 13; /* ELSE */\n    }'
        helper='void helper(void) {\n'+conditional+'\n}\n'
        text=text.replace('void *worker_a(void *arg) {',helper+'void *worker_a(void *arg) {').replace(conditional+'\n    for (int i','    helper();\n    for (int i')
        self.assertEqual('composition_c1',select_rule(text))
        multiple=(SOURCES/'other_lock.c').read_text()
        extra='    pthread_mutex_lock(&m);\n    result += 7; /* OTHER_LOCK */\n    pthread_mutex_unlock(&m);\n'
        multiple=multiple.replace(extra,'').replace('    result += 100; /* BEFORE */\n','    result += 100; /* BEFORE */\n'+extra)
        self.assertEqual(RULE,select_rule(multiple))

    def test_reject_unsupported_source_and_preserve_published_outputs(self):
        from Code2DAG.pipeline.runner import run_blocks
        source=SOURCES/'common.c';original=source.read_text()
        sequential=original.replace('    result += 3; /* COMMON_SUFFIX */','    if (mode == 7) {\n        result += 8;\n    }\n    result += 3; /* COMMON_SUFFIX */')
        loop=original.replace('    result += 2; /* COMMON_PREFIX */','    for (int k = 0; k < 2; k++) {\n        result += k;\n    }')
        crossing=original.replace('    pthread_mutex_unlock(&m);','').replace('        result += 10; /* THEN */','        result += 10; /* THEN */\n    pthread_mutex_unlock(&m);')
        h=c1.CompositionC1Tests()
        for text in [sequential,loop,crossing]:
            with self.subTest(text=text),tempfile.TemporaryDirectory() as tmp:
                input_c=Path(tmp)/'rejected.c';input_c.write_text(text);before=input_c.read_bytes();base,p=h.native(tmp,input_c)
                with self.assertRaises(ValueError):run_blocks(base_dir=base,base_name='rejected',level='level2',rule_name=RULE,source_file=input_c)
                self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists());self.assertEqual(before,input_c.read_bytes())

    def test_timing_fresh_failure_common_binding_and_recovery(self):
        source=SOURCES/'common.c';h=c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as tmp:
            base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name='common',source_file=source)
            b=p/'blocks/level2'/RULE;sp=b/'segments.json';valid=sp.read_bytes();s=json.loads(valid)
            weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']};inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));input_before=inp.read_bytes();output=p/'timing/level2'/RULE/'timing.json'
            bad=copy.deepcopy(s);candidate=next(r for r in bad['segments'] if r['kind']=='lock_path_candidate');candidate['member_template_ids']=candidate['member_template_ids'][1:];sp.write_text(json.dumps(bad));damaged=sp.read_bytes()
            with self.assertRaises(ValueError):import_lock_path_timing(base_dir=base,base_name='common',input_timing=inp)
            self.assertFalse(output.exists());self.assertEqual(damaged,sp.read_bytes());self.assertEqual(input_before,inp.read_bytes())
            sp.write_bytes(valid);result=import_lock_path_timing(base_dir=base,base_name='common',input_timing=inp);published=output.read_bytes()
            inp.write_text('{"weights":{},"weights":{}}')
            with self.assertRaises(ValueError):import_lock_path_timing(base_dir=base,base_name='common',input_timing=inp)
            self.assertEqual(published,output.read_bytes())
            inp.write_bytes(input_before);self.assertEqual(result,import_lock_path_timing(base_dir=base,base_name='common',input_timing=inp))
