"""L2 simultaneous multi-mutex quotient; no C instrumentation or runtime experiments."""
import copy
import hashlib
import itertools
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.lock_model import (analyze_lock_l2, build_lock_views, contract_lock_groups,
                                       aggregate_lock_timing, LockModelError, TIMING_FIELDS)
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.errors import StageError
from Code2DAG.pipeline.runner import default_rule_for_level
from Code2DAG.tests.test_lock_l1 import timing_for, ALGOS, ROOT

SOURCES=ROOT/'Code2DAG/source_files/lock_l2'


def fixture(case='unequal', source=None):
    source=source if source is not None else (SOURCES/(case+'.c')).read_text()
    mutexes,regions=analyze_lock_l2(source)
    rows=[];locations={};mapping={};by_mutex={m:[] for m in mutexes}
    for i,r in enumerate(regions):
        sid='CS:'+r['function']+':'+str(r['lock_range'][0]);a,b=r['segment_range']
        rows.append({'seg_id':sid,'function':r['function'],'kind':'mutex_cs','start_line':a,'end_line':b})
        for api,key in [('pthread_mutex_lock','lock_range'),('pthread_mutex_unlock','unlock_range')]:
            n=r['function']+'/'+api+str(i+1);locations[n]={'function':r['function'],'line':r[key][0]};mapping[n]=sid
        by_mutex[r['mutex_id']].append(sid)
    lines=source.splitlines()
    for sid,api in [('P','pthread_create'),('Q','pthread_join')]:
        line=next(i for i,l in enumerate(lines,1) if api+'(' in l)
        rows.append({'seg_id':sid,'function':'main','kind':'compute','start_line':line,'end_line':line,'unrelated_data':sid})
        n='main/'+api+'1';locations[n]={'function':'main','line':line};mapping[n]=sid
    edges=[]
    for sid in list(s['seg_id'] for s in rows if s['kind']=='mutex_cs'):
        edges.extend([{'src':'P','dst':sid,'kind':'create','source_tag':'entry '+sid},
                      {'src':sid,'dst':'Q','kind':'join','source_tag':'exit '+sid}])
    edges.append({'src':'P','dst':'Q','kind':'intra','source_tag':'unaffected'})
    s={'base_name':'lock_l2','source_file':str(SOURCES/(case+'.c')),'segments':rows,
       'thread_entry_functions':sorted({r['function'] for r in regions}),'node_locations':locations,
       'node_to_segment':mapping,'mappings':{'unrelated':{'keep':'P'}}}
    return source,s,{'nodes':[r['seg_id'] for r in rows],'edges':edges},by_mutex


def build(case='unequal', edges=None, source=None):
    text,s,d,groups=fixture(case,source)
    if edges is not None:d['edges']=edges
    return build_lock_views(base_name='lock_l2',source_text=text,base_segments_json=s,base_dag_json=d)


def cross_edges(groups, cycle=False):
    a,b=groups['mutex_a'],groups['mutex_b']
    if cycle:return [{'src':a[0],'dst':b[0],'kind':'intra'},{'src':b[1],'dst':a[1],'kind':'sem_dep'}]
    return [{'src':a[0],'dst':b[0],'kind':'intra','callsite':'first'},
            {'src':a[1],'dst':b[1],'kind':'intra','callsite':'second'},
            {'src':a[2],'dst':b[0],'kind':'sem_dep','callsite':'third'}]


class LockL2Tests(unittest.TestCase):
    def test_one_mutex_three_complete_regions(self):
        st,sd,ss,dd,meta=build('single_three')
        self.assertTrue(meta['scheduler_compatible']);self.assertEqual([3],[len(g['member_node_ids']) for g in ss['lock_groups']])
        self.assertEqual(5,len(sd['nodes']));self.assertEqual(3,len(dd['nodes']))

    def test_two_mutex_pairs_and_unequal_members_node_formula(self):
        for case,expected in [('two_pairs',[2,2]),('unequal',[3,2])]:
            st,sd,ss,dd,meta=build(case)
            self.assertEqual(expected,[len(g['member_node_ids']) for g in ss['lock_groups']])
            self.assertTrue(meta['scheduler_compatible'])
            self.assertEqual(len(sd['nodes'])-sum(n-1 for n in expected),len(dd['nodes']))
            self.assertEqual({'LOCKGROUP:mutex_a','LOCKGROUP:mutex_b'},set(g['group_id'] for g in ss['lock_groups']))

    def test_legal_intergroup_edges_dedup_and_all_original_sources(self):
        source,s,d,g=fixture();extra=cross_edges(g);d['edges']+=extra
        st,sd,ss,dd,meta=build_lock_views(base_name='lock_l2',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertTrue(meta['scheduler_compatible'])
        cross=[e for e in dd['edges'] if e['src']=='LOCKGROUP:mutex_a' and e['dst']=='LOCKGROUP:mutex_b']
        self.assertEqual({'intra','sem_dep'},{e['kind'] for e in cross})
        self.assertEqual(3,sum(len(e['provenance']) for e in cross))
        self.assertEqual({'first','second','third'},{o['edge']['callsite'] for e in cross for o in e['provenance']})
        origins=[o for e in dd['edges'] for o in e['provenance']]+dd['removed_internal_edges']
        self.assertEqual(list(range(len(d['edges']))),sorted(o['base_edge_index'] for o in origins))
        topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
        timing=aggregate_lock_timing(ss,timing_for(st))
        for algo in ALGOS:
            self.assertEqual(set(dd['nodes']),set(get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json=timing)['priorities']))

    def test_group_order_and_member_order_have_identical_views_mapping_weights(self):
        source,s,d,_=fixture();expected=build();mutexes,regions=analyze_lock_l2(source)
        timing=timing_for(expected[0]);weights=aggregate_lock_timing(expected[2],timing)
        for order in itertools.permutations(mutexes):
            for member_order in [regions,list(reversed(regions))]:
                with patch('Code2DAG.pipeline.lock_model.analyze_lock_l2',return_value=(list(order),copy.deepcopy(member_order))):
                    actual=build_lock_views(base_name='lock_l2',source_text=source,base_segments_json=s,base_dag_json=d)
                self.assertEqual(expected,actual)
                self.assertEqual(weights,aggregate_lock_timing(actual[2],timing))
        g=list(reversed(expected[2]['lock_groups']))
        for group in g:
            for key in ('member_node_ids','member_segment_ids','members'):group[key].reverse()
        self.assertEqual(contract_lock_groups(d,expected[2]['lock_groups']),contract_lock_groups(d,g))

    def test_mutex_declaration_dictionary_order_and_renaming_do_not_choose_schedule_order(self):
        source,s,d,g=fixture();expected=build()
        declarations=['static pthread_mutex_t mutex_a = PTHREAD_MUTEX_INITIALIZER;',
                      'static pthread_mutex_t mutex_b = PTHREAD_MUTEX_INITIALIZER;']
        swapped=source.replace('\n'.join(declarations),'\n'.join(reversed(declarations)))
        self.assertNotEqual(source,swapped)
        actual=build(source=swapped)
        for k in ('nodes','edges'):self.assertEqual(expected[3][k],actual[3][k])
        self.assertEqual(expected[2]['mappings'],actual[2]['mappings'])
        self.assertEqual(aggregate_lock_timing(expected[2],timing_for(expected[0]))['weights'],aggregate_lock_timing(actual[2],timing_for(actual[0]))['weights'])
        renamed=source.replace('mutex_a','zzz').replace('mutex_b','aaa')
        changed=build(source=renamed)
        rename=lambda x:x.replace('LOCKGROUP:zzz','LOCKGROUP:mutex_a').replace('LOCKGROUP:aaa','LOCKGROUP:mutex_b')
        self.assertEqual(set(expected[3]['nodes']),{rename(n) for n in changed[3]['nodes']})
        self.assertEqual({(e['src'],e['dst'],e['kind']) for e in expected[3]['edges']},
                         {(rename(e['src']),rename(e['dst']),e['kind']) for e in changed[3]['edges']})
        self.assertEqual(expected[2]['mappings']['original_node_to_grouped_node'],
                         {n:rename(v) for n,v in changed[2]['mappings']['original_node_to_grouped_node'].items()})

    def test_each_group_weight_is_independent_fieldwise_sum(self):
        st,sd,ss,dd,meta=build();t=timing_for(st);result=aggregate_lock_timing(ss,t)
        for group in ss['lock_groups']:
            for field in TIMING_FIELDS:
                self.assertEqual(sum(t['weights'][sid][field] for sid in group['member_segment_ids']),result['weights'][group['group_id']][field])
        other=copy.deepcopy(t);other['weights'][ss['lock_groups'][1]['member_segment_ids'][0]]={f:v*3 for f,v in other['weights'][ss['lock_groups'][1]['member_segment_ids'][0]].items()}
        changed=aggregate_lock_timing(ss,other)
        self.assertEqual(result['weights']['LOCKGROUP:mutex_a'],changed['weights']['LOCKGROUP:mutex_a'])
        self.assertNotEqual(result['weights']['LOCKGROUP:mutex_b'],changed['weights']['LOCKGROUP:mutex_b'])

    def test_mapping_and_nonmember_rows_edges_metadata_unchanged(self):
        source,s,d,g=fixture();st,sd,ss,dd,meta=build();m=ss['mappings']
        for mutex,ids in g.items():
            gid='LOCKGROUP:'+mutex
            self.assertEqual(set(ids),set(m['grouped_node_to_member_segments'][gid]))
            self.assertEqual(mutex,m['grouped_node_to_mutex_and_source'][gid]['mutex_id'])
            for sid in ids:self.assertEqual(gid,m['original_segment_to_grouped_node'][sid])
        self.assertEqual(s['node_to_segment'],m['original_node_to_execution_segment'])
        self.assertEqual({'keep':'P'},m['unrelated'])
        for node,old in s['node_to_segment'].items():self.assertEqual(m['original_segment_to_grouped_node'][old],m['original_node_to_grouped_node'][node])
        for sid in ('P','Q'):
            self.assertEqual(sid,m['original_segment_to_grouped_node'][sid])
            self.assertEqual(next(r for r in s['segments'] if r['seg_id']==sid),next(r for r in ss['segments'] if r['seg_id']==sid))
        edge=next(e for e in dd['edges'] if e['src']=='P' and e['dst']=='Q')
        self.assertEqual(d['edges'][-1],{k:v for k,v in edge.items() if k!='provenance'})

    def test_duplicate_member_cross_group_or_within_group_is_rejected(self):
        st,sd,ss,dd,_=build();groups=copy.deepcopy(ss['lock_groups']);member=groups[0]['member_segment_ids'][0]
        groups[1]['member_node_ids'].append(member);groups[1]['member_segment_ids'].append(member)
        groups[1]['members'].append(dict(groups[0]['members'][0],mutex_id='mutex_b'))
        with self.assertRaisesRegex(LockModelError,'multiple mutex groups'):contract_lock_groups(sd,groups)
        bad=copy.deepcopy(ss);bad['lock_groups']=groups
        with self.assertRaisesRegex(LockModelError,'multiple mutex groups'):aggregate_lock_timing(bad,timing_for(st))
        bad=copy.deepcopy(ss);bad['lock_groups'][0]['member_segment_ids'].append(member)
        with self.assertRaisesRegex(LockModelError,'duplicate member'):aggregate_lock_timing(bad,timing_for(st))

    def test_cross_group_cycle_keeps_dependencies_and_rejects_all_scheduling(self):
        source,s,d,g=fixture();d['edges']=cross_edges(g,cycle=True)
        st,sd,ss,dd,meta=build_lock_views(base_name='lock_l2',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertFalse(meta['scheduler_compatible']);self.assertEqual(2,len(dd['edges']))
        self.assertEqual({('LOCKGROUP:mutex_a','LOCKGROUP:mutex_b'),('LOCKGROUP:mutex_b','LOCKGROUP:mutex_a')}, {(e['src'],e['dst']) for e in dd['edges']})
        self.assertEqual(2,sum(len(e['provenance']) for e in dd['edges']))
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);p=base/'intermediate_results/lock_l2/pipeline';b=p/'blocks/level2/lock_grouped';t=p/'timing/level2/lock_grouped';b.mkdir(parents=True);t.mkdir(parents=True)
            (b/'segments.json').write_text(json.dumps(ss));(b/'dag_seg.json').write_text(json.dumps(dd));(t/'timing.json').write_text(json.dumps(timing_for(st)))
            for algo in ALGOS:
                with self.assertRaisesRegex(StageError,'not scheduler-compatible'):
                    run_schedule(base_dir=base,base_name='lock_l2',level='level2',rule_name='lock_grouped',algo_name=algo)
            self.assertFalse(list((p/'schedule').rglob('schedule.json')))
        with self.assertRaises(LockModelError):aggregate_lock_timing(ss,timing_for(st))

    def test_any_group_member_missing_any_timing_field_fails(self):
        st,sd,ss,dd,_=build()
        for group in ss['lock_groups']:
            for member in group['member_segment_ids']:
                for field in ('node',)+TIMING_FIELDS:
                    t=timing_for(st)
                    if field=='node':del t['weights'][member]
                    else:del t['weights'][member][field]
                    with self.subTest(group=group['group_id'],member=member,field=field),self.assertRaises(LockModelError):aggregate_lock_timing(ss,t)

    def test_same_group_internal_edges_removed_cross_group_edges_retained(self):
        source,s,d,g=fixture();d['edges']=[{'src':g['mutex_a'][0],'dst':g['mutex_a'][1],'kind':'intra'}, {'src':g['mutex_a'][1],'dst':g['mutex_b'][0],'kind':'intra'}]
        st,sd,ss,dd,_=build_lock_views(base_name='lock_l2',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertEqual(1,len(dd['removed_internal_edges']));self.assertEqual(1,len(dd['edges']))
        self.assertEqual(('LOCKGROUP:mutex_a','LOCKGROUP:mutex_b'),(dd['edges'][0]['src'],dd['edges'][0]['dst']))

    def test_nested_crossing_conditional_loop_and_alias_still_rejected(self):
        source=(SOURCES/'two_pairs.c').read_text()
        variants=[source.replace('    v0 += 10;','    pthread_mutex_lock(&mutex_b);',1),
                  source.replace('pthread_mutex_unlock(&mutex_a)','pthread_mutex_unlock(&mutex_b)',1),
                  source.replace('    v0 = 1;','    if (v0) { v0 = 1; }',1),
                  source.replace('    v0 = 1;','    while (v0) { v0 = 1; }',1),
                  source.replace('pthread_mutex_lock(&mutex_a)','pthread_mutex_lock(alias)',1)]
        for v in variants:
            with self.assertRaises(LockModelError):analyze_lock_l2(v)

    def test_five_algorithms_for_each_legal_model(self):
        for case in ('single_three','two_pairs','unequal'):
            st,sd,ss,dd,meta=build(case);timing=aggregate_lock_timing(ss,timing_for(st))
            for algo in ALGOS:
                result=get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json=timing)
                self.assertEqual(set(dd['nodes']),set(result['priorities']))
        self.assertEqual('effective_line_merge',default_rule_for_level('level2'))

    @unittest.skipUnless(shutil.which('gcc-13'),'native GCC13 required')
    def test_native_all_three_cases_schedule_only_source_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            result=native_acceptance(Path(tmp)/'acceptance')
            self.assertTrue(result['passed']);self.assertEqual(3,len(result['cases']))


def native_acceptance(output):
    output=Path(output);output.mkdir(parents=True,exist_ok=False);summaries={}
    for case in ('single_three','two_pairs','unequal'):
        folder=output/case;folder.mkdir();logs=folder/'logs';logs.mkdir();expand=folder/'expand';expand.mkdir();base=folder/'native';source=SOURCES/(case+'.c');before=hashlib.sha256(source.read_bytes()).hexdigest();commands=[]
        def command(label,args):
            p=subprocess.run([str(x) for x in args],cwd=ROOT,capture_output=True,text=True,timeout=90)
            (logs/(label+'.stdout')).write_text(p.stdout);(logs/(label+'.stderr')).write_text(p.stderr)
            commands.append({'label':label,'argv':[str(x) for x in args],'cwd':str(ROOT),'returncode':p.returncode});(folder/'commands.json').write_text(json.dumps(commands,indent=2))
            if p.returncode:raise AssertionError((case,label,p.stderr))
        command('expand',['gcc-13','-O0','-g','-fdump-rtl-expand','-c',source,'-o',expand/(case+'.o')]);dump=next(expand.glob('*.expand'))
        for mode in ('threads','full'):
            command('generation_'+mode,[sys.executable,'-B','-m','Code2DAG.generation.legacy',dump,'--source-file',source,'--output-base',base,'--force',*(['--threads-only'] if mode=='threads' else [])])
        result=base/'intermediate_results'/case;shutil.copy2(result/'config_files'/(case+'.dot'),result/'dag_generation/dag.dot')
        command('collect',[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'collect','--base-name',case,'--source',source])
        command('blocks',[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'blocks','--base-name',case,'--source',source,'--level','level2','--rule','lock_grouped'])
        pipeline=result/'pipeline';block=pipeline/'blocks/level2/lock_grouped'
        st,sd,ss,dd=[json.loads((block/n).read_text()) for n in ('segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json')]
        assert ss['capabilities']['scheduler_compatible'] and not ss['capabilities']['instrumentation_compatible']
        count=[len(g['member_segment_ids']) for g in ss['lock_groups']];assert count=={'single_three':[3],'two_pairs':[2,2],'unequal':[3,2]}[case]
        assert len(dd['nodes'])==len(sd['nodes'])-sum(n-1 for n in count)
        timing=timing_for(st);(folder/'member_timing_input.json').write_text(json.dumps(timing,indent=2))
        command('import_timing',[sys.executable,'-B','-m','Code2DAG.pipeline.lock_timing','--base-dir',base,'--base-name',case,'--input-timing',folder/'member_timing_input.json'])
        grouped=json.loads((pipeline/'timing/level2/lock_grouped/timing.json').read_text());algorithms={}
        for algo in ALGOS:
            command('schedule_'+algo,[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'schedule','--base-name',case,'--level','level2','--rule','lock_grouped','--algo',algo])
            algorithms[algo]=json.loads((pipeline/f'schedule/level2/lock_grouped/{algo}/schedule.json').read_text());assert set(algorithms[algo]['priorities'])==set(dd['nodes'])
        assert not list((pipeline/'timing').rglob('*.c')) and not (pipeline/'instrument').exists()
        after=hashlib.sha256(source.read_bytes()).hexdigest();assert before==after
        summaries[case]={'source_sha256_before':before,'source_sha256_after':after,'nodes_before':len(sd['nodes']),'nodes_after':len(dd['nodes']),'edges_before':len(sd['edges']),'edges_after':len(dd['edges']),
                         'groups':ss['lock_groups'],'group_weights':{g['group_id']:grouped['weights'][g['group_id']] for g in ss['lock_groups']},'algorithms':algorithms,'command_count':len(commands)}
    summary={'passed':True,'cases':summaries,'timing_input':'deterministic synthetic existing-schema weights, not measurements or WCET','runtime_C_executed':False,'instrumented_C_generated':False}
    (output/'acceptance.json').write_text(json.dumps(summary,indent=2));return summary


if __name__=='__main__':unittest.main()
