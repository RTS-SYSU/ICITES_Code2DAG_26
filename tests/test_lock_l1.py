"""Bounded Lock L1 graph-only tests; no runtime C instrumentation or execution."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from Code2DAG.pipeline.lock_model import analyze_lock_l1, build_lock_views, aggregate_lock_timing, LockModelError
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.runner import run_collector, run_blocks, default_rule_for_level
from Code2DAG.pipeline.rules_registry import get_rule
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.timing import run_timing
from Code2DAG.pipeline.instrument import run_instrument
from Code2DAG.pipeline.instrument_levelx import instrument_prio_all_segments_by_start_line
from Code2DAG.pipeline.lock_timing import import_lock_timing
from Code2DAG.pipeline.errors import StageError

ROOT=Path(__file__).resolve().parents[2]
SOURCE=ROOT/'Code2DAG/source_files/lock_l1/lock_l1.c'
ALGOS=('FIFO','LPF','heft','t_level','zhao2020')
GROUP='LOCKGROUP:shared_mutex'


def fixture():
    source=SOURCE.read_text();mutex,regions=analyze_lock_l1(source)
    rows=[];locations={};mapping={}
    for i,r in enumerate(regions):
        fn=r['function'];a,b=r['segment_range']
        for sid,kind,start,end in [(fn+':before','compute',a-2,a-1),(fn+':critical','mutex_cs',a,b),(fn+':after','compute',b+1,b+1)]:
            rows.append(dict(seg_id=sid,function=fn,kind=kind,start_line=start,end_line=end))
        for api,key in [('pthread_mutex_lock','lock_range'),('pthread_mutex_unlock','unlock_range')]:
            n=fn+'/'+api+'1';locations[n]={'function':fn,'line':r[key][0]};mapping[n]=fn+':critical'
    edges=[]
    for r in regions:
        fn=r['function'];edges.extend([{'src':fn+':before','dst':fn+':critical','kind':'intra'}, {'src':fn+':critical','dst':fn+':after','kind':'intra'}])
    # Shared external endpoints create duplicate projected edges of each kind.
    for kind in ('create','join','sem_dep','intra'):
        edges.extend([{'src':'thread_a:before','dst':r['function']+':critical','kind':kind,'source':kind+' input'} for r in regions])
        edges.extend([{'src':r['function']+':critical','dst':'thread_b:after','kind':kind,'source':kind+' output'} for r in regions])
    segments={'base_name':'lock_l1','source_file':str(SOURCE),'segments':rows,'thread_entry_functions':[r['function'] for r in regions],
              'node_locations':locations,'node_to_segment':mapping,'mappings':{'unchanged_metadata':{'before':'thread_a:before'}}}
    return source,segments,{'nodes':[r['seg_id'] for r in rows],'edges':edges}


def model():
    source,s,d=fixture()
    return build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)


def timing_for(segments):
    return {'base_name':segments['base_name'],'repeats':2,'weights':{r['seg_id']:{'total_ns':20*(i+1),'count':2,'avg_ns':10*(i+1),'min_ns':8*(i+1),'max_ns':12*(i+1)} for i,r in enumerate(segments['segments'])}}


class LockL1Tests(unittest.TestCase):
    def test_normal_contraction_and_exact_two_members(self):
        st,sd,ss,dd,meta=model()
        self.assertTrue(meta['scheduler_compatible'])
        self.assertEqual({'thread_a:critical','thread_b:critical'},set(ss['lock_groups'][0]['member_node_ids']))
        self.assertEqual(6,len(sd['nodes']));self.assertEqual(5,len(dd['nodes']))
        self.assertIn('thread_a:critical',sd['nodes']);self.assertNotIn('thread_a:critical',dd['nodes'])
        self.assertEqual('shared_mutex',ss['lock_groups'][0]['mutex_id'])
        self.assertTrue(all('lock_group' in r for r in st['segments'] if r['kind']=='mutex_cs'))

    def test_external_edge_union_dedup_types_and_provenance(self):
        _,_,_,dd,_=model();_,_,base=fixture();members={'thread_a:critical','thread_b:critical'}
        expected={(GROUP if e['src'] in members else e['src'],GROUP if e['dst'] in members else e['dst'],e['kind']) for e in base['edges']}
        actual={(e['src'],e['dst'],e['kind']) for e in dd['edges']}
        self.assertEqual(expected,actual);self.assertEqual(len(actual),len(dd['edges']))
        all_origins=[o for e in dd['edges'] for o in e['provenance']]
        self.assertEqual(list(range(len(base['edges']))),sorted(o['base_edge_index'] for o in all_origins))
        self.assertTrue(any(len(e['provenance'])>1 for e in dd['edges']))
        for o in all_origins:self.assertEqual(base['edges'][o['base_edge_index']],o['edge'])
        self.assertEqual({'create','join','sem_dep','intra'},{e['kind'] for e in dd['edges']})

    def test_internal_member_edge_removed_without_choosing_order(self):
        source,s,d=fixture();d['edges']=[{'src':'thread_a:critical','dst':'thread_b:critical','kind':'intra','source':'test only'}]
        _,_,_,dd,_=build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertEqual([],dd['edges']);self.assertEqual(1,len(dd['removed_internal_edges']))
        self.assertEqual(d['edges'][0],dd['removed_internal_edges'][0]['edge'])

    def test_complete_mapping_and_unchanged_nonmembers(self):
        st,_,ss,_,_=model();_,base,_=fixture();m=ss['mappings']
        self.assertEqual(base['node_to_segment'],m['original_node_to_execution_segment'])
        self.assertEqual(base['mappings']['unchanged_metadata'],m['unchanged_metadata'])
        for n in base['node_to_segment']:self.assertEqual(GROUP,m['original_node_to_grouped_node'][n])
        for row in base['segments']:
            sid=row['seg_id'];self.assertEqual(GROUP if ':critical' in sid else sid,m['original_segment_to_grouped_node'][sid])
        self.assertEqual(2,len(m['grouped_node_to_member_segments'][GROUP]))
        self.assertEqual('shared_mutex',m['grouped_node_to_mutex_and_source'][GROUP]['mutex_id'])
        self.assertEqual(base['node_to_segment'],st['node_to_segment'])

    def test_all_five_timing_fields_sum_and_other_weights_unchanged(self):
        st,_,ss,_,_=model();timing=timing_for(st);before=copy.deepcopy(timing);out=aggregate_lock_timing(ss,timing)
        members=ss['lock_groups'][0]['member_segment_ids']
        for field in ('total_ns','count','avg_ns','min_ns','max_ns'):
            self.assertEqual(sum(timing['weights'][m][field] for m in members),out['weights'][GROUP][field])
        for sid,w in timing['weights'].items():
            if sid not in members:self.assertEqual(w,out['weights'][sid])
        self.assertEqual(before,timing)

    def test_missing_member_weights_fields_samples_or_unknown_metrics_rejected(self):
        st,_,ss,_,_=model();timing=timing_for(st);member=ss['lock_groups'][0]['member_segment_ids'][0]
        for fault in ('node','avg_ns','count','min_ns','max_ns','total_ns','zero','unknown','negative'):
            t=copy.deepcopy(timing)
            if fault=='node':del t['weights'][member]
            elif fault=='zero':t['weights'][member]['count']=0
            elif fault=='unknown':t['weights'][member]['guess']=1
            elif fault=='negative':t['weights'][member]['total_ns']=-1
            else:del t['weights'][member][fault]
            with self.subTest(fault=fault),self.assertRaises(LockModelError):aggregate_lock_timing(ss,t)

    def test_nonmember_blocker_zero_weight_is_preserved(self):
        st,_,ss,_,_=model();t=timing_for(st);t['weights']['thread_a:before']={f:0 for f in t['weights']['thread_a:before']}
        out=aggregate_lock_timing(ss,t);self.assertEqual(t['weights']['thread_a:before'],out['weights']['thread_a:before'])

    def test_group_dag_topology_and_five_algorithms(self):
        st,_,ss,dd,_=model();timing=aggregate_lock_timing(ss,timing_for(st))
        topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
        for algo in ALGOS:
            r=get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json=timing)
            self.assertEqual(set(dd['nodes']),set(r['priorities']));self.assertIn(GROUP,r['priorities'])

    def test_unpaired_and_nested_locks_rejected(self):
        source=SOURCE.read_text()
        for variant in [source.replace('    pthread_mutex_unlock(&shared_mutex);','',1),
                        source.replace('    a += 10;','    pthread_mutex_lock(&shared_mutex);\n    a += 10;',1),
                        source.replace('    pthread_mutex_lock(&shared_mutex);','    pthread_mutex_unlock(&shared_mutex);',1)]:
            with self.assertRaises(LockModelError):analyze_lock_l1(variant)

    def test_alias_trylock_wrappers_and_other_mutexes_rejected(self):
        source=SOURCE.read_text()
        variants=[source.replace('pthread_mutex_lock(&shared_mutex)','pthread_mutex_trylock(&shared_mutex)',1),
                  source.replace('pthread_mutex_lock(&shared_mutex)','pthread_mutex_lock(alias)',1),
                  source.replace('static int a, b;','static int a, b;\nstatic pthread_mutex_t other = PTHREAD_MUTEX_INITIALIZER;'),
                  source.replace('    a = 1;','    void *alias = &shared_mutex;\n    a = 1;'),
                  source.replace('pthread_mutex_lock(&shared_mutex)','wrapped_lock(&shared_mutex)',1)]
        for v in variants:
            with self.subTest(source=v),self.assertRaises(LockModelError):analyze_lock_l1(v)

    def test_control_flow_macros_semaphores_extra_threads_rejected(self):
        source=SOURCE.read_text()
        variants=[source.replace('    a = 1;','    if (a) { a = 1; }'),
                  source.replace('    a = 1;','    while (a) { a = 1; }'),
                  source.replace('    a = 1;','    return NULL;'),
                  source.replace('    a = 1;','    sem_wait(&s);'),
                  source.replace('#include <pthread.h>','#include <pthread.h>\n#define X 1'),
                  source.replace('    pthread_join(ta, NULL);','    pthread_create(&tc, NULL, thread_a, NULL);\n    pthread_join(ta, NULL);')]
        for v in variants:
            with self.subTest(source=v),self.assertRaises(LockModelError):analyze_lock_l1(v)

    def test_split_or_oversized_critical_segment_rejected(self):
        source,s,d=fixture();target='thread_a:critical'
        s['segments'][1]['end_line']-=1
        with self.assertRaises(LockModelError):build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)
        source,s,d=fixture();s['segments'][1]['start_line']-=1
        with self.assertRaises(LockModelError):build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)
        source,s,d=fixture();s['segments'].append(dict(s['segments'][1],seg_id='split'));d['nodes'].append('split')
        with self.assertRaises(LockModelError):build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)

    def test_contraction_cycle_retained_incompatible_and_schedule_rejected(self):
        source,s,d=fixture();d['edges']=[{'src':'thread_a:critical','dst':'thread_a:after','kind':'intra'}, {'src':'thread_a:after','dst':'thread_b:critical','kind':'sem_dep'}]
        st,sd,ss,dd,meta=build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertFalse(meta['scheduler_compatible']);self.assertEqual(2,len(dd['edges']))
        self.assertEqual({(GROUP,'thread_a:after'),('thread_a:after',GROUP)},{(e['src'],e['dst']) for e in dd['edges']})
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);p=base/'intermediate_results/lock_l1/pipeline';(p/'blocks/level2/lock_grouped').mkdir(parents=True);(p/'timing/level2/lock_grouped').mkdir(parents=True)
            (p/'blocks/level2/lock_grouped/segments.json').write_text(json.dumps(ss));(p/'blocks/level2/lock_grouped/dag_seg.json').write_text(json.dumps(dd));(p/'timing/level2/lock_grouped/timing.json').write_text(json.dumps(timing_for(st)))
            for algo in ALGOS:
                with self.assertRaisesRegex(StageError,'not scheduler-compatible'):run_schedule(base_dir=base,base_name='lock_l1',level='level2',rule_name='lock_grouped',algo_name=algo)

    def test_optional_rule_default_unchanged_and_input_immutable(self):
        self.assertEqual('effective_line_merge',default_rule_for_level('level2'))
        self.assertEqual('conditional_branches',get_rule('level2','conditional_branches').rule_id())
        self.assertEqual('lock_grouped',get_rule('level2','lock_grouped').rule_id())
        source,s,d=fixture();before=copy.deepcopy((s,d));build_lock_views(base_name='lock_l1',source_text=source,base_segments_json=s,base_dag_json=d)
        self.assertEqual(before,(s,d))

    def test_direct_instrumentation_refuses_without_writing_c(self):
        st,sd,ss,dd,meta=model()
        before=hashlib.sha256(SOURCE.read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp);segments=path/'segments.json';segments.write_text(json.dumps(ss))
            with self.assertRaisesRegex(ValueError,'analysis-only'):
                instrument_prio_all_segments_by_start_line(SOURCE,segments_json=segments,priorities={GROUP:99},out_c=path/'must_not_exist.c')
            self.assertFalse((path/'must_not_exist.c').exists())
        self.assertEqual(before,hashlib.sha256(SOURCE.read_bytes()).hexdigest())

    def test_crlf_source_and_comment_decoys_keep_ranges(self):
        source=SOURCE.read_text();expected=analyze_lock_l1(source)
        self.assertEqual(expected,analyze_lock_l1(source.replace('\n','\r\n')))
        source=source.replace('    a += 10;','    a += 10; /* pthread_mutex_lock(&other); if (x) */')
        self.assertEqual(expected,analyze_lock_l1(source))

    @unittest.skipUnless(shutil.which('gcc-13'),'native GCC13 required')
    def test_native_generation_blocks_import_timing_schedule_only_source_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=native_acceptance(Path(tmp)/'acceptance')
            self.assertTrue(r['passed']);self.assertEqual(r['source_before_sha256'],r['source_after_sha256'])


def native_acceptance(output):
    output=Path(output);output.mkdir(parents=True,exist_ok=False);base=output/'native';expand=output/'expand';expand.mkdir();logs=output/'logs';logs.mkdir()
    before=hashlib.sha256(SOURCE.read_bytes()).hexdigest();commands=[]
    def command(label,args):
        p=subprocess.run([str(a) for a in args],cwd=ROOT,capture_output=True,text=True,timeout=90)
        (logs/(label+'.stdout')).write_text(p.stdout);(logs/(label+'.stderr')).write_text(p.stderr)
        commands.append({'label':label,'argv':[str(a) for a in args],'cwd':str(ROOT),'returncode':p.returncode})
        (output/'commands.json').write_text(json.dumps(commands,indent=2))
        if p.returncode:raise AssertionError((label,p.stderr))
    command('expand',['gcc-13','-O0','-g','-fdump-rtl-expand','-c',SOURCE,'-o',expand/'lock_l1.o'])
    dump=next(expand.glob('*.expand'))
    for mode in ('threads','full'):
        command('generation_'+mode,[sys.executable,'-B','-m','Code2DAG.generation.legacy',dump,'--source-file',SOURCE,'--output-base',base,'--force',*(['--threads-only'] if mode=='threads' else [])])
    root=base/'intermediate_results/lock_l1';shutil.copy2(root/'config_files/lock_l1.dot',root/'dag_generation/dag.dot')
    command('collect',[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'collect','--base-name','lock_l1','--source',SOURCE])
    for rule in ('effective_line_merge','lock_grouped'):
        command('blocks_'+rule,[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'blocks','--base-name','lock_l1','--source',SOURCE,'--level','level2','--rule',rule])
    pipeline=root/'pipeline';blocks=pipeline/'blocks/level2/lock_grouped'
    st,sd,ss,dd=[json.loads((blocks/f).read_text()) for f in ('segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json')]
    assert ss['capabilities']['scheduler_compatible'] and len(ss['lock_groups'][0]['member_node_ids'])==2
    assert len(dd['nodes'])==len(sd['nodes'])-1
    # Deterministic existing-schema input, explicitly synthetic; no timed program is generated/run.
    timing=timing_for(st);(output/'member_timing_input.json').write_text(json.dumps(timing,indent=2))
    command('import_timing',[sys.executable,'-B','-m','Code2DAG.pipeline.lock_timing','--base-dir',base,'--base-name','lock_l1','--input-timing',output/'member_timing_input.json'])
    grouped=json.loads((pipeline/'timing/level2/lock_grouped/timing.json').read_text())
    algorithms={}
    for algo in ALGOS:
        command('schedule_'+algo,[sys.executable,'-B','-m','Code2DAG.pipeline.cli','--base-dir',base,'schedule','--base-name','lock_l1','--level','level2','--rule','lock_grouped','--algo',algo])
        algorithms[algo]=json.loads((pipeline/f'schedule/level2/lock_grouped/{algo}/schedule.json').read_text())
        assert set(algorithms[algo]['priorities'])==set(dd['nodes'])
    # Explicit guard checks: no timing project or instrumented C may be emitted.
    rejected={}
    for label,action in [('timing',lambda:run_timing(base_dir=base,base_name='lock_l1',level='level2',rule_name='lock_grouped')),
                         ('instrument',lambda:run_instrument(base_dir=base,base_name='lock_l1',level='level2',rule_name='lock_grouped',algo_name='FIFO',instrument_mode='generic'))]:
        try:action()
        except StageError as e:rejected[label]=str(e)
        else:raise AssertionError('analysis-only stage accepted')
    # run_timing cleans its own output directory before rejecting; re-import analysis weights.
    import_lock_timing(base_dir=base,base_name='lock_l1',input_timing=output/'member_timing_input.json')
    assert not list((pipeline/'instrument').rglob('*.c')) and not list((pipeline/'timing').rglob('*.c'))
    after=hashlib.sha256(SOURCE.read_bytes()).hexdigest();assert before==after
    summary={'passed':True,'source_before_sha256':before,'source_after_sha256':after,'node_count_before':len(sd['nodes']),'node_count_after':len(dd['nodes']),
             'edge_count_before':len(sd['edges']),'edge_count_after':len(dd['edges']),'group':ss['lock_groups'][0],'mappings':ss['mappings'],
             'group_weight':grouped['weights'][GROUP],'algorithms':algorithms,'rejected_execution_stages':rejected,'timing_input':'deterministic synthetic existing-schema weights; not a measurement or WCET',
             'commands':len(commands),'runtime_C_executed':False,'priority_instrumentation_generated':False}
    (output/'acceptance.json').write_text(json.dumps(summary,indent=2));return summary


if __name__=='__main__':unittest.main()
