"""Branch-local mutex composition; real sources and independent path expectations."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition_c2a_model import build_branch_lock_views, RULE
from Code2DAG.pipeline.composition_model import CompositionError, CONTROL
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.composition_c2a_timing import import_branch_lock_timing
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2]
SOURCES=ROOT/'Code2DAG/source_files/composition_c2a'
CASES=['then','else','different','same','two_groups','no_else','else_if','with_loop']


def fixture(text,source):
    lines=text.splitlines();worker=next(i for i,l in enumerate(lines,1) if l.startswith('void *worker'))
    end=next(i for i,l in enumerate(lines[worker:],worker+1) if l=='}')
    main=next(i for i,l in enumerate(lines,1) if l.startswith('int main'))
    rows=[];locations={};mapping={};cursor=worker+1;count=0
    for i in range(worker+1,end):
        if 'pthread_mutex_lock(' not in lines[i-1]:continue
        j=next(j for j in range(i+1,end) if 'pthread_mutex_unlock(' in lines[j-1])
        if cursor<i:rows.append(dict(seg_id='N'+str(len(rows)),function='worker',kind='compute',start_line=cursor,end_line=i-1))
        sid='CS'+str(count);count+=1;rows.append(dict(seg_id=sid,function='worker',kind='mutex_cs',start_line=i,end_line=j));cursor=j+1
        for api,line in [('lock',i),('unlock',j)]:
            node='worker/pthread_mutex_'+api+str(count);locations[node]=dict(function='worker',line=line);mapping[node]=sid
    if cursor<end:rows.append(dict(seg_id='N'+str(len(rows)),function='worker',kind='compute',start_line=cursor,end_line=end-1))
    mainrow=dict(seg_id='MAIN',function='main',kind='compute',start_line=main+1,end_line=len(lines)-1);rows.append(mainrow)
    for api in ['create','join']:
        line=next(i for i,l in enumerate(lines,1) if 'pthread_'+api+'(' in l);node='main/pthread_'+api+'1';locations[node]=dict(function='main',line=line);mapping[node]='MAIN'
    edges=[dict(src=a['seg_id'],dst=b['seg_id'],kind='intra',custom={'index':i}) for i,(a,b) in enumerate(zip(rows[:-1],rows[1:-1]))]
    # Distinct create/join carriers avoid a fixture cycle, as in real generation.
    create=next(l['line'] for n,l in locations.items() if '/pthread_create' in n)
    join=next(l['line'] for n,l in locations.items() if '/pthread_join' in n)
    rows[-1]['end_line']=create;rows.append(dict(seg_id='JOIN',function='main',kind='compute',start_line=join,end_line=len(lines)-1));mapping['main/pthread_join1']='JOIN'
    edges += [dict(src='MAIN',dst=rows[0]['seg_id'],kind='create',custom={'create':True}),dict(src=rows[-3]['seg_id'],dst='JOIN',kind='join',custom={'join':True}),dict(src='MAIN',dst='JOIN',kind='intra')]
    return dict(base_name='fixture',source_file=str(source),segments=rows,node_locations=locations,node_to_segment=mapping,thread_entry_functions=['worker'],mappings={}),dict(nodes=[r['seg_id'] for r in rows],edges=edges)


class CompositionC2aTests(unittest.TestCase):
    def build(self,name,text=None):
        source=SOURCES/(name+'.c');text=source.read_text() if text is None else text;raw,dag=fixture(text,source);frozen=copy.deepcopy((raw,dag))
        views=build_branch_lock_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(frozen,(raw,dag));return text,raw,dag,views

    def contract(self,text,raw,dag,v):
        st,sd,s,d,_=v
        for graph in [sd,d]:topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
        self.assertFalse(any(r['kind'] in CONTROL for r in s['segments']))
        self.assertEqual(len(dag['edges']),len(s['composition_edge_provenance']))
        for g in s['lock_groups']:
            members=g['members'];self.assertTrue(members)
            self.assertTrue(g['condition_memberships'])
            self.assertTrue(all(m['execution_segment']['condition_memberships']==g['condition_memberships'] for m in members))
            for m in members:
                sid=m['member_segment_id'];old=m['execution_segment']['source_segments']
                self.assertIn(g['group_id'],s['mappings']['grouped_node_to_member_segments'])
                for o in old:self.assertIn(g['group_id'],s['mappings']['original_segment_to_execution_segments'][o])
                for n in m['execution_segment']['generation_origin'] or []:self.assertEqual([g['group_id']],s['mappings']['generation_node_to_execution_segments'][n])
                for pos in m['execution_segment']['source_position_ids']:self.assertIn(g['group_id'],s['mappings']['source_position_to_execution_segments'][pos])
        groups=s['lock_groups']
        for a in groups:
            for b in groups:
                if a is b:continue
                ma={m['exclusive_group']:m['branch_id'] for m in a['condition_memberships']};mb={m['exclusive_group']:m['branch_id'] for m in b['condition_memberships']}
                if any(ma[k]!=mb[k] for k in ma.keys()&mb.keys()):
                    self.assertFalse(c1.reachable(d,a['group_id'],b['group_id']));self.assertFalse(c1.reachable(d,b['group_id'],a['group_id']))
        self.assertTrue(sd['exclusive_groups']);self.assertTrue(s['condition_regions'])

    def test_branch_membership_maps_groups_and_no_cross_branch_merge(self):
        for name in CASES:
            with self.subTest(name=name):
                text,raw,dag,v=self.build(name);self.contract(text,raw,dag,v)
                if name=='same':self.assertEqual(2,len(v[2]['lock_groups']));self.assertEqual({'m'},{g['mutex_id'] for g in v[2]['lock_groups']})

    def test_wrong_or_crossing_lock_regions_reject(self):
        text=(SOURCES/'then.c').read_text()
        changes=[text.replace('    if (mode == 0) {','    pthread_mutex_lock(&m);\n    if (mode == 0) {',1).replace('        pthread_mutex_lock(&m);','',1),text.replace('        pthread_mutex_unlock(&m);','',1),text.replace('        pthread_mutex_unlock(&m);','        pthread_mutex_unlock(&unknown);',1),text.replace('        pthread_mutex_unlock(&m);','',1).replace('    result += 1000;','    pthread_mutex_unlock(&m);\n    result += 1000;')]
        for source in changes:
            with self.subTest(source=source),self.assertRaises(ValueError):self.build('then',source)

    def test_native_else_boundary_minimal_reproduction(self):
        # Real GCC emits a raw compute segment containing only `} else {`.
        # Expected: branch-local groups survive; current adapter rejects its ports.
        source=SOURCES/'different.c';before=source.read_bytes()
        h=c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as tmp:
            base,_=h.native(tmp,source)
            run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
        self.assertEqual(before,source.read_bytes())

    def assert_no_cross_branch_paths(self,views):
        rows=views[2]['segments'];dag=views[3]
        for a in rows:
            for b in rows:
                ma={m['exclusive_group']:m['branch_id'] for m in a.get('condition_memberships',[])}
                mb={m['exclusive_group']:m['branch_id'] for m in b.get('condition_memberships',[])}
                if any(ma[k]!=mb[k] for k in ma.keys()&mb.keys()):
                    self.assertFalse(c1.reachable(dag,a['seg_id'],b['seg_id']))
                    self.assertFalse(c1.reachable(dag,b['seg_id'],a['seg_id']))

    def test_directional_boundary_regressions_and_empty_branch(self):
        for name in ['different','then','else','no_else','same']:
            source=(SOURCES/(name+'.c')).read_text()
            variants=[source,source.replace('    } else {','    }\n    else\n    {')]
            if name=='then':variants += [source.replace('        result += 20;',''),source.replace('        result += 20;','        ;')]
            for text in variants:
                with self.subTest(name=name,split=text!=source):
                    text,raw,dag,v=self.build(name,text);self.contract(text,raw,dag,v);self.assert_no_cross_branch_paths(v)
                    for witness in v[2]['composition_edge_provenance']:
                        if witness['disposition']!='condition_boundary_replacement':continue
                        self.assertTrue(witness['replacement_edges'])
                        for edge in witness['replacement_edges']:
                            self.assertIn(edge['kind'],['condition_merge','condition_branch'])
                    # Syntax-only segments retain provenance, but no execution map.
                    clean=__import__('Code2DAG.pipeline.condition_model',fromlist=['_sanitize_c'])._sanitize_c(text).splitlines()
                    import re
                    for rawrow in raw['segments']:
                        part='\n'.join(clean[rawrow['start_line']-1:rawrow['end_line']])
                        if not re.sub(r'\belse\b|[{}\s]','',part):
                            self.assertEqual([],v[2]['mappings']['original_segment_to_execution_segments'][rawrow['seg_id']])

    def test_native_split_boundary_and_ordinary_branch_nodes(self):
        h=c1.CompositionC1Tests();source=SOURCES/'different.c'
        text=source.read_text().replace('    } else {','    }\n    else\n    {')
        # Business statements in both branches must stay mutually exclusive.
        text=text.replace('    if (mode == 0) {','    if (mode == 0) {\n        result += 7;').replace('    else\n    {','    else\n    {\n        result += 9;')
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'split.c';src.write_text(text);before=src.read_bytes();base,p=h.native(tmp,src)
            run_composition_blocks(base_dir=base,base_name='split',source_file=src)
            b=p/'blocks/level2'/RULE
            views=[json.loads((b/n).read_text()) for n in ['segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json']]+[{}]
            self.assert_no_cross_branch_paths(views);self.assertEqual(before,src.read_bytes())

    def native_path(self,name,mode):
        source=SOURCES/(name+'.c');before=source.read_bytes();h=c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as tmp:
            base,p=h.native(tmp,source);run_composition_blocks(base_dir=base,base_name=name,source_file=source)
            b=p/'blocks/level2'/RULE
            views=[json.loads((b/n).read_text()) for n in ['segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json']]+[{}]
            from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
            raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
            self.contract(source.read_text(),raw,dag,views);self.assert_no_cross_branch_paths(views)
            s=views[2];weights={t['template_id']:dict(total_ns=(i+1)*10,count=1,avg_ns=(i+1)*10,min_ns=(i+1)*10,max_ns=(i+1)*10) for i,t in enumerate(s['templates'])}
            inp=Path(tmp)/'timing.json';inp.write_text(json.dumps({'weights':weights}));before_input=inp.read_bytes()
            timing=import_branch_lock_timing(base_dir=base,base_name=name,input_timing=inp)
            for row in s['segments']:
                expected=({field:sum(weights[m['execution_segment']['template_id']][field] for m in row['members']) for field in ['total_ns','count','avg_ns','min_ns','max_ns']} if row['kind']=='lock_group' else weights[row['template_id']])
                self.assertEqual(expected,timing['weights'][row['seg_id']])
            for algo in c1.ALGOS:
                schedule=run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)
                self.assertEqual(set(views[3]['nodes']),set(schedule['priorities']))
            binary=Path(tmp)/'business';h.command(['gcc-13','-std=c11','-pthread','-O0',source,'-o',binary]);execution=h.command([binary,*(['x']*mode)])
            # This C has business CS markers, not timing/priority instrumentation.
            if mode==0:
                tags=[] if name=='else' else ['T1','T2']
                if name=='two_groups':tags+=['U1','U2']
                result=1110 if name=='else' else 1100+len(tags)
            else:
                tags=['E1','E2'] if name in ['else','different','same'] else (['I1','I2'] if name=='else_if' and mode==1 else [])
                result=1100+len(tags) if tags else (1100 if name=='no_else' else 1130 if name=='else_if' else 1120)
            if name=='with_loop':result+=3
            self.assertEqual(['CS:'+t for t in tags]+['RESULT:'+str(result)],execution.stdout.splitlines())
            groups=s['lock_groups'];selected=[g for g in groups if all((m['branch_id']=='then')==(mode==0 if len(g['condition_memberships'])==1 or m['exclusive_group']==s['condition_regions'][0]['condition_id'] else mode==1) for m in g['condition_memberships'])]
            # Map each actually printed CS line to exactly its selected final group.
            events=[]
            lines=source.read_text().splitlines()
            for tag in tags:
                line=next(i for i,l in enumerate(lines,1) if 'CS:'+tag+'\\n' in l)
                mapped=s['mappings']['source_position_to_execution_segments']['LOOPSRC:worker:line:'+str(line)]
                self.assertEqual(1,len(mapped));events.extend(mapped)
            self.assertEqual({g['group_id'] for g in selected},set(events))
            self.assertEqual(before_input,inp.read_bytes())
            published=p/'timing/level2'/RULE/'timing.json';saved=published.read_bytes();sp=b/'segments.json';valid=sp.read_bytes()
            for damage in ['branch','mutex','mapping','metric']:
                data=json.loads(valid);badweights=copy.deepcopy(weights)
                if damage=='branch':data['lock_groups'][0]['condition_memberships'][0]['branch_id']='wrong'
                elif damage=='mutex':data['lock_groups'][0]['mutex_id']='wrong'
                elif damage=='mapping':data['mappings']['generation_node_to_execution_segments']['forged']=['unknown']
                else:next(iter(badweights.values()))['avg_ns']=True
                sp.write_text(json.dumps(data));inp.write_text(json.dumps({'weights':badweights}))
                with self.assertRaises(ValueError):import_branch_lock_timing(base_dir=base,base_name=name,input_timing=inp)
                self.assertEqual(saved,published.read_bytes())
            sp.write_bytes(valid);inp.write_bytes(before_input);self.assertEqual(timing,import_branch_lock_timing(base_dir=base,base_name=name,input_timing=inp))
        self.assertEqual(before,source.read_bytes())

    def test_native_all_branch_paths_timing_and_five_algorithms(self):
        for name in CASES:
            for mode in ([0,1,2] if name=='else_if' else [0,1]):
                with self.subTest(name=name,mode=mode):self.native_path(name,mode)

    def test_cross_branch_pair_and_condition_inside_lock_reject(self):
        from Code2DAG.pipeline.composition import select_rule
        text=(SOURCES/'same.c').read_text()
        # Move one then unlock to the beginning of the else branch.
        crossed=text.replace('        pthread_mutex_unlock(&m);','',1).replace('    } else {','    } else {\n        pthread_mutex_unlock(&m);',1)
        # Surround a complete Condition with a lock: never a fallback view.
        outside=(SOURCES/'then.c').read_text().replace('    if (mode == 0) {','    pthread_mutex_lock(&m);\n    if (mode == 0) {').replace('    result += 1000;','    pthread_mutex_unlock(&m);\n    result += 1000;')
        for bad in [crossed,outside]:
            with self.subTest(source=bad):
                self.assertEqual(RULE,select_rule(bad))
                with self.assertRaises(ValueError):self.build('same',bad)

    def test_rejected_blocks_and_timing_never_publish_or_fall_back(self):
        from Code2DAG.pipeline.composition_c2a_timing import import_branch_lock_timing
        h=c1.CompositionC1Tests();valid=(SOURCES/'different.c').read_text()
        bad=valid.replace('pthread_mutex_unlock(&m);','pthread_mutex_unlock(&n);',1)
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'rejected.c';source.write_text(bad);base,p=h.native(tmp,source)
            with self.assertRaises(ValueError):run_composition_blocks(base_dir=base,base_name='rejected',source_file=source)
            self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())
            self.assertFalse((p/'blocks/level2/conditional_branches').exists());self.assertFalse((p/'blocks/level2/lock_grouped').exists())
            # A valid retry gets new generation metadata and one proper C2a view.
            source.write_text(valid)
            # Use another temporary native base to avoid retaining rejected metadata.
            with tempfile.TemporaryDirectory() as retry:
                base,p=h.native(retry,source);run_composition_blocks(base_dir=base,base_name='rejected',source_file=source)
                b=p/'blocks/level2'/RULE;s=json.loads((b/'segments.json').read_text());sp=b/'segments.json';saved=sp.read_bytes()
                weights={t['template_id']:dict(total_ns=10,count=1,avg_ns=10,min_ns=10,max_ns=10) for t in s['templates']}
                inp=Path(tmp)/'weights.json';inp.write_text(json.dumps({'weights':weights}));output=p/'timing/level2'/RULE/'timing.json'
                altered=copy.deepcopy(s);altered['lock_groups'][0]['condition_memberships'][0]['branch_id']='wrong';sp.write_text(json.dumps(altered));damaged=sp.read_bytes()
                with self.assertRaises(ValueError):import_branch_lock_timing(base_dir=base,base_name='rejected',input_timing=inp)
                self.assertFalse(output.exists());self.assertEqual(damaged,sp.read_bytes())
                sp.write_bytes(saved);import_branch_lock_timing(base_dir=base,base_name='rejected',input_timing=inp);self.assertTrue(output.exists())
