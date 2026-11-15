"""C4b: one static Loop wholly inside a matched lock/unlock critical region."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import run_composition_blocks,select_rule
from Code2DAG.pipeline.composition_c4b_model import RULE,build_loop_in_lock_views
from Code2DAG.pipeline.composition_c4b_timing import import_loop_in_lock_timing
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag

ROOT=Path(__file__).resolve().parents[2];SOURCES=ROOT/'Code2DAG/source_files/composition_c4b'
FIELDS=('total_ns','count','avg_ns','min_ns','max_ns')


def weight_table(templates):
    return {template['template_id']:dict(total_ns=10+i,count=1,avg_ns=10+i,min_ns=10+i,max_ns=10+i) for i,template in enumerate(templates)}


def group_expected(group,weights):
    counts={}
    for member in group['members']:counts[member['template_id']]=counts.get(member['template_id'],0)+1
    return {field:sum(weights[tid][field]*count for tid,count in counts.items()) for field in FIELDS},counts


class C4bTests(unittest.TestCase):
    def publish(self,tmp,source):
        tester=c1.CompositionC1Tests();base,p=tester.native(tmp,source)
        meta=run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
        root=p/'blocks/level2'/RULE
        st=json.loads((root/'segments_struct.json').read_text());sd=json.loads((root/'dag_struct.json').read_text())
        s=json.loads((root/'segments_sched.json').read_text());d=json.loads((root/'dag_sched.json').read_text())
        for segments,dag in [(st,sd),(s,d)]:
            ids=[row['seg_id'] for row in segments['segments']]
            self.assertEqual(len(ids),len(set(ids)));self.assertEqual(set(ids),set(dag['nodes']))
            self.assertFalse(any(edge['src']==edge['dst'] for edge in dag['edges']))
            topo_sort(dag['nodes'],[(edge['src'],edge['dst']) for edge in dag['edges']])
        regions=st['critical_regions']
        for row in s['segments']:
            if row['kind']=='lock_group':continue
            self.assertFalse(any(region['function']==row['function'] and region['lock_range'][0]<=row['start_line'] and row['end_line']<=region['unlock_range'][0] for region in regions),(row,regions))
        for row in st['segments']:
            if row.get('role')!='iteration':continue
            self.assertEqual([row['lock_group_id']],st['mappings']['structural_node_to_execution_segments'][row['seg_id']])
            self.assertEqual(row['lock_group_id'],st['mappings']['iteration_to_group'][row['seg_id']])
        return tester,base,p,meta,st,sd,s,d

    def timing(self,tmp,base,name,s,weights=None):
        weights=weight_table(s['templates']) if weights is None else weights
        inp=Path(tmp)/'weights.json';inp.write_text(json.dumps(dict(weights=weights)))
        return import_loop_in_lock_timing(base_dir=base,base_name=name,input_timing=inp),weights

    def run_algorithms(self,base,name,d):
        for algo in c1.ALGOS:
            self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name=name,level='level2',rule_name=RULE,algo_name=algo)['priorities']))

    def test_l1_zero_one_many_prefix_suffix_timing_and_output(self):
        original=(SOURCES/'l1.c').read_text()
        for bound,expected_result in [(0,'RESULT:1104\n'),(1,'RESULT:1106\n'),(3,'RESULT:1110\n')]:
            with self.subTest(bound=bound),tempfile.TemporaryDirectory() as tmp:
                text=original.replace('i < 3','i < '+str(bound));source=Path(tmp)/'l1.c';source.write_text(text);before=source.read_bytes()
                tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
                self.assertEqual('composition_c4b',meta['rule_name']);self.assertEqual(1,len(st['lock_groups']))
                group=st['lock_groups'][0];self.assertEqual('LOCKGROUP:m',group['group_id'])
                self.assertEqual(['LOCKGROUP:m'],[row['seg_id'] for row in s['segments'] if row['kind']=='lock_group'])
                self.assertFalse(any(row.get('role')=='iteration' for row in s['segments']))
                iterations=[row for row in st['segments'] if row.get('role')=='iteration']
                self.assertEqual(list(range(bound)),[row['iteration_index'] for row in iterations])
                self.assertEqual(bound,sum(row['seg_id'].startswith('LOOP:') for row in iterations))
                for row in iterations:
                    self.assertEqual('LOCKGROUP:m',st['mappings']['structural_node_to_execution_segments'][row['seg_id']][0])
                    self.assertEqual('LOCKGROUP:m',st['mappings']['iteration_to_group'][row['seg_id']])
                self.assertFalse(any(row['seg_id'].startswith('LOOP:') for row in s['segments']))
                before_tid=next(t['template_id'] for t in st['templates'] if t['role']=='before')
                body_tid=next(t['template_id'] for t in st['templates'] if t['role']=='body_template')
                after_tid=next(t['template_id'] for t in st['templates'] if t['role']=='after')
                counts={}
                for member in group['members']:counts[member['template_id']]=counts.get(member['template_id'],0)+1
                self.assertEqual(bound,counts.get(body_tid,0));self.assertEqual(1,counts[before_tid]);self.assertEqual(1,counts[after_tid])
                body_line=next(i for i,l in enumerate(text.splitlines(),1) if 'result += 2;' in l)
                body_key='LOOPSRC:task:line:'+str(body_line)
                self.assertEqual([] if bound==0 else ['LOCKGROUP:m'],st['mappings']['template_to_execution_segments'][body_tid])
                self.assertEqual([] if bound==0 else ['LOCKGROUP:m'],s['mappings']['source_position_to_execution_segments'][body_key])
                result,weights=self.timing(tmp,base,source.stem,s)
                expected={field:sum(weights[tid][field]*count for tid,count in counts.items()) for field in FIELDS}
                self.assertEqual(expected,result['weights']['LOCKGROUP:m'])
                self.assertEqual(bound,len([c for g in result['lock_aggregation']['contributions'].values() for c in g if c['template_id']==body_tid]))
                self.run_algorithms(base,source.stem,d)
                binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual(expected_result,tester.command([binary]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_zero_iteration_empty_critical_region(self):
        text=(SOURCES/'l1.c').read_text().replace('    result += 1;\n','').replace('    result += 3;\n','').replace('i < 3','i < 0')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'empty.c';source.write_text(text);before=source.read_bytes()
            tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            group=st['lock_groups'][0];self.assertEqual([],group['member_segment_ids']);self.assertEqual([],group['members'])
            self.assertFalse(any(row.get('role')=='iteration' for row in st['segments']))
            self.assertIn('LOCKGROUP:m',d['nodes']);self.assertTrue(any(edge['src']=='LOCKGROUP:m' or edge['dst']=='LOCKGROUP:m' for edge in d['edges']))
            body_tid=next(t['template_id'] for t in s['templates'] if t['role']=='body_template')
            self.assertEqual([],s['mappings']['template_to_execution_segments'][body_tid])
            result,weights=self.timing(tmp,base,source.stem,s)
            self.assertEqual({field:0 for field in FIELDS},result['weights']['LOCKGROUP:m'])
            self.run_algorithms(base,source.stem,d)
            binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1100\n',tester.command([binary]).stdout)
            self.assertEqual(before,source.read_bytes())

    def test_l2_l3_and_two_mutex_groups(self):
        cases=[('l2.c','RESULT:1127\n',5,2),('l3.c','RESULT:1116\n',6,2),('two_mutex.c','RESULT:1123\n',5,2)]
        for name,expected_result,iteration_count,loop_count in cases:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source_path=SOURCES/name;text=source_path.read_text();source=Path(tmp)/name;source.write_text(text);before=source.read_bytes()
                tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
                self.assertEqual(loop_count,len(st['loops']));self.assertEqual(iteration_count,len([row for row in st['segments'] if row.get('role')=='iteration']))
                if name=='l3.c':
                    self.assertEqual({(i,j) for i in range(2) for j in range(3)},{(row['outer_iteration_index'],row['inner_iteration_index']) for row in st['segments'] if row.get('role')=='iteration'})
                    groups=[g for g in st['lock_groups']];self.assertEqual(1,len(groups))
                    body=next(t['template_id'] for t in st['templates'] if t['role']=='body_template')
                    self.assertEqual(6,sum(1 for m in groups[0]['members'] if m['template_id']==body))
                if name=='l2.c':
                    groups=[g for g in st['lock_groups']];self.assertEqual(1,len(groups))
                    middle=next(i for i,l in enumerate(text.splitlines(),1) if 'result += 7;' in l)
                    self.assertTrue(any(m.get('start_line')==middle for m in groups[0]['members']))
                    self.assertTrue(any(m.get('role')=='after' for m in groups[0]['members']))
                if name=='two_mutex.c':
                    self.assertEqual({'LOCKGROUP:m','LOCKGROUP:q'},{g['group_id'] for g in st['lock_groups']})
                    self.assertEqual({'LOCKGROUP:m','LOCKGROUP:q'},{row['seg_id'] for row in s['segments'] if row['kind']=='lock_group'})
                    self.assertFalse(any(row.get('lock_group_id')=='LOCKGROUP:m' and row.get('iteration_index') is not None and row.get('loop_id','').startswith('LOOP:task@16') for row in st['segments']))
                result,weights=self.timing(tmp,base,source.stem,s)
                for group in st['lock_groups']:
                    expected,counts=group_expected(group,weights);self.assertEqual(expected,result['weights'][group['group_id']])
                    self.assertEqual(group['member_template_ids'],list(dict.fromkeys(m['template_id'] for m in group['members'])))
                if name=='l3.c':
                    sp=p/'blocks/level2'/RULE/'segments.json';saved=sp.read_bytes();tampered=json.loads(saved);tampered['iteration_instances'][0]['outer_iteration_index']='0';sp.write_text(json.dumps(tampered))
                    inp=Path(tmp)/'weights.json'
                    with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name=source.stem,input_timing=inp)
                    sp.write_bytes(saved);self.timing(tmp,base,source.stem,s)
                self.run_algorithms(base,source.stem,d)
                binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual(expected_result,tester.command([binary]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_sequential_condition_outside_lock_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path=SOURCES/'outside_condition.c';text=source_path.read_text();source=Path(tmp)/'outside_condition.c';source.write_text(text);before=source.read_bytes()
            tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            self.assertEqual({'task'},{region['function'] for region in st['condition_regions']})
            self.assertTrue(any(row.get('condition_memberships') for row in s['segments'] if row['function']=='task'))
            self.assertEqual(1,len(st['lock_groups']))
            result,weights=self.timing(tmp,base,source.stem,s)
            group=st['lock_groups'][0];expected,_=group_expected(group,weights);self.assertEqual(expected,result['weights']['LOCKGROUP:m'])
            self.run_algorithms(base,source.stem,d)
            binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1112\n',tester.command([binary]).stdout)
            self.assertEqual(before,source.read_bytes())

    def test_l3_zero_product_matrix_and_l2_zero_behavior(self):
        for outer,inner,expected_result in [(0,2,'RESULT:1104\n'),(2,0,'RESULT:1104\n'),(0,0,'RESULT:1104\n'),(2,3,'RESULT:1116\n')]:
            with self.subTest(outer=outer,inner=inner),tempfile.TemporaryDirectory() as tmp:
                text=(SOURCES/'l3.c').read_text().replace('i < 2','i < '+str(outer)).replace('j < 3','j < '+str(inner))
                source=Path(tmp)/'l3_zero.c';source.write_text(text);before=source.read_bytes()
                tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
                group=st['lock_groups'][0];product=outer*inner
                body_tid=next(t['template_id'] for t in s['templates'] if t['role']=='body_template')
                before_tid=next(t['template_id'] for t in s['templates'] if t['role']=='before')
                after_tid=next(t['template_id'] for t in s['templates'] if t['role']=='after')
                self.assertEqual([] if product==0 else ['LOCKGROUP:m'],s['mappings']['template_to_execution_segments'][body_tid])
                self.assertEqual(product,len([r for r in st['segments'] if r.get('role')=='iteration']))
                self.assertEqual(product,len(s.get('iteration_instances',[])))
                result,weights=self.timing(tmp,base,source.stem,s)
                expected={field:weights[before_tid][field]+weights[after_tid][field]+(product*weights[body_tid][field] if product else 0) for field in FIELDS}
                self.assertEqual(expected,result['weights']['LOCKGROUP:m'])
                self.assertEqual(product,len([c for c in result['lock_aggregation']['contributions']['LOCKGROUP:m'] if c['template_id']==body_tid]))
                self.run_algorithms(base,source.stem,d)
                binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual(expected_result,tester.command([binary]).stdout)
                self.assertEqual(before,source.read_bytes())
        with tempfile.TemporaryDirectory() as tmp:
            text=(SOURCES/'l2.c').read_text().replace('i < 2','i < 0').replace('j < 3','j < 0');source=Path(tmp)/'l2_zero.c';source.write_text(text);before=source.read_bytes()
            tester,base,p,meta,st,sd,s,d=self.publish(tmp,source);group=st['lock_groups'][0]
            body_tids=[t['template_id'] for t in s['templates'] if t['role']=='body_template']
            self.assertEqual(2,len(body_tids));self.assertTrue(all(s['mappings']['template_to_execution_segments'][tid]==[] for tid in body_tids))
            self.assertFalse(any(r.get('role')=='iteration' for r in st['segments']))
            result,weights=self.timing(tmp,base,source.stem,s)
            expected,_=group_expected(group,weights);self.assertEqual(expected,result['weights']['LOCKGROUP:m'])
            self.run_algorithms(base,source.stem,d)
            binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1111\n',tester.command([binary]).stdout)
            self.assertEqual(before,source.read_bytes())

    def test_loop_template_contract_rejections(self):
        from Code2DAG.pipeline.composition_c4b_timing import _validate_loop_template_contract
        def valid():
            loops=[dict(loop_id='L',parent_loop_id=None,iteration_count=0,body_start=2,body_end=2,function='f')]
            templates=[dict(template_id='BODY',role='body_template',start_line=2,end_line=2)]
            return dict(loops=loops,templates=templates,iteration_instances=[]),{'BODY':[]}
        segments,mapping=valid();_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();segments['templates']=[];mapping={}
        with self.assertRaisesRegex(ValueError,'leaf Loop must publish'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();mapping['BODY']=['LOCKGROUP:m']
        with self.assertRaisesRegex(ValueError,'zero-iteration Loop body template'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();segments['loops'][0]['iteration_count']=1;mapping['BODY']=[]
        with self.assertRaisesRegex(ValueError,'nonempty Loop body template'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();mapping['UNKNOWN']=[]
        with self.assertRaisesRegex(ValueError,'cover exactly'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();segments['loops']=[dict(loop_id='OUT',parent_loop_id=None,iteration_count=1,body_start=2,body_end=4,function='f'),dict(loop_id='IN',parent_loop_id='OUT',iteration_count=2,body_start=3,body_end=3,function='f')];segments['templates']=[dict(template_id='OUTBODY',loop_id='OUT',role='body_template',start_line=2,end_line=4)];mapping={'OUTBODY':[]}
        with self.assertRaisesRegex(ValueError,'nested outer Loop'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();segments['templates'].append(dict(template_id='BEFORE',role='before',start_line=1,end_line=1));mapping['BEFORE']=[]
        with self.assertRaisesRegex(ValueError,'unused template'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})
        segments,mapping=valid();segments['loops'][0]['iteration_count']=2;mapping['BODY']=['LOCKGROUP:m'];segments['iteration_instances']=[]
        with self.assertRaisesRegex(ValueError,'iteration instance count'):_validate_loop_template_contract(segments,mapping,{'LOCKGROUP:m'})

    def test_lock_l1_same_mutex_parallel_regions_one_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path=SOURCES/'two_regions.c';text=source_path.read_text();source=Path(tmp)/'two_regions.c';source.write_text(text);before=source.read_bytes()
            tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            self.assertEqual(1,len(st['lock_groups']));group=st['lock_groups'][0]
            self.assertEqual('LOCKGROUP:shared_mutex',group['group_id'])
            self.assertEqual({'thread_a','thread_b'},{member['function'] for member in group['members']})
            self.assertEqual({'LOOP:thread_a@11','LOOP:thread_b@22'},{loop['loop_id'] for loop in st['loops']})
            self.assertEqual(5,len([row for row in st['segments'] if row.get('role')=='iteration']))
            result,weights=self.timing(tmp,base,source.stem,s)
            expected,counts=group_expected(group,weights);self.assertEqual(expected,result['weights'][group['group_id']])
            self.assertEqual(2,len([tid for tid in counts if 'body_template' in tid]))
            self.run_algorithms(base,source.stem,d)
            binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:503\n',tester.command([binary]).stdout)
            self.assertEqual(before,source.read_bytes())

    def test_main_helper_namespace_and_independent_conditions(self):
        for name,function,expected_result,threads in [('main.c','main','RESULT:127\n',['main','task']),('helper.c','worker','RESULT:1126\n',['main','task'])]:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source_path=SOURCES/name;text=source_path.read_text();source=Path(tmp)/name;source.write_text(text);before=source.read_bytes()
                tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
                self.assertEqual(threads,st['thread_entry_functions']);self.assertEqual(function,st['loops'][0]['function'])
                self.assertEqual('LOCKGROUP:m',st['loops'][0]['lock_group_id']);self.assertEqual('<lockgroup>',next(row['function'] for row in s['segments'] if row['kind']=='lock_group'))
                self.assertEqual({'observer'},{region['function'] for region in st['condition_regions']})
                self.assertEqual(['C4BHELPER:observer'],[row['seg_id'] for row in st['source_only_segments']])
                self.assertFalse(any(row['function']=='observer' for row in s['segments']))
                self.assertTrue(any(row.get('source_only') for row in st['segments'] if row['function']=='observer'))
                self.assertTrue({'create','join','intra'}<={edge['kind'] for edge in d['edges']})
                if name=='helper.c':self.assertNotIn('worker',st['thread_entry_functions'])
                result,weights=self.timing(tmp,base,source.stem,s)
                for group in st['lock_groups']:
                    expected,_=group_expected(group,weights);self.assertEqual(expected,result['weights'][group['group_id']])
                self.run_algorithms(base,source.stem,d)
                binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual(expected_result,tester.command([binary]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_sem_create_join_and_split_tail_provenance(self):
        for name,side in [('sem.c','post'),('sem_prefix.c','pre')]:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source_path=SOURCES/name;text=source_path.read_text();source=Path(tmp)/name;source.write_text(text);before=source.read_bytes()
                tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
                raw,dag=build_level2_segments_and_dag(base_dir=base,base_name=source.stem,source_file=source)
                self.assertEqual(dag['edges'],[edge['edge'] for edge in st['composition_edge_provenance']])
                self.assertEqual(1,len(st['lock_groups']))
                kinds={edge['kind'] for edge in d['edges']}
                self.assertTrue({'create','join','sem_dep','intra'}<=kinds)
                for edge in d['edges']:
                    if edge['kind'] in ('create','join','sem_dep'):
                        self.assertTrue(edge.get('provenance'),edge)
                        self.assertTrue(edge['provenance'][0]['edge']['src'] in {b['seg_id'] for b in raw['segments']})
                sem=next(edge for edge in d['edges'] if edge['kind']=='sem_dep');raw_sem=next(edge for edge in dag['edges'] if edge['kind']=='sem_dep')
                self.assertEqual(raw_sem,sem['provenance'][0]['edge'])
                if side=='post':
                    self.assertEqual('BR:task#003@17-17',sem['src']);self.assertTrue(any(edge['src']=='LOCKGROUP:m' and edge['dst']==sem['src'] for edge in d['edges']))
                else:
                    self.assertEqual('BR:task#002@10-10',sem['dst']);self.assertTrue(any(edge['src']==sem['dst'] and edge['dst']=='LOCKGROUP:m' for edge in d['edges']))
                result,weights=self.timing(tmp,base,source.stem,s)
                group=st['lock_groups'][0];expected,_=group_expected(group,weights);self.assertEqual(expected,result['weights']['LOCKGROUP:m'])
                self.assertFalse(set(group['member_segment_ids'])&{sem['src'],sem['dst']})
                self.run_algorithms(base,source.stem,d)
                binary=Path(tmp)/'program';tester.command(['gcc-13','-pthread',source,'-o',binary]);self.assertEqual('RESULT:1110\n',tester.command([binary]).stdout)
                self.assertEqual(before,source.read_bytes())

    def test_rejections_and_input_immutability(self):
        original=(SOURCES/'l1.c').read_text()
        variants={
            'lock_inside_loop':original.replace('    pthread_mutex_lock(&m);\n','').replace('        result += 2;','        pthread_mutex_lock(&m);\n        result += 2;\n        pthread_mutex_unlock(&m);').replace('    pthread_mutex_unlock(&m);\n',''),
            'cross_boundary':original.replace('    result += 3;\n    pthread_mutex_unlock(&m);','    result += 3;').replace('        result += 2;','        result += 2;\n    pthread_mutex_unlock(&m);'),
            'condition_in_lock':original.replace('    result += 1;','    if (result % 2) {\n        result += 5;\n    }'),
            'condition_in_loop':original.replace('        result += 2;','        if (result % 2) {\n            result += 2;\n        }'),
            'nested_mutex':original.replace('    result += 1;','    pthread_mutex_lock(&m);\n    result += 1;\n    pthread_mutex_unlock(&m);'),
        }
        for name,text in variants.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp:
                source=Path(tmp)/(name+'.c');source.write_text(text);tester=c1.CompositionC1Tests();base,p=tester.native(tmp,source)
                s,d=build_level2_segments_and_dag(base_dir=base,base_name=name,source_file=source)
                before=copy.deepcopy((s,d))
                with self.assertRaises(ValueError):build_loop_in_lock_views(base_name=name,source_text=text,base_segments_json=s,base_dag_json=d)
                self.assertEqual(before,(s,d))
                self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_failure_isolation_timing_rejection_and_legal_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'l1.c';source.write_text((SOURCES/'l1.c').read_text());tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            output=p/'timing/level2'/RULE/'timing.json';weights=weight_table(s['templates']);inp=Path(tmp)/'w.json';inp.write_text(json.dumps(dict(weights=weights)))
            import_loop_in_lock_timing(base_dir=base,base_name='l1',input_timing=inp);published=output.read_bytes();legal=inp.read_bytes()
            body_tid=next(t['template_id'] for t in s['templates'] if t['role']=='body_template')
            faults=[]
            bad=dict(weights);bad['LOOP:task@10:iteration:0']=dict(weights[body_tid]);faults.append(bad)
            bad=copy.deepcopy(weights);bad[body_tid]['count']=True;faults.append(bad)
            bad=copy.deepcopy(weights);bad.pop(next(iter(bad)));faults.append(bad)
            for bad in faults:
                inp.write_text(json.dumps(dict(weights=bad)))
                with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name='l1',input_timing=inp)
                self.assertEqual(published,output.read_bytes())
            inp.write_bytes(legal)
            sp=p/'blocks/level2'/RULE/'segments.json';saved=sp.read_bytes()
            for damage in ['template','member','source','iteration','mapping']:
                tampered=json.loads(saved);group=tampered['lock_groups'][0]
                if damage=='template':group['member_template_ids']=['CROSS']
                elif damage=='member':group['member_segment_ids']=['CROSS']
                elif damage=='source':tampered['mappings']['source_lock_region_to_group']={'CROSS':'LOCKGROUP:m'}
                elif damage=='iteration':tampered['iteration_instances'][0]['iteration_index']=True
                else:tampered['mappings']['template_to_execution_segments'][group['member_template_ids'][0]]=[]
                sp.write_text(json.dumps(tampered))
                with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name='l1',input_timing=inp)
                self.assertEqual(published,output.read_bytes())
            sp.write_bytes(saved)
            result=import_loop_in_lock_timing(base_dir=base,base_name='l1',input_timing=inp);self.assertEqual(published,output.read_bytes())
            self.assertEqual(result,import_loop_in_lock_timing(base_dir=base,base_name='l1',input_timing=inp))
            self.assertEqual(published,output.read_bytes())
            # A rejected C4b source never partially publishes blocks.
            bad_source=Path(tmp)/'bad.c';bad_source.write_text((SOURCES/'l1.c').read_text().replace('    result += 1;','    if (result % 2) {\n        result += 5;\n    }'))
            badwork=Path(tmp)/'badwork';badwork.mkdir();base2,p2=tester.native(badwork,bad_source)
            with self.assertRaises(ValueError):run_composition_blocks(base_dir=base2,base_name='bad',source_file=bad_source)
            self.assertFalse((p2/'blocks/level2'/RULE/'segments.json').exists());self.assertFalse((p2/'timing/level2'/RULE/'timing.json').exists())

    def test_zero_iteration_weights_cross_mutex_and_index_conflicts(self):
        with tempfile.TemporaryDirectory() as tmp:
            text=(SOURCES/'l1.c').read_text().replace('i < 3','i < 0');source=Path(tmp)/'z.c';source.write_text(text)
            tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            body_tid=next(t['template_id'] for t in s['templates'] if t['role']=='body_template')
            weights=weight_table(s['templates']);inp=Path(tmp)/'w.json';inp.write_text(json.dumps(dict(weights=weights)))
            result=import_loop_in_lock_timing(base_dir=base,base_name='z',input_timing=inp)
            group=next(g for g in s['lock_groups'] if g['group_id']=='LOCKGROUP:m')
            self.assertEqual(0,sum(1 for member in group['members'] if member['template_id']==body_tid))
            self.assertNotIn(body_tid,[member['template_id'] for member in group['members']])
            bad=dict(weights);bad['LOOP:task@10:iteration:0']=dict(weights[body_tid]);inp.write_text(json.dumps(dict(weights=bad)))
            with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name='z',input_timing=inp)
            self.assertIn(body_tid,[t['template_id'] for t in s['templates']])
            self.assertEqual([],s['mappings']['template_to_execution_segments'][body_tid])
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'two.c';source.write_text((SOURCES/'two_mutex.c').read_text());tester,base,p,meta,st,sd,s,d=self.publish(tmp,source)
            weights=weight_table(s['templates']);inp=Path(tmp)/'w.json';inp.write_text(json.dumps(dict(weights=weights)))
            import_loop_in_lock_timing(base_dir=base,base_name='two',input_timing=inp)
            sp=p/'blocks/level2'/RULE/'segments.json';saved=sp.read_bytes();tampered=json.loads(saved)
            m=next(g for g in tampered['lock_groups'] if g['group_id']=='LOCKGROUP:m');q=next(g for g in tampered['lock_groups'] if g['group_id']=='LOCKGROUP:q')
            q['member_template_ids']=list(m['member_template_ids']);sp.write_text(json.dumps(tampered))
            with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name='two',input_timing=inp)
            sp.write_bytes(saved);import_loop_in_lock_timing(base_dir=base,base_name='two',input_timing=inp)
            tampered=json.loads(saved);tampered['loops'][0]['iteration_count']=True;sp.write_text(json.dumps(tampered))
            with self.assertRaises(ValueError):import_loop_in_lock_timing(base_dir=base,base_name='two',input_timing=inp)
            sp.write_bytes(saved);import_loop_in_lock_timing(base_dir=base,base_name='two',input_timing=inp)

    def test_c4a_direct_rejection_and_c4b_dispatch(self):
        from Code2DAG.pipeline.composition_c4a_model import build_loop_lock_views as build_c4a
        original=(SOURCES.parent/'composition_c4a'/'many.c').read_text()
        text=original.replace('        pthread_mutex_lock(&m);\n','').replace('        pthread_mutex_unlock(&m);\n','').replace('    for (','    pthread_mutex_lock(&m);\n    for (',1).replace('    result += 1000;','    pthread_mutex_unlock(&m);\n    result += 1000;')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'loop_in_lock.c';source.write_text(text);tester=c1.CompositionC1Tests();base,p=tester.native(tmp,source)
            s,d=build_level2_segments_and_dag(base_dir=base,base_name='loop_in_lock',source_file=source)
            with self.assertRaises(ValueError):build_c4a(base_name='loop_in_lock',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual('composition_c4b',select_rule(text))
            run_composition_blocks(base_dir=base,base_name='loop_in_lock',source_file=source)
            self.assertTrue((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_opaque_and_mixed_base_provenance_preserved_verbatim(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path=SOURCES/'l1.c';text=source_path.read_text();source=Path(tmp)/'chain.c';source.write_text(text)
            base,p=c1.CompositionC1Tests().native(tmp,source)
            s,d=build_level2_segments_and_dag(base_dir=base,base_name='chain',source_file=source)
            opaque={'qa_origin':{'edge_chain':[0,{'nested':['x',True,None]}]},'flags':{'keep':[1,2]}}
            existing={'base_edge_index':900,'edge':{'src':'QA','dst':'QA','kind':'qa'},'extra':{'keep':['yes']}}
            for i,edge in enumerate(d['edges']):
                edge['edge_id']='BASE:%d'%i;edge['custom']={'nested':[i,{'keep':True}]}
                edge['provenance']=[copy.deepcopy(opaque)]
                if i==0:edge['provenance'].append(copy.deepcopy(existing))
                if i==1:edge['provenance'].append(copy.deepcopy(opaque))
            before=copy.deepcopy((s,d))
            st,sd,ss,dd,_=build_loop_in_lock_views(base_name='chain',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(before,(s,d))
            edges=list(dd['edges'])+[item['edge'] for item in dd.get('removed_internal_edges',[])]
            for i,original in enumerate(d['edges']):
                matches=[edge for edge in edges if edge.get('edge_id')==original['edge_id']]
                self.assertTrue(matches,('base edge lost',i));self.assertTrue(all(edge.get('custom')==original['custom'] for edge in matches))
                witnesses=[edge for edge in edges if any(isinstance(item,dict) and item.get('base_edge_index')==i for item in edge.get('provenance',[]))]
                self.assertTrue(witnesses,('formal witness lost',i))
                target=next(edge for edge in witnesses if any(item==opaque for item in edge.get('provenance',[])))
                chain=target['provenance'];opaque_at=next(j for j,item in enumerate(chain) if item==opaque)
                formal_at=next(j for j,item in enumerate(chain) if isinstance(item,dict) and item.get('base_edge_index')==i)
                self.assertLess(opaque_at,formal_at);self.assertEqual(opaque,chain[opaque_at])
            duplicate=[edge for edge in edges if any(isinstance(item,dict) and item.get('base_edge_index')==1 for item in edge.get('provenance',[]))]
            self.assertTrue(duplicate);self.assertTrue(any(sum(1 for item in edge['provenance'] if item==opaque)>=2 for edge in duplicate))
            self.assertTrue(any(any(item==existing for item in edge.get('provenance',[])) for edge in edges))

    def test_malformed_internal_witness_rejected_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path=SOURCES/'l1.c';text=source_path.read_text();source=Path(tmp)/'bad.c';source.write_text(text)
            base,p=c1.CompositionC1Tests().native(tmp,source)
            s,base_d=build_level2_segments_and_dag(base_dir=base,base_name='bad',source_file=source)
            bad_values=[
                [{'base_edge_index':'0','edge':{'src':'a','dst':'b','kind':'intra'}}],
                [{'base_edge_index':0,'edge':'not-a-dict'}],
                [{'base_edge_index':0}],
                [{'base_edge_index':True,'edge':{'src':'a','dst':'b','kind':'intra'}}],
                [{'base_edge_index':0,'edge':{'src':'a'}}],
                'not-a-list',
            ]
            for value in bad_values:
                with self.subTest(value=value):
                    d=copy.deepcopy(base_d);d['edges'][0]['provenance']=copy.deepcopy(value);before=copy.deepcopy((s,d))
                    with self.assertRaisesRegex(ValueError,'witness|provenance'):
                        build_loop_in_lock_views(base_name='bad',source_text=text,base_segments_json=s,base_dag_json=d)
                    self.assertEqual(before,(s,d))

    def test_repeated_calls_and_dispatch(self):
        text=(SOURCES/'l1.c').read_text()
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'l1.c';source.write_text(text);base,p=c1.CompositionC1Tests().native(tmp,source)
            s,d=build_level2_segments_and_dag(base_dir=base,base_name='l1',source_file=source);before=copy.deepcopy((s,d))
            first=build_loop_in_lock_views(base_name='l1',source_text=text,base_segments_json=s,base_dag_json=d)
            second=build_loop_in_lock_views(base_name='l1',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(first,second);self.assertEqual(before,(s,d))
            self.assertEqual('composition_c4b',select_rule(text))
            lock_inside=(SOURCES.parent/'composition_c4a'/'many.c').read_text()
            self.assertEqual('composition_c4a',select_rule(lock_inside))

    def test_custom_edge_fields_and_provenance_survive_grouping(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'sem.c';source.write_text((SOURCES/'sem.c').read_text());base,p=c1.CompositionC1Tests().native(tmp,source)
            s,d=build_level2_segments_and_dag(base_dir=base,base_name='sem',source_file=source)
            for index,edge in enumerate(d['edges']):edge['custom']={'nested':[index,{'keep':True}]};edge['edge_id']='raw-'+str(index)
            st,sd,ss,dd,meta=build_loop_in_lock_views(base_name='sem',source_text=source.read_text(),base_segments_json=s,base_dag_json=d)
            for index,edge in enumerate(d['edges']):
                matches=[out for out in dd['edges'] if any(w.get('base_edge_index')==index or w.get('edge',{}).get('edge_id')==edge.get('edge_id') for w in out.get('provenance',[]))]
                if edge['src']==edge['dst']:continue
                self.assertTrue(matches or edge['kind']=='intra',('missing edge',index,edge,dd['edges']))
                for out in matches:
                    if out.get('edge_id')==edge.get('edge_id'):self.assertEqual(edge['custom'],out['custom'])


if __name__=='__main__':unittest.main()
