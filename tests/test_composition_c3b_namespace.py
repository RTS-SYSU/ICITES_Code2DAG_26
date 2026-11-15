"""Real function identity, distinct from a thread entry or local node name."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from Code2DAG.tests import test_composition_c3b as c3b
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition_c3b_model import build_loop_condition_views,_condition_body_function,RULE
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.composition_model import CompositionError


def renamed(name,function):
    text,raw,dag=c3b.fixture(name);text=text.replace('worker',function)
    for row in raw['segments']:
        if row['function']=='worker':row['function']=function
    for loc in raw['node_locations'].values():
        if loc['function']=='worker':loc['function']=function
    raw['thread_entry_functions']=[function]
    return text,raw,dag


class C3bNamespaceTests(unittest.TestCase):
    def test_zero_one_many_and_two_dimensional_function_identity(self):
        for function in ['worker','task','compute_task_47']:
            for name in ['zero','one','many','l3']:
                with self.subTest(function=function,name=name):
                    text,raw,dag=renamed(name,function);before=copy.deepcopy((raw,dag))
                    st,sd,s,d,_=build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
                    self.assertEqual(before,(raw,dag));self.assertEqual([function],s['thread_entry_functions'])
                    self.assertTrue(all(b['function']==function for b in s['condition_templates']))
                    positions=s['mappings']['source_position_to_execution_segments']
                    for i,line in enumerate(text.splitlines(),1):
                        if 'result +=' in line:
                            key='LOOPSRC:'+function+':line:'+str(i);self.assertIn(key,positions)
                            if name=='zero' and ('THEN' in line or 'ELSE' in line):self.assertEqual([],positions[key])
                    if function!='worker':self.assertFalse(any(k.startswith('LOOPSRC:worker:') for k in positions))
                    if name=='l3':self.assertEqual({(i,j) for i in range(2) for j in range(3)},{(r['outer_iteration_index'],r['inner_iteration_index']) for r in s['condition_regions']})

    def test_missing_or_conflicting_formal_function_ownership_rejects(self):
        from Code2DAG.pipeline.condition_model import ConditionalRegion
        region=ConditionalRegion('C','task',1,2,1,2,[])
        for loop,regions in [({},[region]),({'function':None},[region]),({'function':''},[region]),({'function':'wrong'},[region]),({'function':'task'},[])]:
            with self.subTest(loop=loop),self.assertRaisesRegex(CompositionError,'function owner'):_condition_body_function(loop,regions)
        text,raw,dag=renamed('zero','task');raw['segments'][0].pop('function')
        with self.assertRaisesRegex(CompositionError,'function owner'):build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)

    def test_missing_owner_does_not_publish_partial_view(self):
        from Code2DAG.pipeline.runner import run_blocks
        text,raw,dag=renamed('zero','task');raw['segments'][0].pop('function')
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);source=base/'task.c';source.write_text(text);p=base/'intermediate_results/fixture/pipeline';p.mkdir(parents=True);(p/'block_info.json').write_text(json.dumps({'source_file':str(source)}))
            with patch('Code2DAG.pipeline.rules.composition_c3b_rule._build_merge_post_wait'),patch('Code2DAG.pipeline.rules.composition_c3b_rule.build_level2_segments_and_dag',return_value=(raw,dag)):
                with self.assertRaisesRegex(CompositionError,'function owner'):run_blocks(base_dir=base,base_name='fixture',level='level2',rule_name=RULE,source_file=source)
            self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_thread_entry_and_internal_function_are_not_conflated(self):
        text,raw,dag=renamed('zero','helper')
        text=text.replace('int main(int argc','void *entry(void *arg) {\n    return helper(arg);\n}\nint main(int argc').replace('NULL, helper, NULL','NULL, entry, NULL')
        for row in raw['segments']:
            if row['function']=='main':row['start_line']+=3;row['end_line']+=3
        for loc in raw['node_locations'].values():
            if loc['function']=='main':loc['line']+=3
        raw['thread_entry_functions']=['entry']
        entryline=next(i for i,l in enumerate(text.splitlines(),1) if 'return helper(arg)' in l)
        raw['segments'].append(dict(seg_id='ENTRY',function='entry',kind='compute',start_line=entryline,end_line=entryline))
        raw['node_locations']['entry/helper1']=dict(function='entry',line=entryline);raw['node_to_segment']['entry/helper1']='ENTRY'
        dag['nodes'].append('ENTRY')
        next(e for e in dag['edges'] if e['kind']=='create')['dst']='ENTRY'
        dag['edges'].append(dict(src='ENTRY',dst='OLD',kind='call'))
        st,sd,s,d,_=build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(['entry'],s['thread_entry_functions']);self.assertEqual({'helper'},{b['function'] for b in s['condition_templates']})
        i=next(i for i,line in enumerate(text.splitlines(),1) if 'THEN' in line)
        self.assertEqual([],s['mappings']['source_position_to_execution_segments']['LOOPSRC:helper:line:'+str(i)])
        self.assertNotIn('LOOPSRC:entry:line:'+str(i),s['mappings']['source_position_to_execution_segments'])

    def test_native_missing_internal_owner_is_rejected_without_fallback(self):
        text=(c3b.SOURCES/'zero.c').read_text().replace('worker','helper')
        text=text.replace('int main(int argc','void *entry(void *arg) {\n    return helper(arg);\n}\nint main(int argc').replace('NULL, helper, NULL','NULL, entry, NULL')
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'helper_case.c';source.write_text(text);base,p=c1.CompositionC1Tests().native(tmp,source)
            with self.assertRaisesRegex(CompositionError,'unowned Condition'):run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
            self.assertFalse((p/'blocks/level2'/RULE/'segments.json').exists())

    def test_two_functions_keep_identical_local_names_separate(self):
        text,raw,dag=renamed('one','left')
        right='void *right(void *unused) {\n    result += 5;\n    return NULL;\n}\n'
        text=text.replace('int main(int argc',right+'int main(int argc')
        for row in raw['segments']:
            row['local_name']='body'
            if row['function']=='main':row['start_line']+=4;row['end_line']+=4
        for loc in raw['node_locations'].values():
            if loc['function']=='main':loc['line']+=4
        line=next(i for i,l in enumerate(text.splitlines(),1) if 'result += 5;' in l)
        raw['segments'].append(dict(seg_id='RIGHT:body',local_name='body',function='right',kind='compute',start_line=line,end_line=line+1));dag['nodes'].append('RIGHT:body')
        st,sd,s,d,_=build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        targets=s['mappings']['original_segment_to_execution_segments']['RIGHT:body'];self.assertEqual(['RIGHT:body'],targets)
        rightrow=next(r for r in s['segments'] if r['seg_id']=='RIGHT:body');self.assertEqual('right',rightrow['function']);self.assertEqual('body',rightrow['local_name'])
        self.assertEqual(['RIGHT:body'],s['mappings']['source_position_to_execution_segments']['LOOPSRC:right:line:'+str(line)])
        self.assertTrue(all(r['function']=='left' for r in s['condition_regions']))

    def test_l2_different_function_ranges_reject_without_cross_mapping(self):
        text,raw,dag=renamed('one','left')
        start=text.index('void *left');end=text.index('int main');right=text[start:end].replace('left','right')
        text=text[:end]+right+text[end:];shift=len(right.splitlines())
        for row in raw['segments']:
            if row['function']=='main':row['start_line']+=shift;row['end_line']+=shift
        for loc in raw['node_locations'].values():
            if loc['function']=='main':loc['line']+=shift
        lines=text.splitlines();a=next(i for i,l in enumerate(lines,1) if l.startswith('void *right'))+1;b=next(i for i in range(a,len(lines)) if lines[i-1]=='}')-1
        raw['segments'].append(dict(seg_id='RIGHT',function='right',kind='compute',start_line=a,end_line=b));dag['nodes'].append('RIGHT');before=copy.deepcopy((raw,dag))
        with self.assertRaisesRegex(ValueError,'same function'):build_loop_condition_views(base_name='fixture',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(before,(raw,dag))
