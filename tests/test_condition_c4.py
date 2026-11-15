"""C4: one multiline braced if, with an implicit empty false path."""
import shutil
import tempfile
from pathlib import Path
import unittest
from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.tests.test_condition_nested import ROOT, ALGOS, reachable, native_acceptance

SOURCE_C4=ROOT/'Code2DAG/source_files/condition_c4/condition_c4.c'
PATHS={'TRUE':(True,False),'FALSE':(False,False)}

def call_lines(source):
    return {name:next(i for i,line in enumerate(source.splitlines(),1) if 'puts("'+name+'")' in line)
            for name in ('optional_work','after_condition')}

def views():
    source=SOURCE_C4.read_text();lines=source.splitlines()
    first=next(i for i,l in enumerate(lines,1) if '(void)unused;' in l)
    last=next(i for i,l in enumerate(lines,1) if 'return NULL;' in l)
    main=next(i for i,l in enumerate(lines,1) if 'puts("main:start")' in l)
    bs={'segments':[{'seg_id':'W','function':'worker','kind':'compute','start_line':first,'end_line':last},
                    {'seg_id':'M','function':'main','kind':'compute','start_line':main,'end_line':len(lines)-1}]}
    return source,build_condition_views(base_name='c4',source_text=source,base_segments_json=bs,base_dag_json={'nodes':['W','M'],'edges':[]})

def structure_check(source,st,sd,ss,dd):
    assert len(ss['condition_regions'])==1
    outer=ss['condition_regions'][0];group=outer['condition_id']
    assert outer['parent_id'] is None and ss['capabilities']['scheduler_compatible']
    branches={b['branch_id']:b for b in outer['branches']}
    assert set(branches)=={'then','else'}
    assert all(branches['else'][key] is None for key in ('start_line','end_line'))
    assert len(sd['exclusive_groups'])==len(dd['exclusive_groups'])==1
    controls={}
    for kind,short in [('condition_entry','entry'),('condition_exit','exit')]:
        rows=[r for r in st['segments'] if r['kind']==kind and r['condition_id']==group]
        assert len(rows)==1;controls['outer_'+short]=rows[0]['seg_id']
    labels={}
    for name,line in call_lines(source).items():
        rows=[r for r in ss['segments'] if r['function']=='worker' and r['start_line']<=line<=r['end_line']]
        assert len(rows)==1;row=rows[0];labels[name]=row['seg_id']
        members={(m['exclusive_group'],m['branch_id']) for m in row.get('condition_memberships',[])}
        assert members==({(group,'then')} if name=='optional_work' else set())
    assert len(set(labels.values()))==2
    assert not any(m['branch_id']=='else' for r in ss['segments'] for m in r.get('condition_memberships',[]))
    entry,exit=controls['outer_entry'],controls['outer_exit']
    bypass=[e for e in sd['edges'] if e['src']==entry and e['dst']==exit]
    assert len(bypass)==1 and bypass[0]['branch_id']=='else' and bypass[0]['guard']=='false' and bypass[0]['exclusive_group']==group
    assert reachable(sd['edges'],entry,labels['optional_work'])
    assert reachable(sd['edges'],labels['optional_work'],exit)
    assert reachable(sd['edges'],exit,labels['after_condition'])
    for graph in (sd,dd):topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
    return {'outer':outer,'inner':None,'labels':labels,'control_nodes':controls}

def active(ss,structure,flags):
    choice='then' if flags[0] else 'else'
    return {r['seg_id'] for r in ss['segments'] if all(m['branch_id']==choice for m in r.get('condition_memberships',[]))}

def project(st,sd,structure,flags):
    group=structure['outer']['condition_id'];choice='then' if flags[0] else 'else'
    selected=set()
    branch=next(b for b in structure['outer']['branches'] if b['branch_id']=='then')
    for r in st['segments']:
        in_then=not r.get('synthetic') and r['function']=='worker' and branch['start_line']<=r['start_line']<=r['end_line']<=branch['end_line']
        if flags[0] or not in_then:selected.add(r['seg_id'])
    edges=[e for e in sd['edges'] if e['src'] in selected and e['dst'] in selected and
           (e.get('exclusive_group')!=group or e.get('branch_id')==choice)]
    c=structure['control_nodes'];labels=structure['labels']
    assert (labels['optional_work'] in selected)==flags[0]
    assert reachable(edges,c['outer_entry'],c['outer_exit']) and reachable(edges,c['outer_exit'],labels['after_condition'])
    if not flags[0]:assert any(e['src']==c['outer_entry'] and e['dst']==c['outer_exit'] for e in edges)
    else:assert not any(e['src']==c['outer_entry'] and e['dst']==c['outer_exit'] for e in edges)
    topo_sort(sorted(selected),[(e['src'],e['dst']) for e in edges])
    return selected,edges

def expected_output(flags):
    return '\n'.join(['main:start']+(['optional_work'] if flags[0] else [])+['after_condition',f'value={110 if flags[0] else 100} evaluations=1','main:end',''])

CASE={'paths':PATHS,'call_lines':call_lines,'validate_structure':structure_check,'project':project,'active_nodes':active,
      'expected_output':expected_output,'environment':lambda f:{'C2D_FLAG':'1' if f[0] else None},
      'scope':'C4: one multiline braced if; implicit empty false path; no nesting or early transfer'}

class C4Tests(unittest.TestCase):
    def test_implicit_false_is_direct_structural_edge_without_execution_node(self):
        source,(st,sd,ss,dd,meta)=views();structure_check(source,st,sd,ss,dd)
        self.assertTrue(meta['scheduler_compatible'])
    def test_true_and_false_projections_rejoin_before_after_condition(self):
        source,(st,sd,ss,dd,_)=views();structure=structure_check(source,st,sd,ss,dd)
        for flags in PATHS.values():project(st,sd,structure,flags)
    def test_five_algorithms_assign_only_real_execution_nodes(self):
        source,(st,sd,ss,dd,_)=views();structure_check(source,st,sd,ss,dd)
        for algo in ALGOS:
            result=get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json={'weights':{n:{'avg_ns':1} for n in dd['nodes']}})
            self.assertEqual(set(dd['nodes']),set(result['priorities']))
            self.assertFalse(any(r.get('synthetic') for r in ss['segments']))
    @unittest.skipUnless(shutil.which('gcc-13'),'Ubuntu GCC13 required')
    def test_native_two_paths_mapping_dependencies_and_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            result=native_acceptance(Path(tmp)/'acceptance',source_file=SOURCE_C4,case=CASE)
            self.assertTrue(result['passed']);self.assertEqual(set(PATHS),set(result['paths']))

if __name__=='__main__':unittest.main()
