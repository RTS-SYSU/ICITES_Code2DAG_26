"""C3 only: exactly one else-if and a final else, all bodies braced."""
import json
from pathlib import Path
import unittest
from Code2DAG.pipeline.condition_model import build_condition_views
from Code2DAG.tests.test_condition_nested import ROOT
SOURCE_C3=ROOT/'Code2DAG/source_files/condition_c3/condition_c3.c'

def base_payload(source):
    lines=source.splitlines()
    first=next(i for i,l in enumerate(lines,1) if '(void)unused;' in l)
    last=next(i for i,l in enumerate(lines,1) if 'return NULL;' in l)
    main=next(i for i,l in enumerate(lines,1) if 'const char *arg' in l)
    return {'segments':[{'seg_id':'W','function':'worker','kind':'compute','start_line':first,'end_line':last},
       {'seg_id':'M','function':'main','kind':'compute','start_line':main,'end_line':len(lines)-1}]}, {'nodes':['W','M'],'edges':[]}

class C3Tests(unittest.TestCase):
    def test_c3_header_is_not_business_code_sharing_a_boundary(self):
        source=SOURCE_C3.read_text();bs,bd=base_payload(source)
        st,sd,ss,dd,meta=build_condition_views(base_name='c3',source_text=source,base_segments_json=bs,base_dag_json=bd)
        self.assertTrue(meta['scheduler_compatible'],meta['blocked_reasons'])
        self.assertEqual(2,len(ss['condition_regions']))


# C3 uses the existing hierarchical representation, not a new multiway node.
import shutil
import tempfile
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.pipeline.algo_registry import get_algo
from Code2DAG.tests.test_condition_nested import ALGOS,reachable,native_acceptance

LABELS=('branch_zero','branch_one','branch_other','after_condition')
PATHS={'ZERO':(True,False),'ONE':(False,True),'OTHER':(False,False)}
def c3_lines(source):
    return {label:next(i for i,l in enumerate(source.splitlines(),1) if 'puts("'+label+'")' in l) for label in LABELS}

def c3_structure(source,st,sd,ss,dd):
    regions=ss['condition_regions'];assert len(regions)==2
    outer=next(r for r in regions if r['parent_id'] is None);inner=next(r for r in regions if r['parent_id'])
    assert inner['parent_id']==outer['condition_id'] and inner['parent_branch_id']=='else'
    assert outer['condition_id']!=inner['condition_id'] and len(sd['exclusive_groups'])==len(dd['exclusive_groups'])==2
    assert ss['capabilities']['scheduler_compatible']
    for graph in (sd,dd):topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
    labels={}
    for name,line in c3_lines(source).items():
        found=[s for s in ss['segments'] if s['function']=='worker' and s['start_line']<=line<=s['end_line']];assert len(found)==1
        row=found[0];labels[name]=row['seg_id'];members={(m['exclusive_group'],m['branch_id']) for m in row.get('condition_memberships',[])}
        expected=set()
        if name=='branch_zero':expected={(outer['condition_id'],'then')}
        elif name!='after_condition':expected={(outer['condition_id'],'else'),(inner['condition_id'],'then' if name=='branch_one' else 'else')}
        assert members==expected,(name,members)
    assert len(set(labels.values()))==4
    controls={}
    for region,prefix in [(outer,'outer'),(inner,'inner')]:
        for kind,short in [('condition_entry','entry'),('condition_exit','exit')]:
            nodes=[s['seg_id'] for s in st['segments'] if s['kind']==kind and s['condition_id']==region['condition_id']];assert len(nodes)==1;controls[prefix+'_'+short]=nodes[0]
    oe,ox,ie,ix=[controls[k] for k in ['outer_entry','outer_exit','inner_entry','inner_exit']]
    pairs={(e['src'],e['dst']) for e in sd['edges']};assert (oe,ie) in pairs and (ix,ox) in pairs
    for edge in sd['edges']:
        if edge['src']==oe and edge['dst']==ie:assert edge['exclusive_group']==outer['condition_id'] and edge['branch_id']=='else'
    assert reachable(sd['edges'],ox,labels['after_condition'])
    for graph in (sd,dd):
        for a in LABELS[:3]:
            for b in LABELS[:3]:
                if a!=b:assert not reachable(graph['edges'],labels[a],labels[b])
    return {'outer':outer,'inner':inner,'labels':labels,'control_nodes':controls}

def c3_project(st,sd,structure,flags):
    outer,inner=structure['outer'],structure['inner'];choices={outer['condition_id']:'then' if flags[0] else 'else',inner['condition_id']:'then' if flags[1] else 'else'}
    active=set()
    for row in st['segments']:
        if row.get('synthetic'):
            if row['condition_id']==inner['condition_id'] and flags[0]:continue
            active.add(row['seg_id']);continue
        membership=[(r['condition_id'],b['branch_id']) for r in [outer,inner] for b in r['branches'] if b['start_line'] is not None and b['start_line']<=row['start_line']<=row['end_line']<=b['end_line']]
        if all(choices[g]==v for g,v in membership):active.add(row['seg_id'])
    edges=[e for e in sd['edges'] if e['src'] in active and e['dst'] in active];topo_sort(sorted(active),[(e['src'],e['dst']) for e in edges])
    chosen='branch_zero' if flags[0] else ('branch_one' if flags[1] else 'branch_other');labels=structure['labels'];c=structure['control_nodes']
    assert {n for n in LABELS[:3] if labels[n] in active}=={chosen}
    assert {e['src'] for e in edges if e['dst']==c['outer_exit']}=={labels['branch_zero'] if flags[0] else c['inner_exit']}
    if flags[0]:assert c['inner_entry'] not in active and c['inner_exit'] not in active
    else:assert {e['src'] for e in edges if e['dst']==c['inner_exit']}=={labels[chosen]}
    assert reachable(edges,labels[chosen],labels['after_condition'])
    return active,edges

def c3_output(flags):
    name='branch_zero' if flags[0] else ('branch_one' if flags[1] else 'branch_other');value=110 if flags[0] else (120 if flags[1] else 130)
    return '\n'.join(['main:start',name,'after_condition',f'value={value} outer_evaluations=1 inner_evaluations={int(not flags[0])}','main:end',''])

CASE={'paths':PATHS,'call_lines':c3_lines,'validate_structure':c3_structure,'project':c3_project,'expected_output':c3_output,
      'environment':lambda f:{'C2D_MODE':'0' if f[0] else ('1' if f[1] else '7')},
      'scope':'C3: one if, exactly one else-if, final else; three multiline braced bodies'}

class C3ClosureTests(unittest.TestCase):
    def test_hierarchical_views_global_exclusivity_and_five_algorithms(self):
        source=SOURCE_C3.read_text();bs,bd=base_payload(source);st,sd,ss,dd,_=build_condition_views(base_name='c3',source_text=source,base_segments_json=bs,base_dag_json=bd)
        structure=c3_structure(source,st,sd,ss,dd)
        for flags in PATHS.values():c3_project(st,sd,structure,flags)
        for algo in ALGOS:
            result=get_algo(algo).compute(segments_json=ss,dag_json=dd,timing_json={'weights':{n:{'avg_ns':1} for n in dd['nodes']}})
            self.assertEqual(set(dd['nodes']),set(result['priorities']))

    def test_actual_inline_business_code_still_rejected(self):
        original=SOURCE_C3.read_text()
        variants=[original.replace(') {\n        puts("branch_zero");',') { puts("branch_zero");',1),
                  original.replace(') {\n        puts("branch_one");',') { puts("branch_one");',1),
                  original.replace('    } else {\n        puts("branch_other");','    } else { puts("branch_other");',1)]
        for source in variants:
            bs,bd=base_payload(source);*_,meta=build_condition_views(base_name='c3_unsafe',source_text=source,base_segments_json=bs,base_dag_json=bd)
            self.assertFalse(meta['scheduler_compatible'])
            self.assertTrue(any('control boundary' in r for r in meta['blocked_reasons']))

    def test_longer_chain_does_not_receive_c3_header_exemption(self):
        source=SOURCE_C3.read_text().replace('    } else {\n        puts("branch_other");','    } else if (mode == 2) {\n        puts("not_supported");\n    } else {\n        puts("branch_other");',1)
        bs,bd=base_payload(source);*_,meta=build_condition_views(base_name='longer',source_text=source,base_segments_json=bs,base_dag_json=bd)
        self.assertFalse(meta['scheduler_compatible'])

    @unittest.skipUnless(shutil.which('gcc-13'),'Ubuntu GCC13 required')
    def test_native_c3_three_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            result=native_acceptance(Path(tmp)/'acceptance',source_file=SOURCE_C3,case=CASE)
            self.assertTrue(result['passed']);self.assertEqual(set(PATHS),set(result['paths']))

if __name__=='__main__':unittest.main()
