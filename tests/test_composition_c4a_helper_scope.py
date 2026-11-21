"""C4a models Conditions in ordinary helpers that own no schedulable old segment."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.tests import test_composition_c4a as c4a
from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.composition_c4a_model import RULE,build_loop_lock_views
from Code2DAG.pipeline.composition_c4a_timing import import_loop_lock_timing
from Code2DAG.pipeline.schedule import run_schedule
from Code2DAG.core.func_ranges import _strip_comments_and_strings,_find_definition_signature,_scan_to_body_open,_match_brace_block

HELPERS='''void observer_else(void) {
    int value = result;
    if (value % 2) {
        result += 11;
    } else {
        result += 17;
    }
}
void observer_noelse(void) {
    int value = result;
    if (value % 3) {
        result += 19;
    }
}
void observer_elseif(void) {
    int value = result;
    if (value % 2) {
        result += 23;
    } else if (value % 3) {
        result += 29;
    } else {
        result += 31;
    }
}
void observer_nested(void) {
    int value = result;
    if (value % 2) {
        if (value % 3) {
            result += 37;
        } else {
            result += 41;
        }
    } else {
        result += 43;
    }
}
void plain_helper(void) {
    int value = result;
    result += value;
}
void other_helper(void) {
    int value = result;
    if (value % 5) {
        result += 47;
    } else {
        result += 53;
    }
}
void uncalled_helper(void) {
    int value = result;
    if (value % 7) {
        result += 59;
    } else {
        result += 61;
    }
}
'''
CALLS='''    observer_else();
    observer_noelse();
    observer_elseif();
    observer_nested();
    plain_helper();
    other_helper();
'''
CONDITION_HELPERS=['observer_else','observer_noelse','observer_elseif','observer_nested','other_helper','uncalled_helper']


def _metadata(text,names):
    clean=_strip_comments_and_strings(text);rows=[]
    for name in names:
        found=_find_definition_signature(clean,name);assert found is not None,(name,text)
        opening=_scan_to_body_open(clean,found[1]);assert opening is not None,name
        closing=_match_brace_block(clean,opening);assert closing is not None,name
        start=clean.count('\n',0,found[0])+1;end=clean.count('\n',0,closing)+1;cursor=opening+1
        while cursor<closing and (clean[cursor].isspace() or clean[cursor] in '{}'):cursor+=1
        body_start=clean.count('\n',0,cursor)+1
        rows.append(dict(name=name,start_line=start,end_line=end,level1_start_line=body_start))
    return dict(source='helper_case.c',functions=rows,missing=[])


def fixture(root):
    text=(c4a.SOURCES/'many.c').read_text().replace('int main(void)',HELPERS+'int main(void)')
    text=text.replace('    pthread_join(t, NULL);','    pthread_join(t, NULL);\n'+CALLS)
    lines=text.splitlines();task=next(i for i,l in enumerate(lines,1) if l.startswith('void *task'))
    task_end=next(i for i in range(task+1,len(lines)+1) if lines[i-1]=='}')
    main=next(i for i,l in enumerate(lines,1) if l.startswith('int main'))
    rows=[dict(seg_id='OLD',function='task',kind='compute',start_line=task+1,end_line=task_end-1),
          dict(seg_id='MAIN',function='main',kind='compute',start_line=main+1,end_line=len(lines)-1)]
    locations={};mapping={}
    for fn in ['observer_else','observer_noelse','observer_elseif','observer_nested','plain_helper','other_helper']:
        call=next(i for i,l in enumerate(lines,1) if l.strip()==fn+'();')
        locations[fn]=dict(function='main',line=call);mapping[fn]='MAIN'
    source=Path(root)/'helper_case.c';source.write_text(text)
    meta=Path(root)/'functions_ranges.json';meta.write_text(json.dumps(_metadata(text,['task']+CONDITION_HELPERS+['plain_helper','main'])))
    raw=dict(base_name='helper_case',source_file=str(source),inputs={'functions_ranges':str(meta)},
             segments=rows,thread_entry_functions=['task'],node_locations=locations,node_to_segment=mapping)
    dag=dict(nodes=['OLD','MAIN'],edges=[dict(src='MAIN',dst='OLD',kind='create',custom={'keep':[1]})])
    return text,raw,dag


class HelperScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def build(self):
        text,raw,dag=fixture(self.root);before=copy.deepcopy((raw,dag))
        views=build_loop_lock_views(base_name='helper_case',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual(before,(raw,dag))
        return text,raw,dag,views

    def test_helper_forms_metadata_and_no_fake_execution(self):
        text,raw,dag,(st,sd,s,d,_)=self.build()
        regions={r['function']:r for r in s['condition_regions'] if r['function'] in CONDITION_HELPERS}
        self.assertEqual(set(CONDITION_HELPERS),set(regions))
        self.assertEqual({'observer_else','observer_noelse','observer_elseif','observer_nested','other_helper','uncalled_helper'},set(regions))
        self.assertNotIn('plain_helper',{r['function'] for r in s['condition_regions']})
        self.assertTrue(all(r['condition_id'].startswith('COND:'+fn+'#') for fn,r in regions.items()))
        else_if=[r for r in s['condition_regions'] if r['function']=='observer_elseif']
        self.assertEqual(2,len(else_if));outer=next(r for r in else_if if r['parent_id'] is None)
        child=next(r for r in else_if if r['parent_id'] is not None)
        self.assertEqual(outer['condition_id'],child['parent_id']);self.assertEqual('else',child['parent_branch_id'])
        nested=[r for r in s['condition_regions'] if r['function']=='observer_nested']
        self.assertEqual(2,len(nested));self.assertEqual('then',next(r for r in nested if r['parent_id'])['parent_branch_id'])
        self.assertIsNone(regions['observer_noelse']['branches'][1]['start_line'])
        ids={r['seg_id'] for r in st['segments']}
        for fn in CONDITION_HELPERS:
            self.assertIn('C4AHELPER:'+fn,{row['seg_id'] for row in s['source_only_segments']})
            self.assertTrue(any(r['seg_id'].startswith('CE:'+fn) for r in st['segments']))
            self.assertFalse(any(r['function']==fn and not r.get('source_only') for r in st['segments']))
            self.assertFalse(any(r['function']==fn for r in s['segments']))
        self.assertFalse(any(r.get('function')=='plain_helper' for r in st['segments']))
        self.assertFalse(any('plain_helper' in t.get('template_id','') for t in st['templates']))
        helper_rows=[r for r in st['segments'] if r.get('function') in CONDITION_HELPERS]
        self.assertTrue(helper_rows);self.assertTrue(all(r.get('source_only') and r.get('analysis_only') for r in helper_rows))
        for row in helper_rows:
            self.assertNotIn('template_id',row);self.assertNotIn('generation_origin',row)
        self.assertFalse(any(r.get('function') in CONDITION_HELPERS for r in s['segments']))
        self.assertFalse(any('C4AHELPER:' in json.dumps(t) for t in st['templates']))
        membership=[r['condition_memberships'] for r in helper_rows if len(r.get('condition_memberships',[]))==2]
        self.assertTrue(membership)
        self.assertTrue(any(r.get('kind')=='condition_entry' for r in st['segments'] if r.get('function') in CONDITION_HELPERS))
        groups=sd['exclusive_groups']
        for fn in CONDITION_HELPERS:
            self.assertTrue(any(k.startswith('COND:'+fn+'#') for k in groups))
        self.assertTrue(any(e['kind']=='condition_branch' and 'observer_else' in e['src'] for e in sd['edges']))
        self.assertIn('mappings',s);self.assertIn('mappings',st)
        # Formal source positions exist but never claim an execution target.
        line=next(i for i,l in enumerate(text.splitlines(),1) if 'result += 11;' in l)
        key='C4AHELPER:observer_else:line:'+str(line)
        self.assertEqual([],s['mappings']['source_position_to_execution_segments'][key])
        self.assertTrue(s['mappings']['source_position_to_structural_nodes'][key])
        self.assertTrue(s['mappings']['source_only_segment_to_structural_nodes']['C4AHELPER:observer_else'])
        self.assertNotIn('C4AHELPER:observer_else',s['mappings']['generation_node_to_execution_segments'])
        # Called helpers retain their call-site mapping; the uncalled helper is still modeled.
        for fn in ['observer_else','observer_noelse','observer_elseif','observer_nested','plain_helper','other_helper']:
            self.assertTrue(s['mappings']['generation_node_to_execution_segments'][fn])
            self.assertTrue(set(s['mappings']['generation_node_to_execution_segments'][fn])<=set(d['nodes']))
        self.assertNotIn('uncalled_helper',s['mappings']['generation_node_to_execution_segments'])
        self.assertEqual(dag['edges'],[e['edge'] for e in s['composition_edge_provenance']])
        self.assertEqual(['task'],s['thread_entry_functions'])

    def test_repeated_calls_are_deterministic_and_inputs_unchanged(self):
        text,raw,dag,(st,sd,s,d,_)=self.build();before=copy.deepcopy((raw,dag))
        again=build_loop_lock_views(base_name='helper_case',source_text=text,base_segments_json=raw,base_dag_json=dag)
        self.assertEqual((st,sd,s,d),again[:4]);self.assertEqual(before,(raw,dag))

    def test_native_observer_without_old_segment_timing_algorithms_and_output(self):
        text='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int result;
void *observer(void *arg) {
    if (result % 2) {
        result += 11;
    } else {
        result += 17;
    }
    return NULL;
}
void *task(void *arg) {
    result += 100;
    for (int i = 0; i < 0; i++) {
        result += 2;
        pthread_mutex_lock(&m);
        result += 1;
        pthread_mutex_unlock(&m);
        result += 3;
    }
    result += 1000;
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    observer(NULL);
    printf("RESULT:%d\\n", result);
    return 0;
}
'''
        with tempfile.TemporaryDirectory() as tmp:
            source_path=Path(tmp)/'observer_condition.c';source_path.write_text(text);before=source_path.read_bytes()
            base,p=c1.CompositionC1Tests().native(tmp,source_path)
            run_composition_blocks(base_dir=base,base_name=source_path.stem,source_file=source_path)
            root=p/'blocks/level2'/RULE
            st=json.loads((root/'segments_struct.json').read_text());sd=json.loads((root/'dag_struct.json').read_text())
            s=json.loads((root/'segments_sched.json').read_text());d=json.loads((root/'dag_sched.json').read_text())
            regions=[r for r in st['condition_regions'] if r['function']=='observer']
            self.assertEqual(1,len(regions));self.assertEqual(['then','else'],[b['branch_id'] for b in regions[0]['branches']])
            self.assertTrue(any(r['function']=='observer' and r.get('source_only') for r in st['segments']))
            self.assertFalse(any(r['function']=='observer' for r in s['segments']))
            self.assertFalse(any('C4AHELPER:' in json.dumps(t) for t in st['templates']))
            weights={t['template_id']:dict(total_ns=i+10,count=1,avg_ns=i+10,min_ns=i+10,max_ns=i+10) for i,t in enumerate(st['templates'])}
            timing=Path(tmp)/'weights.json';timing.write_text(json.dumps(dict(weights=weights)))
            result=import_loop_lock_timing(base_dir=base,base_name=source_path.stem,input_timing=timing)
            self.assertEqual({r['seg_id'] for r in s['segments']},set(result['weights']))
            for algo in c1.ALGOS:self.assertEqual(set(d['nodes']),set(run_schedule(base_dir=base,base_name=source_path.stem,level='level2',rule_name=RULE,algo_name=algo)['priorities']))
            binary=Path(tmp)/'program';c1.CompositionC1Tests().command(['gcc-13','-pthread',source_path,'-o',binary])
            self.assertEqual('RESULT:1117\n',c1.CompositionC1Tests().command([binary]).stdout)
            self.assertEqual(before,source_path.read_bytes())

    def test_native_uncalled_observer_is_still_modeled_without_call_mapping(self):
        text='''#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int result;
void *observer(void *arg) {
    if (result % 2) {
        result += 11;
    } else {
        result += 17;
    }
    return NULL;
}
void *task(void *arg) {
    result += 100;
    for (int i = 0; i < 0; i++) {
        result += 2;
        pthread_mutex_lock(&m);
        result += 1;
        pthread_mutex_unlock(&m);
        result += 3;
    }
    result += 1000;
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\\n", result);
    return 0;
}
'''
        with tempfile.TemporaryDirectory() as tmp:
            source_path=Path(tmp)/'uncalled_helper.c';source_path.write_text(text);before=source_path.read_bytes()
            base,p=c1.CompositionC1Tests().native(tmp,source_path)
            run_composition_blocks(base_dir=base,base_name=source_path.stem,source_file=source_path)
            root=p/'blocks/level2'/RULE
            st=json.loads((root/'segments_struct.json').read_text());s=json.loads((root/'segments_sched.json').read_text())
            self.assertEqual(['observer'],[r['function'] for r in st['condition_regions']])
            self.assertEqual(['C4AHELPER:observer'],[r['seg_id'] for r in st['source_only_segments']])
            self.assertFalse(any(r['function']=='observer' for r in s['segments']))
            self.assertNotIn('observer',s['mappings']['generation_node_to_execution_segments'])
            self.assertEqual(before,source_path.read_bytes())


if __name__=='__main__':unittest.main()
