"""C2c: Condition + independent Lock without static Loop."""
import copy,json,tempfile,unittest
from pathlib import Path
from Code2DAG.tests import test_composition_c1 as c1
from Code2DAG.pipeline.composition import select_rule,run_composition_blocks
from Code2DAG.pipeline.composition_c2c_model import RULE,build_condition_lock_views,CompositionError
from Code2DAG.pipeline.algo.common import topo_sort

ROOT=Path(__file__).resolve().parents[2]; SOURCE=ROOT/'Code2DAG/source_files/composition_c2c/benchmark.c'

class CompositionC2cTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp=tempfile.TemporaryDirectory(); cls.root=Path(cls._tmp.name)
        cls.text=SOURCE.read_text(); assert select_rule(cls.text)==RULE
        cls.h=c1.CompositionC1Tests(); base,p=cls.h.native(cls.root,SOURCE); cls.base=base; cls.p=p
        run_composition_blocks(base_dir=base,base_name=SOURCE.stem,source_file=SOURCE)
        b=p/'blocks/level2'/RULE
        cls.seg=json.loads((b/'segments_sched.json').read_text())
        cls.dag=json.loads((b/'dag_sched.json').read_text())
    @classmethod
    def tearDownClass(cls): cls._tmp.cleanup()

    def test_positive_structure(self):
        s=self.seg; d=self.dag
        self.assertEqual(RULE,s['rule_name']); self.assertEqual(0,len(s['loops']))
        self.assertEqual(5,len(s['condition_regions']))
        groups=s['lock_groups']; self.assertEqual(2,len(groups))
        self.assertEqual({'m1'},{g['mutex_id'] for g in groups})
        self.assertEqual({'task_5','task_7'},{g['members'][0]['function'] for g in groups})
        self.assertTrue(all(len(g['member_segment_ids'])==1 for g in groups))
        self.assertFalse(any('task_6' in json.dumps(g) for g in groups))
        creates=[e for e in d['edges'] if e.get('kind')=='create']
        self.assertEqual(7,len(creates))
        topo_sort(d['nodes'],[(e['src'],e['dst']) for e in d['edges']])
        self.assertEqual(set(d['nodes']),{r['seg_id'] for r in s['segments']})
        # source mapping completeness
        for r in s['segments']:
            self.assertTrue(r.get('source_segments') or r.get('generation_origin') or r.get('synthetic'))
        for e in d['edges']:
            self.assertTrue(e.get('provenance'))
        # 16 native paths, one branch per condition; no mutex branch cross-links.
        src=self.text.splitlines(); choices={}
        for mode in range(16):
            vals={'choose_outer':mode&1,'choose_inner':(mode>>1)&1,'choose_left':(mode>>2)&1,'choose_right':(mode>>3)&1}
            active=[]
            for r in s['segments']:
                ok=True
                for m in r.get('condition_memberships',[]):
                    cid=m['exclusive_group']
                    region=next(x for x in s['condition_regions'] if x['condition_id']==cid)
                    import re
                    var=re.search(r'choose_(\w+)\s*==\s*0',src[region['entry_line']-1]).group(1)
                    want='then' if vals['choose_'+var]==0 else 'else'
                    if m['branch_id']!=want:ok=False;break
                if ok:active.append(r['seg_id'])
            self.assertEqual(25,len(active))
            self.assertEqual(len(set(active)),len(active))
        # then/else branch nodes never mutually reachable
        cn=[r for r in s['segments'] if r.get('condition_memberships')]
        for i,a in enumerate(cn):
            for b in cn[i+1:]:
                ma={m['exclusive_group']:m['branch_id'] for m in a['condition_memberships']}
                mb={m['exclusive_group']:m['branch_id'] for m in b['condition_memberships']}
                if any(k in mb and ma[k]!=mb[k] for k in ma):
                    def reach(x,y):
                        st=[x];seen=set()
                        while st:
                            n=st.pop()
                            if n==y:return True
                            if n not in seen:
                                seen.add(n);st.extend(e['dst'] for e in d['edges'] if e['src']==n)
                        return False
                    self.assertFalse(reach(a['seg_id'],b['seg_id']))
                    self.assertFalse(reach(b['seg_id'],a['seg_id']))

    def test_unpaired_lock_rejected(self):
        bad=self.text.replace('    pthread_mutex_unlock(&m1);\n    burn(1965096);','    burn(1965096);',1)
        self.assertIn('pthread_mutex_lock(&m1)',bad)
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'bad.c';src.write_text(bad)
            base,p=self.h.native(Path(tmp),src)
            with self.assertRaises((CompositionError,ValueError)):
                run_composition_blocks(base_dir=base,base_name='bad',source_file=src)

    def test_nested_lock_rejected(self):
        bad=self.text.replace('    pthread_mutex_lock(&m1);\n    burn(205810);\n    pthread_mutex_unlock(&m1);','    pthread_mutex_lock(&m1);\n    pthread_mutex_lock(&m1);\n    burn(205810);\n    pthread_mutex_unlock(&m1);\n    pthread_mutex_unlock(&m1);',1)
        self.assertGreaterEqual(bad.count('pthread_mutex_lock'),3)
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'bad.c';src.write_text(bad)
            base,p=self.h.native(Path(tmp),src)
            with self.assertRaises((CompositionError,ValueError)):
                run_composition_blocks(base_dir=base,base_name='bad',source_file=src)

    def test_other_shapes_not_routed_to_c2c(self):
        for rel in ['composition_c2a/then.c','composition_c5b/then.c']:
            p=ROOT/'Code2DAG/source_files'/rel
            if p.exists(): self.assertNotEqual(RULE,select_rule(p.read_text()),rel)

if __name__=='__main__': unittest.main()
