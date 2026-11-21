"""Physical source coordinates must survive literal sanitization."""
import unittest
from Code2DAG.pipeline.condition_model import _sanitize_c
from Code2DAG.pipeline.loop_model import build_loop_views, LoopModelError
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.tests.test_loop_comment_directives import fixture

BS = chr(92)
class LiteralLineContractTests(unittest.TestCase):
    def contract(self,text):
        before=text[:];clean=_sanitize_c(text)
        self.assertEqual(len(text),len(clean))
        self.assertEqual([i for i,c in enumerate(text) if c=='\n'],[i for i,c in enumerate(clean) if c=='\n'])
        self.assertEqual(before,text)
        return clean
    def literal(self,quote='"',newline='\n',count=1):
        text=quote+('hidden'+BS+newline)*count+'#define for if {}'+quote+'; int visible;\n'
        clean=self.contract(text)
        self.assertNotIn('hidden',clean);self.assertNotIn('#define',clean);self.assertNotIn('for',clean);self.assertNotIn('if',clean);self.assertNotIn('{}',clean)
        self.assertIn('int visible;',clean)
    def test_01_string_LF(self):self.literal()
    def test_02_char_LF(self):self.literal("'")
    def test_03_string_CRLF(self):self.literal(newline='\r\n')
    def test_04_char_CRLF(self):self.literal("'",'\r\n')
    def test_05_two_continuations(self):self.literal(count=2)
    def test_06_hidden_active_looking_tokens(self):self.literal(count=3)
    def test_07_ordinary_escapes(self):
        for quote in ['"',"'"]:
            for escaped in [BS,'"',"'",'n']:
                text=quote+BS+escaped+quote+';\n'
                self.assertEqual(' '*(len(text)-2)+';\n',self.contract(text))
    def test_08_comments_preserve_offsets(self):
        text='// for if\n/* #define\n{} */ int x;\n'
        self.assertEqual(' '*9+'\n'+' '*10+'\n'+' '*6+'int x;\n',self.contract(text))
    def check_loop(self,two,prelude):
        text,s,d,e=fixture(prelude,two);builder=build_loop_pair_views if two else build_loop_views
        v=builder(base_name='literal',source_text=text,base_segments_json=s,base_dag_json=d);ss=v[2];a='LOOP:main@'+str(e['h1']);ids=[a+':before',a+':iteration:0']
        if two:
            b='LOOP:main@'+str(e['h2']);ids += [a+':after',b+':iteration:0',b+':after']
        else:ids += [a+':after']
        self.assertEqual(ids,v[3]['nodes']);self.assertEqual(set(zip(ids,ids[1:])),{(x['src'],x['dst']) for x in v[3]['edges']})
        self.assertEqual([e['h1']]+([e['h2']] if two else []),[x['header_line'] for x in ss['loops']])
        self.assertEqual([e['b1']]+([e['b2']] if two else []),[x['body_start'] for x in ss['loops']])
        m=ss['mappings'];self.assertEqual({'main':ids},m['generation_node_to_execution_segments']);self.assertEqual({'OLD':ids},m['original_segment_to_execution_segments'])
        positions={e['pre']:ids[0],e['b1']:ids[1],e['post']:ids[-1]}
        if two:positions.update({e['middle']:ids[2],e['b2']:ids[3]})
        self.assertEqual({f'LOOPSRC:main:line:{n}':[node] for n,node in positions.items()},m['source_position_to_execution_segments'])
    def test_09_L1_prefix_and_coordinates(self):self.check_loop(False,'static char *s="a'+BS+'\n'+'#define hidden";')
    def test_10_L2_five_segments_and_coordinates(self):self.check_loop(True,'static char *s="a'+BS+'\n'+'#define hidden";')
    def test_11_L1_char_continuation(self):self.check_loop(False,"static char c='"+BS+'\n'+"x';")
    def test_12_L2_multiple_continuations(self):self.check_loop(True,'static char *s="a'+BS+'\n'+'b'+BS+'\n'+'for if {}";')
    def test_13_ordinary_Condition_sanitization_unchanged(self):
        self.assertEqual('if (flag) { work(); } else { other(); }\n',self.contract('if (flag) { work(); } else { other(); }\n'))
        self.assertEqual('char c =    ; char *s =       ;\n',self.contract("char c = 'x'; char *s = \"text\";\n"))
    def test_14_directive_guard_still_strict(self):
        self.check_loop(True,'/*\n#define hidden\n*/')
        text,s,d,e=fixture('static char *s="a'+BS+'\n'+'hidden";\n#define ACTIVE 1',True)
        with self.assertRaisesRegex(LoopModelError,'rejects macros'):
            build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
