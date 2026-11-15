"""Regress only physical continuation inside _sanitize_c line_comment state."""
import copy
import hashlib
import re
import unittest
from Code2DAG.pipeline.condition_model import _sanitize_c, analyze_conditionals
from Code2DAG.pipeline.loop_model import build_loop_views, LoopModelError
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.pipeline.loop_nested_model import build_loop_nested_views
from Code2DAG.tests.test_loop_comment_directives import fixture as simple_fixture
from Code2DAG.tests.test_loop_l3 import fixture as nested_fixture, SOURCE
from Code2DAG.tests.test_condition_model import SIMPLE_SOURCE

BS=chr(92)
FAKE='for (int ghost=0; ghost<1; ghost++) { ghost++; }'


class ContinuedLineCommentTests(unittest.TestCase):
    def contract(self,text):
        frozen=text[:];digest=hashlib.sha256(text.encode()).hexdigest();clean=_sanitize_c(text)
        self.assertEqual(len(text),len(clean));self.assertEqual([i for i,c in enumerate(text) if c=='\n'],[i for i,c in enumerate(clean) if c=='\n']);self.assertEqual(frozen,text);self.assertEqual(digest,hashlib.sha256(text.encode()).hexdigest());return clean
    def test_01_LF_comment_fake_for_hidden(self):
        text='// docs '+BS+'\n'+FAKE+'\nint alive;\n';clean=self.contract(text);self.assertNotIn('ghost',clean);self.assertNotIn('for',clean);self.assertIn('int alive;',clean)
    def test_02_CRLF_splice_preserves_CR_and_LF(self):
        text='// docs '+BS+'\r\n'+FAKE+'\r\nint alive;\r\n';clean=self.contract(text);pos=text.index(BS);self.assertEqual(' \r\n',clean[pos:pos+3]);self.assertNotIn('for',clean);self.assertIn('int alive;',clean)
    def test_03_two_consecutive_splices(self):
        text='// docs '+BS+'\nfor ghost '+BS+'\nif ghost {}\nint alive;\n';clean=self.contract(text);self.assertNotIn('for',clean);self.assertNotIn('if',clean);self.assertNotIn('{}',clean);self.assertIn('int alive;',clean)
    def test_04_many_consecutive_splices(self):
        text='// docs '+(BS+'\nfor ghost ')*5+'\nint alive;\n';clean=self.contract(text);self.assertNotIn('ghost',clean);self.assertIn('int alive;',clean)
    def test_05_fake_if_braces_directives_stay_hidden(self):
        text='// docs '+BS+'\nif (ghost) {} '+BS+'\n#define HIDDEN for '+BS+'\n#pragma hidden {}\nint alive;\n';clean=self.contract(text)
        for token in ['if','ghost','{','}','#define','#pragma','for']:self.assertNotIn(token,clean)
        self.assertIn('int alive;',clean)
    def test_06_ordinary_LF_ends_comment_and_real_code_visible(self):
        text='// for hidden\nfor (int real=0; real<1; real++) {\n}\n';clean=self.contract(text);self.assertEqual(1,len(re.findall(r'\bfor\b',clean)));self.assertIn('real',clean)
    def test_07_space_or_tab_after_backslash_does_not_splice(self):
        for spacing in [' ','  ','\t']:
            text='// docs '+BS+spacing+'\nint visible;\n';clean=self.contract(text);self.assertIn('int visible;',clean)
    def test_08_last_of_multiple_backslashes_splices(self):
        for count in [2,3,4]:
            text='// docs '+BS*count+'\n'+FAKE+'\nint alive;\n';clean=self.contract(text);self.assertNotIn('for',clean);self.assertIn('int alive;',clean)
    def test_09_EOF_comment_without_final_LF(self):
        for text in ['int alive; // for hidden','// docs '+BS+'\n'+FAKE]:
            clean=self.contract(text);self.assertNotIn('for',clean)
            if text.startswith('int'):self.assertIn('int alive;',clean)
    def test_10_comment_end_does_not_hide_next_active_statement(self):
        text='// docs '+BS+'\nif (ghost) {}\nif (real) { alive(); }\n';clean=self.contract(text);self.assertNotIn('ghost',clean);self.assertIn('if (real)',clean)
    def test_11_Condition_only_real_if_is_detected(self):
        text=SIMPLE_SOURCE.replace('int main(void) {\n','int main(void) {\n    // example '+BS+'\n    if (ghost) { phantom(); } else { phantom2(); }\n',1)
        before=text[:];analysis=analyze_conditionals(text,{'main':(2,10)});self.assertEqual(1,len(analysis.regions));self.assertEqual((5,9),(analysis.regions[0].entry_line,analysis.regions[0].exit_line));self.assertEqual(before,text)
    def test_12_L1_fake_for_does_not_add_loop(self):
        text,s,d,e=simple_fixture('// example '+BS+'\n'+FAKE,two=False);v=build_loop_views(base_name='comment',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(1,len(v[2]['loops']));self.assertEqual(e['h1'],v[2]['loops'][0]['header_line']);self.assertEqual(3,len(v[3]['nodes']))
    def test_13_L2_fake_for_does_not_add_loop(self):
        text,s,d,e=simple_fixture('// example '+BS+'\n'+FAKE,two=True);v=build_loop_pair_views(base_name='comment',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual([e['h1'],e['h2']],[l['header_line'] for l in v[2]['loops']]);self.assertEqual(5,len(v[3]['nodes']))
    def test_14_L3_restores_physical_IDs_six_leaves_and_inputs(self):
        text,s,d=nested_fixture(text='// documentation example '+BS+'\n'+FAKE+'\n'+SOURCE.read_text());before=copy.deepcopy((text,s,d));v=build_loop_nested_views(base_name='comment',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(14,len(v[1]['nodes']));self.assertEqual(8,len(v[3]['nodes']));self.assertEqual([5,6],[l['header_line'] for l in v[2]['loops']]);expected=[f'LOOP:main@6:outer:{i}:iteration:{j}' for i in range(2) for j in range(3)];self.assertEqual(expected,v[2]['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:7']);self.assertEqual(before,(text,s,d))
    def test_15_real_active_third_loop_still_rejected(self):
        text=SOURCE.read_text().replace('    result += 10;','    for (int k=0; k<1; k++) {\n        result += k;\n    }\n    result += 10;');text,s,d=nested_fixture(text=text)
        with self.assertRaisesRegex(LoopModelError,'exactly two nested'):build_loop_nested_views(base_name='comment',source_text=text,base_segments_json=s,base_dag_json=d)
    def test_16_string_char_and_other_states_keep_existing_results(self):
        for quote,body in [('"','for if {}'),("'",'x')]:
            for newline in ['\n','\r\n']:
                literal=quote+BS+newline+body+quote;text=literal+';\nint alive;\n';expected=''.join(c if c in '\r\n' else ' ' for c in literal)+';\nint alive;\n';self.assertEqual(expected,self.contract(text))
        for text,expected in [('// doc\nint x;\n','      \nint x;\n'),('/* doc */int x;\n','         int x;\n'),('#define HIDDEN 1\nint x;\n','                \nint x;\n')]:self.assertEqual(expected,self.contract(text))
