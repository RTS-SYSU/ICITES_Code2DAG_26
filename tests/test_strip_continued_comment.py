"""Core line-comment splice contract and direct caller regressions only."""
import copy
from pathlib import Path
import tempfile
import unittest
from Code2DAG.core.func_ranges import _strip_comments_and_strings, extract_func_ranges
from Code2DAG.pipeline.loop_model import build_loop_views, LoopModelError
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.pipeline.loop_nested_model import build_loop_nested_views
from Code2DAG.pipeline.lock_model import analyze_lock_l1
from Code2DAG.tests.test_loop_comment_directives import fixture as simple_fixture
from Code2DAG.tests.test_loop_l3 import fixture as nested_fixture, SOURCE
from Code2DAG.tests.test_lock_l1 import SOURCE as LOCK_SOURCE
BS=chr(92)

class StripContinuedCommentTests(unittest.TestCase):
    def contract(self,text):
        before=text[:];clean=_strip_comments_and_strings(text)
        self.assertEqual(len(text),len(clean))
        self.assertEqual([i for i,c in enumerate(text) if c=='\n'],[i for i,c in enumerate(clean) if c=='\n'])
        self.assertEqual(before,text)
        return clean
    def test_LF_fake_directives_hidden(self):
        for token in ['#define HIDDEN 1','#pragma hidden','#if 1']:
            text='// doc '+BS+'\n'+token+'\nint alive;\n';clean=self.contract(text)
            self.assertNotIn('#',clean);self.assertIn('int alive;',clean)
    def test_CRLF_fake_directives_hidden(self):
        text='// doc '+BS+'\r\n#define HIDDEN 1\r\nint alive;\r\n'
        clean=self.contract(text);self.assertNotIn('#',clean);pos=text.index(BS);self.assertEqual(' \r\n',clean[pos:pos+3]);self.assertIn('int alive;',clean)
    def test_multiple_continuations(self):
        text='// doc '+BS+'\n#define HIDDEN '+BS+'\r\n#pragma hidden '+BS+'\n#if 1\nint alive;\n';clean=self.contract(text)
        self.assertNotIn('#',clean);self.assertIn('int alive;',clean)
    def test_ordinary_comment_end_exposes_active_directive(self):
        clean=self.contract('// doc '+BS+'\n#define HIDDEN 1\n#define ACTIVE 1\n')
        self.assertNotIn('HIDDEN',clean);self.assertIn('#define ACTIVE 1',clean)
    def test_whitespace_separation_not_continuation(self):
        for gap in [' ','\t','  ']:
            clean=self.contract('// doc '+BS+gap+'\n#define ACTIVE 1\n');self.assertIn('#define ACTIVE 1',clean)
    def test_multiple_backslashes_and_EOF(self):
        for count in [1,2,3]:
            clean=self.contract('// doc '+BS*count+'\n#define HIDDEN 1');self.assertNotIn('#',clean)
    def test_other_states_unchanged(self):
        for text,expected in [('/* x */ int a;\n','        int a;\n'),("\"abc\" 'x'\n",'         \n'),('// abc\nint a;\n','      \nint a;\n')]:
            self.assertEqual(expected,self.contract(text))
    def test_L1_and_L2_commented_directives_and_include(self):
        for two,build in [(False,build_loop_views),(True,build_loop_pair_views)]:
            for token in ['#define HIDDEN 1','#pragma hidden','#if 1']:
                text,s,d,e=simple_fixture('// doc '+BS+'\n'+token+'\n#include <stddef.h>',two=two);before=copy.deepcopy((text,s,d))
                v=build(base_name='strip',source_text=text,base_segments_json=s,base_dag_json=d)
                self.assertEqual(2 if two else 1,len(v[2]['loops']));self.assertEqual(before,(text,s,d))
    def test_L3_commented_directives_and_physical_mappings(self):
        for token in ['#define HIDDEN 1','#pragma hidden','#if 1']:
            text,s,d=nested_fixture(text='// doc '+BS+'\n'+token+'\n#include <stddef.h>\n'+SOURCE.read_text());before=copy.deepcopy((text,s,d))
            v=build_loop_nested_views(base_name='strip',source_text=text,base_segments_json=s,base_dag_json=d)
            self.assertEqual(14,len(v[1]['nodes']));self.assertEqual(8,len(v[3]['nodes']));self.assertEqual([6,7],[l['header_line'] for l in v[2]['loops']])
            self.assertEqual(6,len(v[2]['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:8']));self.assertEqual(before,(text,s,d))
    def test_active_directives_rejected_all_Loop_paths(self):
        for token in ['#define ACTIVE 1','#pragma active','#if 1']:
            prelude='// doc '+BS+'\n#define HIDDEN 1\n'+token
            for two,build in [(False,build_loop_views),(True,build_loop_pair_views)]:
                text,s,d,e=simple_fixture(prelude,two=two)
                with self.assertRaisesRegex(LoopModelError,'non-include directives'):build(base_name='strip',source_text=text,base_segments_json=s,base_dag_json=d)
            text,s,d=nested_fixture(text=prelude+'\n'+SOURCE.read_text())
            with self.assertRaisesRegex(LoopModelError,'non-include directives'):build_loop_nested_views(base_name='strip',source_text=text,base_segments_json=s,base_dag_json=d)
    def test_Lock_fake_tokens_hidden_real_regions_unchanged(self):
        text=LOCK_SOURCE.read_text();base=analyze_lock_l1(text)
        prelude='// documentation '+BS+'\n#define FAKE pthread_mutex_lock(&fake); '+BS+'\npthread_mutex_t fake; pthread_mutex_unlock(&fake);\n'
        source=prelude+text;before=source[:];mutex,regions=analyze_lock_l1(source)
        self.assertEqual(base[0],mutex);self.assertEqual(2,len(regions));self.assertEqual(before,source)
        for a,b in zip(base[1],regions):
            self.assertEqual(a['function'],b['function'])
            for key in ['lock_range','unlock_range','segment_range']:self.assertEqual([v+3 for v in a[key]],list(b[key]))
    def test_function_range_ignores_fake_definition_preserves_lines(self):
        text='// doc '+BS+'\nint target(void) { return 99; }\nint target(void) {\n    return 0;\n}\n'
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'input.c';p.write_text(text);before=p.read_bytes();rows=extract_func_ranges(p,['target']);self.assertEqual(before,p.read_bytes())
        self.assertEqual(1,len(rows));self.assertEqual((3,3,5,4),(rows[0].start_line,rows[0].body_start_line,rows[0].end_line,rows[0].last_return_line))
