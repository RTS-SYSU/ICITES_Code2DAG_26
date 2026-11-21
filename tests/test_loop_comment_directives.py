"""Only regress comment/literal versus active preprocessor directive detection."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from Code2DAG.pipeline.loop_model import build_loop_views,LoopModelError
from Code2DAG.pipeline.loop_pair_model import build_loop_pair_views
from Code2DAG.pipeline.runner import run_blocks


def fixture(prelude='',two=True,middle_note=''):
    lines=prelude.splitlines()+['int main(void) {','    int sum = 0;'];pre=len(lines)
    h1=len(lines)+1;lines+=['    for (int i=0; i<1; i++) {','        sum += i;','    }'];b1=h1+1
    mid=None;h2=None;b2=None
    if two:
        lines+=middle_note.splitlines();mid=len(lines)+1;lines+=['    sum += 10;'];h2=len(lines)+1;lines+=['    for (int j=0; j<1; j++) {','        sum += j;','    }'];b2=h2+1
    post=len(lines)+1;lines+=['    sum += 100;','    return 0;','}'];text='\n'.join(lines)+'\n'
    s=dict(segments=[dict(seg_id='OLD',function='main',kind='compute',start_line=pre,end_line=post)],node_locations={},node_to_segment={'main':'OLD'})
    return text,s,dict(nodes=['OLD'],edges=[]),dict(pre=pre,h1=h1,b1=b1,middle=mid,h2=h2,b2=b2,post=post)


class LoopCommentDirectiveTests(unittest.TestCase):
    def check_valid(self,prelude='',two=True,middle_note=''):
        text,s,d,e=fixture(prelude,two,middle_note);frozen=copy.deepcopy((text,s,d));builder=build_loop_pair_views if two else build_loop_views
        st,sd,ss,dd,_=builder(base_name='comment',source_text=text,base_segments_json=s,base_dag_json=d)
        a=f'LOOP:main@{e["h1"]}';expected=[a+':before',a+':iteration:0']
        if two:
            b=f'LOOP:main@{e["h2"]}';expected += [a+':after',b+':iteration:0',b+':after']
        else:expected += [a+':after']
        self.assertEqual(expected,dd['nodes']);self.assertEqual(set(zip(expected,expected[1:])),{(x['src'],x['dst']) for x in dd['edges']})
        self.assertEqual([e['h1']]+([e['h2']] if two else []),[l['header_line'] for l in ss['loops']])
        self.assertEqual([e['b1']]+([e['b2']] if two else []),[l['body_start'] for l in ss['loops']])
        m=ss['mappings'];self.assertEqual({'main':expected},m['generation_node_to_execution_segments']);self.assertEqual({'OLD':expected},m['original_segment_to_execution_segments'])
        desired={f'LOOPSRC:main:line:{e["pre"]}':[a+':before'],f'LOOPSRC:main:line:{e["b1"]}':[a+':iteration:0'],f'LOOPSRC:main:line:{e["post"]}':[expected[-1]]}
        if two:desired.update({f'LOOPSRC:main:line:{e["middle"]}':[a+':after'],f'LOOPSRC:main:line:{e["b2"]}':[b+':iteration:0']})
        self.assertEqual(desired,m['source_position_to_execution_segments']);self.assertEqual(frozen,(text,s,d))
        if two:
            middle=next(r for r in ss['segments'] if r['seg_id']==a+':after');self.assertEqual((e['middle'],e['middle']),(middle['start_line'],middle['end_line']))
        return ss

    def both(self,prelude):
        for two in [False,True]:
            with self.subTest(two=two):self.check_valid(prelude,two)

    def test_01_block_comment_define_L1_and_L2(self):
        self.both('/* documentation only:\n#define DOCUMENTED_FOR for\n*/')

    def test_02_line_comment_define_L1_and_L2(self):
        self.both('// #define COMMENT_ONLY 1\n// for (int ghost=0; ghost<2; ghost++) {}')

    def test_03_string_literal_and_escaped_quotes(self):
        self.both('static const char *text = "#define STRING_ONLY 1";\nstatic const char *escaped = "\\\"#pragma hidden\\\" \\\\";')

    def test_04_character_literals_and_escapes(self):
        self.both("static char hash = '#';\nstatic char slash = '\\\\';\nstatic char quote = '\\'';\nstatic char newline = '\\n';")

    def test_05_indented_if_and_pragma_in_block_comment(self):
        self.both('/* examples:\n   #if 0\n\t#pragma once\n   #endif\n*/')

    def test_06_active_directive_after_comment_must_reject(self):
        for prelude in ['/* example */\n#define ACTIVE 1','/* example */ #define ACTIVE 1']:
            for two in [False,True]:
                text,s,d,e=fixture(prelude,two);builder=build_loop_pair_views if two else build_loop_views
                with self.subTest(prelude=prelude,two=two):
                    with self.assertRaisesRegex(LoopModelError,'rejects macros and non-include directives'):builder(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)

    def test_07_real_active_noninclude_directives_still_reject(self):
        for prelude in ['#define ACTIVE 1','#if 1\n#endif','#ifdef ACTIVE\n#endif','#pragma once','#line 1']:
            for two in [False,True]:
                text,s,d,e=fixture(prelude,two);builder=build_loop_pair_views if two else build_loop_views
                with self.subTest(prelude=prelude,two=two):
                    with self.assertRaisesRegex(LoopModelError,'rejects macros and non-include directives'):builder(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)

    def test_08_active_include_remains_allowed(self):
        self.both('#include <stddef.h>\n#include <stdio.h>')

    def test_09_comments_in_middle_preserve_offsets_IDs_and_all_mappings(self):
        self.check_valid('/* top\n#define FAKE 1\n*/',True,'    /* middle docs:\n       #pragma hidden\n       #if EXAMPLE\n    */')

    def test_10_input_text_and_objects_unchanged(self):
        text,s,d,e=fixture('/* docs\n#define FAKE 1\n*/',True);before=copy.deepcopy((text,s,d));digest=hashlib.sha256(text.encode()).hexdigest();build_loop_pair_views(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d);self.assertEqual(before,(text,s,d));self.assertEqual(digest,hashlib.sha256(text.encode()).hexdigest())

    def test_11_reject_blocks_without_half_outputs_then_recover(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);pipeline=base/'intermediate_results/comment/pipeline';pipeline.mkdir(parents=True);(pipeline/'block_info.json').write_text('{}')
            bad=base/'active.c';text,s,d,e=fixture('#define REAL 1',True);bad.write_text(text);bad_before=bad.read_bytes();kwargs=dict(base_dir=base,base_name='comment',level='level2',rule_name='loop_static_pair')
            with self.assertRaisesRegex(LoopModelError,'rejects macros'):run_blocks(**kwargs,source_file=bad)
            self.assertFalse((pipeline/'blocks').exists());self.assertEqual(bad_before,bad.read_bytes())
            text,s,d,e=fixture('/* comment\n#define FAKE 1\n*/',True);good=base/'legal.c';good.write_text(text);s['source_file']=str(good)
            with patch('Code2DAG.pipeline.rules.loop_pair_rule.build_level2_segments_and_dag',return_value=(s,d)):run_blocks(**kwargs,source_file=good)
            paths=list((pipeline/'blocks').rglob('*'));prior={str(p.relative_to(pipeline)):p.read_bytes() for p in paths if p.is_file()}
            with self.assertRaisesRegex(LoopModelError,'rejects macros'):run_blocks(**kwargs,source_file=bad)
            self.assertEqual(prior,{str(p.relative_to(pipeline)):p.read_bytes() for p in (pipeline/'blocks').rglob('*') if p.is_file()})

    def test_12_same_line_block_comment_before_active_pragma(self):
        for two in [False,True]:
            text,s,d,e=fixture('/* harmless */ #pragma once',two);builder=build_loop_pair_views if two else build_loop_views
            with self.subTest(two=two):
                with self.assertRaisesRegex(LoopModelError,'rejects macros'):builder(base_name='x',source_text=text,base_segments_json=s,base_dag_json=d)
