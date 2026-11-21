"""Targeted regression for whitespace/newlines in the existing Loop L1 header."""
import unittest
from Code2DAG.pipeline.loop_model import analyze_static_loop,build_loop_views,LoopModelError


def fixture(header):
    lines=['int main(void) {','    int sum = 0;',*header.splitlines(),'        sum += i;','    }','    sum += 10;','    return 0;','}']
    body=3+len(header.splitlines());after=body+2
    segments={'segments':[{'seg_id':'OLD','function':'main','kind':'compute','start_line':2,'end_line':after}],'node_locations':{},'node_to_segment':{}}
    return '\n'.join(lines)+'\n',segments,{'nodes':['OLD'],'edges':[]},body,after


class LoopHeaderRegressionTests(unittest.TestCase):
    def check_header(self,header,n=3):
        text,s,d,body,after=fixture(header)
        st,sd,ss,dd,meta=build_loop_views(base_name='header',source_text=text,base_segments_json=s,base_dag_json=d)
        lid='LOOP:main@3';iterations=[lid+':iteration:'+str(i) for i in range(n)]
        execution=[lid+':before',*iterations,lid+':after'];structure=[lid+':before',lid+':entry',*iterations,lid+':exit',lid+':after']
        self.assertEqual(execution,dd['nodes']);self.assertEqual(set(zip(execution,execution[1:])),{(e['src'],e['dst']) for e in dd['edges']})
        self.assertEqual(set(zip(structure,structure[1:])),{(e['src'],e['dst']) for e in sd['edges']})
        rows=[r for r in ss['segments'] if r.get('role')=='iteration']
        self.assertEqual(list(range(n)),[r['iteration_index'] for r in rows]);self.assertTrue(all((r['start_line'],r['end_line'])==(body,body) and r['generation_origin'] is None for r in rows))
        post=next(r for r in ss['segments'] if r['role']=='after');self.assertEqual((after,after),(post['start_line'],post['end_line']))
        m=ss['mappings'];self.assertEqual({},m['generation_node_to_execution_segments']);self.assertEqual({'OLD':execution},m['original_segment_to_execution_segments'])
        self.assertEqual(iterations,m['source_position_to_execution_segments'][f'LOOPSRC:main:line:{body}'])
        self.assertEqual([lid+':after'],m['source_position_to_execution_segments'][f'LOOPSRC:main:line:{after}'])
        self.assertEqual(body+1,ss['loops'][0]['exit_line'])

    def test_three_line_header_brace_on_closing_paren_line(self):
        self.check_header('    for (int i = 0;\n         i < 3;\n         i++) {')

    def test_multiline_header_brace_on_next_line(self):
        self.check_header('    for (int i = 0;\n         i < 3;\n         i++)\n    {')

    def test_counts_maps_source_lines_and_successor_both_layouts(self):
        for n in [0,1,3,10]:
            for suffix in [' {','\n    {']:
                with self.subTest(n=n,suffix=suffix):self.check_header(f'    for (int i = 0;\n         i < {n};\n         i++)'+suffix,n)

    def test_multiline_strict_unsupported_headers_still_rejected(self):
        header='    for (int i = 0;\n         i < 3;\n         i++) {'
        for old,new in [('i = 0','i = 1'),('i < 3','i <= 3'),('i++','++i'),('i++','i += 1'),('i < 3','i < n'),('i < 3','i < (1 + 2)'),('i < 3','i < -1'),('i < 3','i < (f(), 3)')]:
            with self.subTest(new=new):
                with self.assertRaises(LoopModelError):analyze_static_loop(fixture(header.replace(old,new))[0])

    def test_comments_and_strings_containing_for_are_not_loops(self):
        for text in ['/* for (int i=0; i<3; i++) { } */','// for (int i=0; i<3; i++) { }\n','const char *s = "for (int i=0; i<3; i++) { }";']:
            with self.subTest(text=text):self.assertIsNone(analyze_static_loop(text))

    def test_comment_whitespace_and_blank_line_before_open_brace(self):
        self.check_header('    for /* header */ (int i = 0;\n         i < 3; /* comment */\n         i++)\n\n    /* between */\n    {')
