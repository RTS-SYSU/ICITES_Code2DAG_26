"""Only regress ambiguous product/declaration discrimination in Loop L3."""
import copy
import unittest
from Code2DAG.pipeline.loop_model import LoopModelError
from Code2DAG.pipeline.loop_nested_model import build_loop_nested_views
from Code2DAG.tests.test_loop_l3 import fixture, SOURCE


class LoopInductionProductTests(unittest.TestCase):
    def accept(self,statement):
        text,s,d=fixture(text=SOURCE.read_text().replace('result += i + j;',statement));before=copy.deepcopy((text,s,d))
        st,sd,ss,dd,_=build_loop_nested_views(base_name='product',source_text=text,base_segments_json=s,base_dag_json=d)
        self.assertEqual(14,len(sd['nodes']));self.assertEqual(8,len(dd['nodes']))
        expected=[f'LOOP:main@4:outer:{i}:iteration:{j}' for i in range(2) for j in range(3)]
        self.assertEqual(expected,[r['seg_id'] for r in ss['segments'] if r['role']=='iteration'])
        self.assertEqual(expected,ss['mappings']['source_position_to_execution_segments']['LOOPSRC:main:line:5'])
        self.assertEqual(set(zip(dd['nodes'],dd['nodes'][1:])),{(e['src'],e['dst']) for e in dd['edges']})
        self.assertEqual(before,(text,s,d))
    def reject(self,statement):
        text,s,d=fixture(text=SOURCE.read_text().replace('result += i + j;',statement))
        with self.assertRaisesRegex(LoopModelError,'induction variable'):
            build_loop_nested_views(base_name='product',source_text=text,base_segments_json=s,base_dag_json=d)
    def test_01_plain_multiplication_reads(self):
        for statement in ['result * i;','result * j;','result * i * j;']:
            with self.subTest(statement=statement):self.accept(statement)
    def test_02_assignment_and_parenthesized_reads(self):
        for statement in ['result = result * i;','result = result * j;','result * (i + j);','(result * i) * j;','result * ((i));']:
            with self.subTest(statement=statement):self.accept(statement)
    def test_03_builtin_direct_pointer_and_comma_declarations_still_reject(self):
        for statement in ['int i;','int *i;','int i, j;','int unused, j;','int * j;']:
            with self.subTest(statement=statement):self.reject(statement)
    def test_04_explicit_struct_union_enum_declarations_still_reject(self):
        for statement in ['struct S *i;','union U *j;','enum E i;']:
            with self.subTest(statement=statement):self.reject(statement)
    def test_05_unambiguous_direct_typename_still_rejects(self):
        for statement in ['TypeName i;','TypeName j;']:
            with self.subTest(statement=statement):self.reject(statement)
    def test_06_assignment_increment_decrement_compound_and_address_checks_remain(self):
        for name in ['i','j']:
            for statement in [name+'=1;',name+'+=1;','++'+name+';',name+'++;','--'+name+';',name+'--;','escape(&'+name+');','escape(&('+name+'));']:
                with self.subTest(statement=statement):self.reject(statement)
    def test_07_whitespace_between_product_tokens_is_not_a_type_signal(self):
        for statement in ['result\t*\ti;','result   *   j;','result * i*j;']:
            with self.subTest(statement=statement):self.accept(statement)
