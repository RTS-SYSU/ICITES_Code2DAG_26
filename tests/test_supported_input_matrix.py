"""Original pipeline: the shared syntax must work across all module families.

Generated variants are internal regression inputs, not external evidence.
No timing import, instrumentation or scheduler is run by these tests.
"""
import collections
import hashlib
import json
from pathlib import Path
import re
import tempfile
import unittest

from Code2DAG.pipeline.composition import select_rule, run_composition_blocks
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.tests import test_composition_c1 as c1


CASES = [
    ('effective_line_merge', None),
    ('conditional_branches', 'condition_c4/condition_c4.c'),
    ('lock_grouped', 'lock_l1/lock_l1.c'),
    ('loop_static_nested', 'loop_l1/loop_l1.c'),
    ('loop_static_nested', 'loop_l2/loop_l2.c'),
    ('loop_static_nested', 'loop_l3/loop_l3.c'),
    ('composition_c1', 'composition_c1/ckl.c'),
    ('composition_c2a', 'composition_c2a/then.c'),
    ('composition_c2b', 'composition_c2b/common.c'),
    ('composition_c3a', 'composition_c3a/l3.c'),
    ('composition_c3b', 'composition_c3b/l3.c'),
    ('composition_c4a', 'composition_c4a/l3.c'),
    ('composition_c4b', 'composition_c4b/l3.c'),
    ('composition_c5a', 'composition_c5a/l3.c'),
    ('composition_c5b', 'composition_c5b/l3.c'),
    ('composition_c5c', 'composition_c5c/l3.c'),
    ('composition_c5d', 'composition_c5d/l3.c'),
    ('composition_c5e', 'composition_c5e/l3.c'),
    ('composition_c5f', 'composition_c5f/l3.c'),
]

BASE_SOURCE = '''#include <pthread.h>
int value;
void *worker(void *arg) {
    value += 1;
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    return 0;
}
'''


def without_offsets(value):
    # Enum declarations/null spelling alter byte offsets, not physical lines.
    if isinstance(value,dict):
        offsets = {'start_offset','end_offset'}
        if {'brace','header','exit'} <= set(value):
            offsets |= {'start','brace','end'}
        return {k:without_offsets(v) for k,v in value.items()
                if k not in offsets}
    if isinstance(value,list):
        return [without_offsets(v) for v in value]
    return value


def variant(text):
    # Same number of lines, so graph source coordinates and IDs can be compared.
    declaration = 'typedef enum { INPUT_ZERO, INPUT_ONE } input_mode_t; '
    for name in ('mode', 'flag'):
        old = 'int '+name+';'
        if old in text:
            text = text.replace(old, declaration+'input_mode_t '+name+';', 1)
            break
    else:
        # Use an existing scalar where possible; this is still internal test C.
        text = text+'\n'+declaration+'input_mode_t input_mode;\n'
    text = re.sub(r'pthread_create\(([^,]+),\s*NULL,\s*(\w+),\s*NULL\)',
                  r'pthread_create(\1, 0, \2, 0)', text)
    text = re.sub(r'pthread_join\(([^,]+),\s*NULL\)', r'pthread_join(\1, 0)', text)
    return text


class SupportedInputMatrixTests(unittest.TestCase):
    def test_native_shared_syntax_all_families_and_loop_nesting(self):
        h = c1.CompositionC1Tests()
        with tempfile.TemporaryDirectory() as temp:
            for index,(rule,relative) in enumerate(CASES):
                with self.subTest(rule=rule,source=relative):
                    original = (c1.ROOT/'Code2DAG/source_files'/relative).read_text() if relative else BASE_SOURCE
                    views = []
                    for label,text in [('baseline',original),('extended',variant(original))]:
                        root = Path(temp)/str(index)/label
                        root.mkdir(parents=True)
                        source = root/(Path(relative).name if relative else 'base.c')
                        source.write_text(text)
                        frozen = hashlib.sha256(source.read_bytes()).hexdigest()
                        self.assertEqual(rule,select_rule(text))
                        base,pipeline = h.native(root,source)
                        run_composition_blocks(base_dir=base,base_name=source.stem,source_file=source)
                        block = pipeline/'blocks/level2'/rule
                        data = [json.loads((block/name).read_text()) for name in
                                ['segments_struct.json','dag_struct.json','segments_sched.json','dag_sched.json']]
                        for segments,graph in [(data[0],data[1]),(data[2],data[3])]:
                            ids = {row['seg_id'] for row in segments['segments']}
                            self.assertEqual(ids,set(graph['nodes']))
                            topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in graph['edges']])
                            for field in ['source_position_to_execution_segments',
                                          'generation_node_to_execution_segments',
                                          'original_segment_to_execution_segments']:
                                for targets in segments.get('mappings',{}).get(field,{}).values():
                                    # Both published views expose execution maps;
                                    # their targets belong to the scheduling view.
                                    self.assertTrue(set(targets)<=set(data[3]['nodes']))
                        self.assertEqual(frozen,hashlib.sha256(source.read_bytes()).hexdigest())
                        views.append(data)
                    # The extension must not alter the execution structure.
                    for graph_index in (1,3):
                        a,b = views[0][graph_index],views[1][graph_index]
                        self.assertEqual(a['nodes'],b['nodes'])
                        self.assertEqual(a['edges'],b['edges'])
                    for seg_index in (0,2):
                        a,b=views[0][seg_index],views[1][seg_index]
                        self.assertEqual(without_offsets(a['segments']),without_offsets(b['segments']))
                        for field in ['loops','condition_regions','lock_groups','mappings']:
                            self.assertEqual(without_offsets(a.get(field)),without_offsets(b.get(field)))


if __name__ == '__main__':
    unittest.main()
