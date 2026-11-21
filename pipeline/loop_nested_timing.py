"""Loop L3 explicit-template timing, with local two-dimensional binding checks."""
import argparse
import hashlib
import json
from pathlib import Path

from .io_utils import write_json
from .loop_model import copy_loop_timing, LoopModelError
from .loop_timing import _read_loop_timing_json
from .loop_nested_model import MAX_STRUCTURAL_NODES


def _validate_nested_segments(payload):
    def fail(reason, row=None):
        row = row or {}
        raise LoopModelError(f"Loop L3 segments conflict: node={row.get('seg_id', '<metadata>')} "
                             f"role={row.get('role', '<metadata>')} template={row.get('template_id', '<none>')} "
                             f"coordinate=({row.get('outer_iteration_index')},{row.get('inner_iteration_index')}): {reason}")
    def bounds(row, first='start_line', last='end_line'):
        a,b = row.get(first),row.get(last)
        return (a,b) if type(a) is int and type(b) is int and 1 <= a <= b else None
    if not isinstance(payload, dict) or payload.get('rule_name') != 'loop_static_nested':
        fail('expected loop_static_nested segments')
    loops = payload.get('loops')
    if not isinstance(loops, list) or len(loops) != 2 or not all(isinstance(l,dict) for l in loops):
        fail('expected exactly two loop metadata rows')
    outer,inner = loops;oid,iid = outer.get('loop_id'),inner.get('loop_id')
    if not isinstance(oid,str) or not isinstance(iid,str) or not oid or not iid or oid == iid:
        fail('missing or duplicate loop_id')
    fn = outer.get('function')
    if (not isinstance(fn,str) or not fn or inner.get('function') != fn or
        type(outer.get('depth')) is not int or type(inner.get('depth')) is not int or
        outer.get('depth') != 1 or inner.get('depth') != 2 or
        outer.get('parent_loop_id') is not None or inner.get('parent_loop_id') != oid):
        fail('invalid outer/inner parent or depth')
    if (not isinstance(outer.get('variable'),str) or not isinstance(inner.get('variable'),str) or
        not outer['variable'] or not inner['variable'] or outer['variable'] == inner['variable']):
        fail('invalid or identical induction variables')
    for loop in loops:
        if (type(loop.get('iteration_count')) is not int or not 0 <= loop['iteration_count'] <= 2147483647 or
            bounds(loop,'body_start','body_end') is None or bounds(loop,'header_line','exit_line') is None or
            loop['loop_id'] != f"LOOP:{fn}@{loop['header_line']}"):
            fail('invalid loop count, identity or source range')
    if (outer.get('original_segment') != inner.get('original_segment') or not outer.get('original_segment') or
        not outer['header_line'] < inner['header_line'] < inner['body_start'] <= inner['body_end'] < inner['exit_line'] < outer['exit_line'] or
        (outer['body_start'],outer['body_end']) != (inner['header_line'],inner['exit_line'])):
        fail('conflicting nested loop source ranges or original owner')
    rows,templates = payload.get('segments'),payload.get('templates')
    if not isinstance(rows,list) or not all(isinstance(r,dict) for r in rows) or not isinstance(templates,list) or not all(isinstance(t,dict) for t in templates):
        fail('invalid execution/template rows')
    ids = [r.get('seg_id') for r in rows]
    if any(not isinstance(n,str) or not n for n in ids) or len(ids) != len(set(ids)):
        fail('empty or duplicate node ID')
    by_template = {}
    allowed = {oid+':before':(oid,'before'),iid+':body_template':(iid,'body_template'),oid+':after':(oid,'after')}
    for template in templates:
        tid = template.get('template_id')
        if not isinstance(tid,str) or not tid or tid in by_template:
            fail('empty or duplicate template_id',template)
        if tid not in allowed or (template.get('loop_id'),template.get('role')) != allowed[tid]:
            fail('cross-layer or unknown template binding',template)
        if bounds(template) is None:
            fail('invalid template source range',template)
        if tid == iid+':body_template' and bounds(template) != bounds(inner,'body_start','body_end'):
            fail('body template source range conflicts with inner loop',template)
        if tid == oid+':before' and template['end_line'] >= outer['header_line']:
            fail('before template crosses outer boundary',template)
        if tid == oid+':after' and template['start_line'] <= outer['exit_line']:
            fail('after template crosses outer boundary',template)
        by_template[tid] = template
    if iid+':body_template' not in by_template:
        fail('missing inner body_template')
    n1,n2 = outer['iteration_count'],inner['iteration_count']
    estimated = n1*n2+2+2*n1+int(oid+':before' in by_template)+int(oid+':after' in by_template)
    if estimated + sum(r.get('loop_id') is None for r in rows) > MAX_STRUCTURAL_NODES:
        fail('structural node limit exceeded')
    coordinates = set();leaf_order = [];bound_order = [];role_counts = {'before':0,'after':0}
    for row in rows:
        lid,role,tid = row.get('loop_id'),row.get('role'),row.get('template_id')
        if lid is None and role is None and tid is None:
            continue
        if role not in ('iteration','before','after') or tid not in by_template:
            fail('unknown role or empty/nonexistent template reference',row)
        if row.get('kind') != 'compute' or row.get('function') != fn:
            fail('invalid execution kind or function',row)
        bound_order.append(row['seg_id']);template = by_template[tid]
        if role == 'iteration':
            i,j = row.get('outer_iteration_index'),row.get('inner_iteration_index')
            if type(i) is not int or type(j) is not int or not 0 <= i < n1 or not 0 <= j < n2:
                fail('missing or out-of-range two-dimensional index',row)
            if (i,j) in coordinates:
                fail('duplicate two-dimensional coordinate',row)
            coordinates.add((i,j));leaf_order.append((i,j))
            if type(row.get('iteration_index')) is not int or row['iteration_index'] != j:
                fail('iteration_index conflicts with inner index',row)
            if lid != iid or tid != iid+':body_template' or row.get('parent_loop_id') != oid:
                fail('cross-layer iteration template binding',row)
            if row['seg_id'] != f'{iid}:outer:{i}:iteration:{j}':
                fail('node identity conflicts with two-dimensional coordinate',row)
        else:
            role_counts[role] += 1
            if lid != oid or tid != oid+':'+role or row['seg_id'] != tid or any(row.get(k) is not None for k in ('outer_iteration_index','inner_iteration_index','iteration_index','parent_loop_id')):
                fail('cross-layer prefix/suffix template binding',row)
        if bounds(row) is None or bounds(row) != bounds(template):
            fail('execution source range conflicts with template',row)
    if len(coordinates) != n1*n2:
        fail('missing coordinate: incomplete Cartesian product')
    if leaf_order != sorted(coordinates):
        fail('iteration rows not in lexicographic order')
    for role,count in role_counts.items():
        if count != int(oid+':'+role in by_template):
            fail('missing or repeated '+role+' execution')
    expected = ([oid+':before'] if role_counts['before'] else [])+[f'{iid}:outer:{i}:iteration:{j}' for i,j in leaf_order]+([oid+':after'] if role_counts['after'] else [])
    if bound_order != expected:
        fail('prefix/leaf/suffix execution order conflict')
    mappings = payload.get('mappings',{})
    if mappings.get('original_segment_to_execution_segments',{}).get(outer['original_segment']) != expected:
        fail('original owner mapping conflicts with execution coverage')
    body_ids = [f'{iid}:outer:{i}:iteration:{j}' for i,j in leaf_order]
    statements = inner.get('statements')
    if not isinstance(statements,list) or not statements:
        fail('missing ordinary body source positions')
    spans = []
    for pair in statements:
        if not isinstance(pair,(list,tuple)) or len(pair) != 2 or type(pair[0]) is not int or type(pair[1]) is not int or not inner['body_start'] <= pair[0] <= pair[1] <= inner['body_end']:
            fail('invalid body statement source range')
        if spans and pair[0] <= spans[-1][1]:
            fail('overlapping body statement source ranges')
        spans.append(pair)
    if spans[0][0] != inner['body_start'] or spans[-1][1] != inner['body_end']:
        fail('statement ranges conflict with inner body')
    positions = mappings.get('source_position_to_execution_segments',{})
    nodes_by_id = {row['seg_id']: row for row in rows}
    body_id_set = set(body_ids)
    for a,b in spans:
        for line in range(a,b+1):
            targets = positions.get(f'LOOPSRC:{fn}:line:{line}')
            if not isinstance(targets, list) or any(not isinstance(target, str) or not target for target in targets):
                fail('body source mapping conflicts: targets must be a list of node IDs')
            if len(targets) != len(set(targets)):
                fail('body source mapping conflicts: duplicate target ID')
            leaves = [target for target in targets if target in body_id_set]
            extras = [target for target in targets if target not in body_id_set]
            if leaves != body_ids:
                fail('body source mapping conflicts with Cartesian coverage')
            if targets != extras + body_ids:
                fail('body source mapping conflicts: extra targets must precede all leaves')
            for target in extras:
                row = nodes_by_id.get(target)
                control_id = target in (oid+':entry', oid+':exit') or (target.startswith(iid+':outer:') and target.rsplit(':',1)[-1] in ('entry','exit'))
                if (row is None or row.get('kind') != 'compute' or row.get('loop_id') is not None or
                    target == outer['original_segment'] or control_id):
                    fail('body source mapping conflicts: extra target is unknown, stale, control or loop-owned: '+target)


def import_loop_nested_timing(*, base_dir, base_name, input_timing):
    root = Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    payload = _read_loop_timing_json(root/'blocks/level2/loop_static_nested/segments.json')
    _validate_nested_segments(payload)
    source = Path(payload['source_file'])
    if hashlib.sha256(source.read_bytes()).hexdigest() != payload['source_sha256']:
        raise LoopModelError('source changed since nested loop blocks generation')
    result = copy_loop_timing(payload, _read_loop_timing_json(input_timing))
    result['input_timing_sha256'] = hashlib.sha256(Path(input_timing).read_bytes()).hexdigest()
    write_json(root/'timing/level2/loop_static_nested/timing.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-dir',type=Path,required=True);parser.add_argument('--base-name',required=True)
    parser.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_loop_nested_timing(**vars(parser.parse_args())),indent=2))


if __name__ == '__main__': main()
