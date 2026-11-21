"""Import Loop L1 template timing without compiling, instrumenting or running C."""
import argparse
import hashlib
import json
from pathlib import Path
from .io_utils import read_json, write_json
from .loop_model import copy_loop_timing, LoopModelError


def _read_loop_timing_json(path):
    """Reject duplicate keys at every nesting level before publishing timing."""
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise LoopModelError(f'duplicate key: {key}')
            result[key] = value
        return result

    try:
        return json.loads(Path(path).read_text(encoding='utf-8'), object_pairs_hook=unique_object)
    except json.JSONDecodeError as exc:
        raise LoopModelError(f'invalid Loop timing JSON: {exc}') from exc


def _validate_loop_segments(payload):
    """Check existing Loop template bindings without changing input or weights."""
    rows = payload.get('segments', [])

    def fail(reason, row=None, template=None):
        row = row or {}
        template = template or {}
        raise LoopModelError(
            f"Loop segments conflict: node={row.get('seg_id', '<metadata>')} "
            f"role={row.get('role', template.get('role', '<metadata>'))} "
            f"template={row.get('template_id', template.get('template_id', '<none>'))}: {reason}")

    def template_fail(reason, template):
        row = next((r for r in rows if r.get('template_id') == template.get('template_id')), None)
        fail(reason, row, template)

    def bounds(item, first='start_line', last='end_line'):
        a, b = item.get(first), item.get(last)
        return (a, b) if type(a) is int and type(b) is int and 1 <= a <= b else None

    loops = {}
    for loop in payload.get('loops', []):
        lid = loop.get('loop_id')
        if not isinstance(lid, str) or not lid:
            fail('missing loop_id')
        if lid in loops:
            fail(f'duplicate loop_id: {lid}')
        if bounds(loop, 'body_start', 'body_end') is None:
            fail(f'invalid loop body range: {lid}')
        loops[lid] = loop

    templates = {}
    for template in payload.get('templates', []):
        tid = template.get('template_id')
        if not isinstance(tid, str) or not tid:
            template_fail('empty template_id', template)
        if tid in templates:
            template_fail('duplicate template_id: ' + tid, template)
        # Existing templates have no loop_id field: their IDs already use loop_id:role.
        owner, _, suffix = tid.rpartition(':')
        role = template.get('role')
        if owner not in loops or role not in ('before', 'body_template', 'after') or suffix != role:
            template_fail('unknown template loop or role', template)
        if template.get('loop_id', owner) != owner:
            template_fail('template loop_id conflicts with its existing ID', template)
        if bounds(template) is None:
            template_fail('invalid template source range', template)
        if role == 'body_template' and bounds(template) != bounds(loops[owner], 'body_start', 'body_end'):
            template_fail('body template range conflicts with loop body', template)
        templates[tid] = (owner, template)

    for lid in loops:
        if lid + ':body_template' not in templates:
            fail(f'missing body_template for loop {lid}')

    for row in rows:
        lid, role, tid = row.get('loop_id'), row.get('role'), row.get('template_id')
        # Untouched ordinary segments have no Loop binding and retain existing timing behavior.
        if lid is None and role is None and tid is None:
            continue
        if lid not in loops or role not in ('iteration', 'before', 'after'):
            fail('unknown execution loop or role', row)
        if not isinstance(tid, str) or not tid or tid not in templates:
            fail('empty or nonexistent template reference', row)
        owner, template = templates[tid]
        if owner != lid:
            fail('cross-loop template reference', row)
        expected_role = 'body_template' if role == 'iteration' else role
        if template['role'] != expected_role:
            fail('execution role conflicts with template role', row)
        if bounds(row) is None or bounds(row) != bounds(template):
            fail('execution source range conflicts with template range', row)


def import_loop_timing(*,base_dir,base_name,input_timing):
    root=Path(base_dir)/'intermediate_results'/base_name/'pipeline'
    segments=read_json(root/'blocks/level2/loop_static/segments.json')
    source=Path(segments['source_file'])
    if hashlib.sha256(source.read_bytes()).hexdigest()!=segments['source_sha256']:
        raise LoopModelError('source changed since loop blocks generation')
    _validate_loop_segments(segments)
    result=copy_loop_timing(segments,_read_loop_timing_json(input_timing))
    result['input_timing_sha256']=hashlib.sha256(Path(input_timing).read_bytes()).hexdigest()
    write_json(root/'timing/level2/loop_static/timing.json',result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--base-name',required=True);p.add_argument('--input-timing',type=Path,required=True)
    print(json.dumps(import_loop_timing(**vars(p.parse_args())),indent=2))


if __name__=='__main__':main()
