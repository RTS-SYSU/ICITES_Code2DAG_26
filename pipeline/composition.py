"""Opt-in C1 entry. Standalone inputs use the original publisher and rule name."""
import argparse
import json
from pathlib import Path
from .composition_model import features, RULE


def select_rule(text):
    # Compatibility views are selected before the general feature dispatch.
    # They are still built from build_level2_segments_and_dag(); they only
    # expand or reconnect the base segments for shapes the base builder keeps
    # as one source execution (static thread arrays, helper mutex pointers).
    from .compatibility_thread_array_model import is_thread_array_case
    if is_thread_array_case(text):
        return 'compatibility_thread_array'
    from .compatibility_helper_mutex_model import is_helper_mutex_case
    if is_helper_mutex_case(text):
        return 'compatibility_helper_mutex'
    from .compatibility_error_checked_model import is_error_checked_sync_case
    if is_error_checked_sync_case(text):
        return 'compatibility_error_checked_sync'
    kinds = features(text)
    if not kinds: return 'effective_line_merge'
    if len(kinds) == 1:
        return {'condition':'conditional_branches','lock':'lock_grouped','loop':'loop_static_nested'}[next(iter(kinds))]
    if {'lock','loop'} <= kinds:
        if 'condition' in kinds:
            # C5f: the outer Lock contains a static Loop whose body Conditions
            # are instantiated per iteration.
            from .composition_c5f_model import lock_loop_condition_overlap as c5f_overlap
            if c5f_overlap(text):return 'composition_c5f'
        if 'condition' in kinds:
            # C5e: one complete outer Lock contains a Condition whose leaf
            # branches own static Loops.
            from .composition_c5e_model import lock_condition_loop_overlap as c5e_overlap
            if c5e_overlap(text):return 'composition_c5e'
        if 'condition' in kinds:
            # C5d: a Loop body owns a complete Lock that contains the Condition.
            from .composition_c5d_model import loop_condition_lock_overlap as c5d_overlap
            if c5d_overlap(text):return 'composition_c5d'
        if 'condition' in kinds:
            # C5c commits when a Loop body owns a Condition leaf Lock.
            from .composition_c5c_model import loop_condition_lock_overlap
            if loop_condition_lock_overlap(text):return 'composition_c5c'
        if 'condition' in kinds:
            # C5b is the reverse of C5a: the Lock is complete inside a
            # Condition leaf and the Loop is complete inside that Lock.
            from .composition_c5b_model import condition_lock_loop_overlap
            if condition_lock_loop_overlap(text):return 'composition_c5b'
        if 'condition' in kinds:
            # The approved three-layer nesting commits to C5a as soon as a
            # complete Lock sits inside a Condition-leaf Loop.  Invalid
            # combinations must fail C5a validation, never fall through.
            from .composition_c5a_model import condition_leaf_loop_lock_overlap
            if condition_leaf_loop_lock_overlap(text):return 'composition_c5a'
        from .composition_c4b_model import loop_inside_lock_overlap
        if loop_inside_lock_overlap(text):return 'composition_c4b'
        from .composition_c4a_model import lock_loop_overlap
        if lock_loop_overlap(text):return 'composition_c4a'
    if {'condition','loop'} <= kinds:
        from .composition_c3b_model import condition_inside_loop
        if condition_inside_loop(text):return 'composition_c3b'
        from .composition_c3a_model import condition_loop_overlap
        if condition_loop_overlap(text):return 'composition_c3a'
    if {'condition','lock'} <= kinds:
        # Dispatch by complete syntax, so invalid overlaps cannot fall back.
        from .condition_model import analyze_conditionals, _sanitize_c
        clean=_sanitize_c(text)
        # A mutex operation in a Condition span commits to C2a validation.
        regions=analyze_conditionals(text,{'<source>':(1,len(text.splitlines()))}).regions
        if any(__import__('re').search(r'\bpthread_mutex_\w+\s*\(',clean[r.start_offset:r.end_offset]) for r in regions):
            return 'composition_c2a'
        # Match by the analyzer's function/thread ownership and actual pair.
        from .composition_c2b_model import _source_matched_regions
        function_ranges,matched_regions=_source_matched_regions(text)
        owned_conditions=analyze_conditionals(text,function_ranges).regions
        if any(r.function==lock['function']==lock['thread_entry_function']
               and lock['lock_range'][0]<r.entry_line and r.exit_line<lock['unlock_range'][0]
               and lock['start_offset']<r.start_offset and r.end_offset<lock['end_offset']
               for r in owned_conditions for lock in matched_regions):
            return 'composition_c2b'
        from .composition_c2b_basic_model import helper_condition_inside_lock
        if helper_condition_inside_lock(text,function_ranges,matched_regions):
            return 'composition_c2b'
        # Condition and Lock coexist but neither contains the other and there
        # is no Loop: C2c handles the independent (non-nested) case.
        if 'loop' not in kinds:
            return 'composition_c2c'
    return RULE


def run_composition_blocks(*, base_dir, base_name, source_file):
    from .runner import run_blocks
    rule = select_rule(source_file.read_bytes().decode('utf-8'))
    return run_blocks(base_dir=base_dir,base_name=base_name,source_file=source_file,level='level2',rule_name=rule)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-dir',type=Path,required=True)
    p.add_argument('--base-name',required=True)
    p.add_argument('--source',type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run_composition_blocks(base_dir=a.base_dir,base_name=a.base_name,source_file=a.source),indent=2))


if __name__ == '__main__': main()
