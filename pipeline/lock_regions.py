"""Pure, function-scoped matching of direct mutex calls; no combination policy."""
import re
from collections import defaultdict
from ..core.func_ranges import normalize_func_name
from .condition_model import _sanitize_c


class LockRegionError(ValueError):
    pass


def source_function_name(name):
    """Return the source function shared by generated execution instances."""
    return normalize_func_name(name)


def owner_instances(function_ranges, line):
    """Return all execution-instance names for one unique source function.

    A generator may emit ``worker`` plus ``worker@instanceN`` with identical
    source ranges.  Those are two executions of one source function, not two
    source-level owners.  The function still returns both instance names so
    callers can preserve both executions.  Different source functions, or
    instances with inconsistent source ranges, remain ambiguous and raise.
    """
    matches = []
    for name, (a, b) in function_ranges.items():
        if a <= line <= b:
            matches.append((name, a, b))
    if not matches:
        raise LockRegionError('lock call has no formal function owner')
    groups = defaultdict(list)
    for name, a, b in matches:
        groups[source_function_name(name)].append((name, a, b))
    if len(groups) != 1:
        raise LockRegionError('lock call has multiple source function owners')
    entries = next(iter(groups.values()))
    bounds = {(a, b) for _, a, b in entries}
    if len(bounds) != 1:
        raise LockRegionError('source function instances disagree on range')
    return [name for name, _, _ in entries]


def match_lock_regions(source_text, function_ranges):
    """Use caller-supplied formal ranges; never infer a thread from a function."""
    clean=_sanitize_c(source_text);lines=clean.splitlines()
    mutexes=re.findall(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M)
    def require(test,message):
        if not test:raise LockRegionError(message)
    stack=None;regions=[]
    for m in re.finditer(r'\bpthread_mutex_(?!t\b)\w+\s*\(',clean):
        n=clean.count('\n',0,m.start())+1;whole=lines[n-1]
        call=re.fullmatch(r'\s*pthread_mutex_(lock|unlock)\s*\(\s*&\s*(\w+)\s*\)\s*;\s*',whole)
        require(call is not None and call.group(2) in mutexes,'requires direct lock/unlock pairs')
        owners=owner_instances(function_ranges,n)
        if call.group(1)=='lock':
            require(stack is None,'nested/crossing locks unsupported')
            stack=(call.group(2),owners,n,m.start())
        else:
            require(stack is not None and stack[0]==call.group(2) and
                    set(stack[1])==set(owners),'mismatched unlock')
            mutex,instances,start,offset=stack;stack=None
            body=[i for i in range(start+1,n) if lines[i-1].strip()]
            for fn in instances:
                regions.append(dict(mutex_id=mutex,function=fn,
                    lock_range=[start,start],unlock_range=[n,n],
                    segment_range=[start,n],start_offset=offset,
                    end_offset=m.start()+len(whole.strip()),
                    critical_range=[body[0],body[-1]] if body else None))
    require(stack is None,'unpaired lock')
    return regions
