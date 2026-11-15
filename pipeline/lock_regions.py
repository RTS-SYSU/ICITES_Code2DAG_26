"""Pure, function-scoped matching of direct mutex calls; no combination policy."""
import re
from .condition_model import _sanitize_c


class LockRegionError(ValueError):
    pass


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
        owners=[fn for fn,(a,b) in function_ranges.items() if a<=n<=b]
        require(len(owners)==1,'lock must belong to one formal function')
        fn=owners[0]
        if call.group(1)=='lock':
            require(stack is None,'nested/crossing locks unsupported');stack=(call.group(2),fn,n,m.start())
        else:
            require(stack is not None and stack[:2]==(call.group(2),fn),'mismatched unlock')
            mutex,fn,start,offset=stack;stack=None
            body=[i for i in range(start+1,n) if lines[i-1].strip()]
            regions.append(dict(mutex_id=mutex,function=fn,lock_range=[start,start],unlock_range=[n,n],segment_range=[start,n],start_offset=offset,end_offset=m.start()+len(whole.strip()),critical_range=[body[0],body[-1]] if body else None))
    require(stack is None,'unpaired lock')
    return regions
