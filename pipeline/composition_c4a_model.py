"""C4a local Loop-first mutex instances; never group across iterations."""
import copy,hashlib,re,json
from pathlib import Path
from .composition_model import CONTROL,require,_mask,_project,_ordered_unique,digest
from .condition_model import _sanitize_c,_match_pair,_skip_ws,analyze_conditionals,build_condition_views
from .lock_regions import match_lock_regions
from .composition_c2a_model import contract_branch_groups
from .composition_c3a_model import _template_targets
from .composition_c3b_model import _iteration_fields
from .loop_nested_model import build_loop_nested_views
from ..core.func_ranges import (_strip_comments_and_strings,is_ignored_compiler_fn,
    _find_definition_signature,_scan_to_body_open,_match_brace_block,normalize_func_name)
from .algo.common import topo_sort
RULE='composition_c4a'


def lock_loop_overlap(text):
    clean=_sanitize_c(text)
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');end=_match_pair(clean,_skip_ws(clean,close+1),'{','}')
        if re.search(r'\bpthread_mutex_\w+\s*\(',clean[m.start():end+1]):return True
    # Reverse and crossing placement must commit to this validator too.
    for m in re.finditer(r'\bpthread_mutex_lock\s*\(',clean):
        end=re.search(r'\bpthread_mutex_unlock\s*\(',clean[m.end():])
        if end and re.search(r'\bfor\b',clean[m.end():m.end()+end.start()]):return True
    return False



def _formal_function_spans(raw):
    """Read official generation-owned function body ranges for every source function.

    Level-2 deliberately omits leaf helpers that own no callsite metadata.  C4a
    still needs their formal ranges to analyze Conditions outside Loop+Lock.
    The metadata referenced by the base view is authoritative; no source-wide
    name scan is used to discover extra functions.
    """
    inputs=raw.get('inputs')
    if not isinstance(inputs,dict) or 'functions_ranges' not in inputs:return {},{}
    value=inputs.get('functions_ranges')
    require(isinstance(value,str) and bool(value),'C4a invalid formal functions_ranges metadata')
    path=Path(value)
    if not path.is_absolute():
        source=raw.get('source_file')
        require(isinstance(source,str) and bool(source),'C4a cannot resolve formal functions_ranges metadata')
        path=Path(source).parent/path
    require(path.is_file(),'C4a missing formal functions_ranges metadata: '+str(path))
    try:data=json.loads(path.read_text(encoding='utf-8',errors='replace'))
    except Exception as exc:raise ValueError('C4a invalid formal functions_ranges metadata: '+str(path)) from exc
    rows=data.get('functions') if isinstance(data,dict) else None
    require(isinstance(rows,list),'C4a invalid formal functions_ranges metadata: '+str(path))
    spans={};sources={}
    for row in rows:
        if not isinstance(row,dict):continue
        name=row.get('name') or row.get('base_name')
        if not isinstance(name,str) or not name:continue
        name=normalize_func_name(name)
        start=row.get('start_line');end=row.get('end_line')
        body_start=row.get('level1_start_line') or row.get('first_stmt_line') or start
        if not (isinstance(start,int) and isinstance(end,int) and start>0 and end>=start):
            start=body_start;end=None
            last_return=row.get('last_return_line');last_stmt=row.get('last_stmt_line')
            level1_end=row.get('level1_end_line');end_line=row.get('end_line')
            if isinstance(last_return,int) and last_return>0:end=last_return-1
            elif isinstance(last_stmt,int) and last_stmt>0:end=last_stmt
            elif isinstance(level1_end,int) and level1_end>0:end=level1_end
            elif isinstance(end_line,int) and end_line>0:end=end_line-1
        if isinstance(start,int) and isinstance(end,int) and start>0 and end>=start:
            if not (isinstance(body_start,int) and start<=body_start<=end):body_start=start
            if name in spans:require(spans[name]==(start,end),'C4a conflicting formal function range: '+name)
            else:spans[name]=(start,end);sources[name]=(body_start,end)
    return dict(sorted(spans.items())),dict(sorted(sources.items()))


def _condition_function_ranges(source_text, segments, formal=None):
    """Resolve formal functions, preferring generation metadata over local scans."""
    clean=_strip_comments_and_strings(source_text);ranges=dict(formal or {})
    for row in segments:
        fn=row.get('function')
        require(type(fn) is str and bool(fn),'C4a missing formal function owner')
        if fn not in ranges:
            cursor=0;definitions=[]
            while True:
                found=_find_definition_signature(clean,normalize_func_name(fn),cursor)
                if found is None:break
                _,paren=found;cursor=paren+1;opening=_scan_to_body_open(clean,paren)
                if opening is None:continue
                closing=_match_brace_block(clean,opening)
                require(closing is not None,'C4a incomplete formal function: '+fn)
                definitions.append((clean.count('\n',0,found[0])+1,clean.count('\n',0,closing)+1))
            require(len(definitions)==1,'C4a ambiguous or missing function definition: '+fn)
            ranges[fn]=definitions[0]
        a,b=ranges[fn]
        require(a<=row['start_line']<=row['end_line']<=b,'C4a segment outside formal function: '+str(row['seg_id']))
    return ranges


def _helper_condition_views(*,base_name,source_text,raw,sources,functions,prefix='C4AHELPER'):
    """Condition-only structural view for helpers that own no schedulable old segment."""
    segments=[dict(seg_id=prefix+':'+fn,function=fn,kind='compute',start_line=sources[fn][0],
                   end_line=sources[fn][1],source_only=True,analysis_only=True) for fn in functions]
    payload=copy.deepcopy(raw)
    payload.update(segments=segments,node_to_segment={},node_locations={},mappings={})
    dag=dict(nodes=[row['seg_id'] for row in segments],edges=[])
    struct,struct_dag,execution,_,meta=build_condition_views(
        base_name=base_name,source_text=source_text,base_segments_json=payload,base_dag_json=dag)
    require(meta['scheduler_compatible'],'C4a incompatible helper Condition')
    return struct,struct_dag,execution,segments



def _restore_condition_dependencies(segments, source_dag, condition_segments, condition_dag):
    """Clone complete dependencies onto the Condition model's formal ports."""
    originals={r['seg_id']:r for r in segments['segments']};bindings={}
    for edge in source_dag['edges']:
        def ports(sid):
            base=originals[sid]
            return sorted((r for r in condition_segments['segments'] if r['function']==base['function'] and max(r['start_line'],base['start_line'])<=min(r['end_line'],base['end_line'])),key=lambda r:(r['start_line'],0 if r['kind']=='condition_entry' else 2 if r['kind']=='condition_exit' else 1,r['end_line']))
        left,right=ports(edge['src']),ports(edge['dst'])
        require(left and right,'C4a cannot trace Condition dependency ports')
        key=(left[-1]['seg_id'],right[0]['seg_id'],edge['kind'])
        matches=[e for e in condition_dag['edges'] if (e['src'],e['dst'],e['kind'])==key]
        # Lexical intra edges replaced by Condition branch semantics remain in
        # the full original ledger; never connect sibling branches directly.
        require(matches or edge['kind']=='intra','C4a cannot preserve external dependency')
        if matches:bindings.setdefault(key,[]).append(edge)
    result=copy.deepcopy(condition_dag);result['edges']=[];emitted=set()
    for edge in condition_dag['edges']:
        key=(edge['src'],edge['dst'],edge['kind'])
        if key not in bindings:result['edges'].append(copy.deepcopy(edge));continue
        if key in emitted:continue
        emitted.add(key)
        for original in bindings[key]:
            clone=copy.deepcopy(original);clone.update(src=edge['src'],dst=edge['dst'])
            result['edges'].append(clone)
    return result


def _contract_loop_groups(dag, groups):
    groups,projection,result=contract_branch_groups(dag,groups)
    for edge in result['edges']:
        witnesses=copy.deepcopy(edge['provenance']);chain=[]
        for witness in witnesses:
            for original in witness['edge'].get('provenance',[]):
                if original not in chain:chain.append(copy.deepcopy(original))
        edge['provenance']=chain+witnesses
    return groups,projection,result



def _expand_absorbed_intra(rows, edges, absorbed):
    """Project absorbed witnesses through concrete source-segment ports."""
    compute={r['seg_id']:r for r in rows if r['kind'] not in CONTROL};result=[]
    for edge in edges:
        left,right=compute.get(edge['src']),compute.get(edge['dst'])
        matches=[w for w in absorbed if left is not None and right is not None and w['edge']['src'] in left['source_segments'] and w['edge']['dst'] in right['source_segments']]
        if not matches:result.append(copy.deepcopy(edge));continue
        # An already cloned compute-to-compute edge passes unchanged through
        # the ordinary control projection; parallel witnesses stay independent.
        if edge.get('edge_instance_id') and any(w.get('base_edge_index')==origin['base_edge_index'] and w.get('edge')==origin['edge'] for w in edge.get('provenance',[]) for origin in matches):
            result.append(copy.deepcopy(edge));continue
        for witness in matches:
            clone=copy.deepcopy(witness['edge']);clone.update(src=edge['src'],dst=edge['dst'])
            clone['edge_instance_id']='C4AEDGE:'+digest([witness['base_edge_index'],edge])
            clone.setdefault('provenance',[]).append(dict(copy.deepcopy(witness),projection_edge=copy.deepcopy(edge)))
            result.append(clone)
    return result


def build_loop_lock_views(*,base_name,source_text,base_segments_json,base_dag_json):
    raw=copy.deepcopy(base_segments_json);dag=copy.deepcopy(base_dag_json);bases=raw['segments'];byid={r['seg_id']:r for r in bases}
    require(len(byid)==len(bases) and set(byid)==set(dag['nodes']),'C4a raw node identity conflict');topo_sort(dag['nodes'],[(e['src'],e['dst']) for e in dag['edges']])
    clean=_sanitize_c(source_text);lines=clean.splitlines();line=lambda p:clean.count('\n',0,p)+1
    require(not re.search(r'^\s*#\s*(?!include\b)\w+',_strip_comments_and_strings(source_text),re.M),'C4a rejects active directives')
    require(not re.search(r'\b(?:typedef|sem_\w+|pthread_mutexattr\w*|pthread_rwlock\w*)\b',clean),'C4a unsupported synchronization or aliases')
    formal,sources=_formal_function_spans(raw)
    ranges=_condition_function_ranges(source_text,bases,formal)
    locks=match_lock_regions(source_text,ranges);require(locks,'C4a missing matched Lock')
    for lock in locks:
        lock['thread_entry_function']=lock['function'] if lock['function'] in raw.get('thread_entry_functions',[]) else None
    spans=[]
    for m in re.finditer(r'\bfor\b',clean):
        close=_match_pair(clean,_skip_ws(clean,m.end()),'(',')');brace=_skip_ws(clean,close+1);end=_match_pair(clean,brace,'{','}')
        owners=[fn for fn,(a,b) in ranges.items() if a<=line(m.start())<=line(end)<=b]
        require(len(owners)==1,'C4a Loop has no formal thread/function owner')
        spans.append(dict(start=m.start(),brace=brace,end=end,header=line(m.start()),exit=line(end),function=owners[0]))
    require(1<=len(spans)<=2,'C4a supports only Loop L1/L2/L3')
    # Lock and Condition ownership use the same formal function ranges;
    # independent Conditions are analyzed in every metadata-owned function.
    condition_ranges=ranges
    condition_analyses=[analyze_conditionals(source_text,{fn:bounds}) for fn,bounds in condition_ranges.items()]
    require(not any(a.diagnostics for a in condition_analyses),'C4a invalid Condition')
    independent_conditions=[r for a in condition_analyses for r in a.regions]
    for c in independent_conditions:require(not any(max(c.start_offset,p['start'])<min(c.end_offset,p['end']+1) for p in spans),'C4a Condition must stay outside Loop/Lock')
    # Ordinary helpers without a Level-2 ownership row are analyzed from the
    # same formal ranges.  They never become thread entries or execution rows.
    segment_functions={r['function'] for r in bases}
    helper_functions=sorted({r.function for r in independent_conditions if r.function not in segment_functions})
    helper_views=_helper_condition_views(base_name=base_name,source_text=source_text,raw=raw,
                                         sources=sources,functions=helper_functions) if helper_functions else None
    for lock in locks:
        contained=[p for p in spans if p['function']==lock['function'] and p['brace']<lock['start_offset'] and lock['end_offset']<=p['end']]
        require(contained,'C4a matched lock/unlock must stay inside one Loop body')
        lock['loop_header']=max(contained,key=lambda p:p['start'])['header']
        ordinary=[i for i in range(lock['lock_range'][0]+1,lock['unlock_range'][0]) if lines[i-1].strip()]
        require(ordinary,'C4a empty critical region');lock['critical_range']=[ordinary[0],ordinary[-1]]
        require(not re.search(r'\b(?:for|while|do|if|else|switch|return|goto|break|continue|pthread_\w+)\b','\n'.join(lines[ordinary[0]-1:ordinary[-1]])),'C4a unsupported control/synchronization inside critical region')
    declarations=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M));names=[m.group(1) for m in declarations]
    require(names and len(set(names))==len(names) and len(names)==len(re.findall(r'\bpthread_mutex_t\b',clean)),'C4a requires direct mutex declarations')
    remainder=_mask(clean,[(m.start(),m.end()) for m in declarations]);remainder=re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;','',remainder)
    require(not any(re.search(r'\b'+re.escape(n)+r'\b',remainder) for n in names),'C4a rejects mutex aliases or extra uses')
    # Local lexical owner normalization, preserving all original edge witnesses.
    outer=[p for p in spans if not any(q['start']<p['start']<p['end']<q['end'] for q in spans)];buckets=[]
    for p in outer:
        selected={r['seg_id'] for r in bases if r['function']==p['function'] and max(r['start_line'],p['header'])<=min(r['end_line'],p['exit'])}
        require(selected,'C4a missing Loop source owners')
        intersect=[b for b in buckets if b&selected]
        for b in intersect:selected|=b;buckets.remove(b)
        buckets.append(selected)
    projection={n:n for n in byid};normalized=[]
    for index,bucket in enumerate(buckets):
        parts=[byid[n] for n in bucket];fn=parts[0]['function'];require(all(r['function']==fn for r in parts),'C4a conflicting function owners')
        sid='C4AOWNER:'+fn+':'+str(index)
        normalized.append(dict(seg_id=sid,function=fn,kind='compute',start_line=min(r['start_line'] for r in parts),end_line=max(r['end_line'] for r in parts)))
        for n in bucket:projection[n]=sid
    normalized += [copy.deepcopy(r) for r in bases if projection[r['seg_id']]==r['seg_id']]
    normalized.sort(key=lambda r:(r['start_line'],r['seg_id']));nedges=[];absorbed_intra=[]
    for i,original in enumerate(dag['edges']):
        a,b=projection[original['src']],projection[original['dst']]
        if a==b:
            require(original['kind']=='intra','C4a cannot absorb non-intra dependency')
            absorbed_intra.append(dict(base_edge_index=i,edge=copy.deepcopy(original)));continue
        e=copy.deepcopy(original);e.update(src=a,dst=b);e.setdefault('provenance',[]).append(dict(base_edge_index=i,edge=copy.deepcopy(original)))
        if e not in nedges:nedges.append(e)
    payload=copy.deepcopy(raw);payload.update(segments=normalized,node_to_segment={n:projection[s] for n,s in raw.get('node_to_segment',{}).items()})
    ndag=dict(nodes=[r['seg_id'] for r in normalized],edges=nedges);topo_sort(ndag['nodes'],[(e['src'],e['dst']) for e in nedges])
    offsets=[0]+[m.end() for m in re.finditer('\n',source_text)]
    pthread=[(offsets[i],offsets[i+1] if i+1<len(offsets) else len(source_text)) for i,l in enumerate(lines) if re.search(r'\bpthread_\w+\b',l)]
    condition_struct,condition_dag,condition_exec,_,meta=build_condition_views(base_name=base_name,source_text=source_text,base_segments_json=payload,base_dag_json=ndag)
    require(meta['scheduler_compatible'],'C4a incompatible outside Condition')
    # Reuse the complete serialized execution row by its formal structural ID.
    # Do not rebuild branch metadata or mutate either original Condition view.
    condition_rows={r['source_structural_node']:r for r in condition_exec['segments']}
    cs=copy.deepcopy(condition_struct);cd=_restore_condition_dependencies(payload,ndag,condition_struct,condition_dag)
    cs['segments']=[copy.deepcopy(condition_rows[r['seg_id']]) if r['kind'] not in CONTROL else copy.deepcopy(r) for r in condition_struct['segments']]
    for p in outer:
        owners=[r for r in cs['segments'] if r['function']==p['function'] and r['kind']=='compute' and r['start_line']<=p['header']<=r['end_line']]
        require(len(owners)==1,'C4a missing unique Loop window');owners[0]['end_line']=max(owners[0]['end_line'],p['exit'])
    helper_rows=[];helper_edges=[];helper_conditions=[];helper_groups={};helper_segment_rows=[]
    helper_segment_to_struct={};helper_position_to_struct={}
    if helper_views is not None:
        helper_struct,helper_dag,helper_exec,helper_segments=helper_views
        helper_exec_by_id={r['seg_id']:r for r in helper_exec['segments']}
        helper_segment_rows=[dict(copy.deepcopy(r),source_only=True,analysis_only=True) for r in helper_segments]
        for original in helper_struct['segments']:
            row=copy.deepcopy(original);row.update(source_only=True,analysis_only=True)
            if row['kind'] not in CONTROL:
                members=helper_exec_by_id.get(row['seg_id'],{}).get('condition_memberships')
                if members:row['condition_memberships']=copy.deepcopy(members)
                fn=row['function'];row['source_position_ids']=['C4AHELPER:'+fn+':line:'+str(n) for n in range(row['start_line'],row['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            helper_rows.append(row)
            for sid in row.get('source_segments',[]):
                if sid.startswith('C4AHELPER:'):helper_segment_to_struct.setdefault(sid,[]).append(row['seg_id'])
            for key in row.get('source_position_ids',[]):helper_position_to_struct.setdefault(key,[]).append(row['seg_id'])
        helper_edges=[copy.deepcopy(e) for e in helper_dag['edges']]
        helper_conditions=[copy.deepcopy(r) for r in helper_struct['condition_regions']]
        helper_groups=copy.deepcopy(helper_dag.get('exclusive_groups',{}))
    loop_source=_mask(source_text,pthread+[(r.start_offset,r.end_offset) for r in independent_conditions]);payload['segments']=cs['segments'];payload['node_to_segment']={}
    st,sd,_,_,_=build_loop_nested_views(base_name=base_name,source_text=loop_source,base_segments_json=payload,base_dag_json=cd)
    templates=copy.deepcopy(st['templates']);loops=copy.deepcopy(st['loops']);local={};lock_templates=[];source_components=[]
    for loop in loops:
        selected=[r for r in locks if r['loop_header']==loop['header_line']]
        if not selected:continue
        selected.sort(key=lambda r:r['lock_range'][0]);fn=loop['function'];require(all(r['function']==fn for r in selected),'C4a function mismatch')
        old=[t for t in templates if t['role']=='body_template' and t['start_line']==loop['body_start'] and t['end_line']==loop['body_end']];require(len(old)==1,'C4a body template identity conflict')
        loop['source_body_template']=dict(copy.deepcopy(old[0]),analysis_only=True);templates=[t for t in templates if t['template_id']!=old[0]['template_id']]
        pieces=[];cursor=loop['body_start']
        def ordinary(a,b):
            effective=[n for n in range(a,b+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
            if effective:pieces.append(dict(role='ordinary',start_line=effective[0],end_line=effective[-1]))
        for lock in selected:
            ordinary(cursor,lock['lock_range'][0]-1);pieces.append(dict(role='critical',start_line=lock['lock_range'][0],end_line=lock['unlock_range'][0],lock=copy.deepcopy(lock)));cursor=lock['unlock_range'][0]+1
        ordinary(cursor,loop['body_end']);local[loop['loop_id']]=pieces
        for index,piece in enumerate(pieces):
            tid='C4ASRC:'+loop['loop_id']+':part:'+str(index);piece['source_template_id']=tid
            source_components.append(dict(template_id=tid,loop_id=loop['loop_id'],function=fn,role='loop_part_template',loop_part_role=piece['role'],analysis_only=True,start_line=piece['start_line'],end_line=piece['end_line'],source_position_ids=['LOOPSRC:'+fn+':line:'+str(n) for n in range(piece['start_line'],piece['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]))
        lock_templates.extend(dict(copy.deepcopy(lock),loop_id=loop['loop_id'],base_group_id='LOCKGROUP:'+lock['mutex_id']) for lock in selected)
    rows=[];replacement={};edges=[];groups=[];instances=[]
    for row in st['segments']:
        if row.get('role')!='iteration' or row.get('loop_id') not in local:rows.append(copy.deepcopy(row));continue
        fields=_iteration_fields(row);scope=fields['loop_iteration_id'];chain=[];members={}
        for i,piece in enumerate(local[row['loop_id']]):
            sid=scope+':part:'+str(i);tid='C4ATPL:'+sid
            new=dict(fields,seg_id=sid,template_id=tid,source_template_id=piece['source_template_id'],function=row['function'],kind='mutex_cs' if piece['role']=='critical' else 'compute',role='iteration',start_line=piece['start_line'],end_line=piece['end_line'],loop_part_role=piece['role'])
            if piece['role']=='critical':
                lock=piece['lock'];new.update(mutex_id=lock['mutex_id'],base_group_id='LOCKGROUP:'+lock['mutex_id'],lock_source=copy.deepcopy(lock));members.setdefault(lock['mutex_id'],[]).append(new)
            rows.append(new);chain.append(sid);templates.append(dict(fields,template_id=tid,function=row['function'],role='iteration_component',start_line=new['start_line'],end_line=new['end_line'],mutex_id=new.get('mutex_id')))
        require(chain,'C4a missing iteration components');replacement[row['seg_id']]=(chain[0],chain[-1]);instances.append(dict(fields,entry=chain[0],exit=chain[-1]))
        edges.extend(dict(src=a,dst=b,kind='intra',source='loop_lock_lexical_order') for a,b in zip(chain,chain[1:]))
        for mutex,parts in members.items():
            gid='LOCKGROUP:'+mutex+'@'+scope;groups.append(dict(fields,group_id=gid,base_group_id='LOCKGROUP:'+mutex,mutex_id=mutex,member_node_ids=[r['seg_id'] for r in parts],member_segment_ids=[r['seg_id'] for r in parts],members=[],aggregation_method='fieldwise_sum_of_member_weights'))
    # Source-only helper Conditions stay in the structural program view.  They
    # have no old segment, template, generation node or scheduling identity.
    rows.extend(copy.deepcopy(helper_rows))
    for old in sd['edges']:
        e=copy.deepcopy(old)
        if e['src'] in replacement:e['src']=replacement[e['src']][1]
        if e['dst'] in replacement:e['dst']=replacement[e['dst']][0]
        edges.append(e)
    edges.extend(copy.deepcopy(helper_edges))
    exclusive_groups=copy.deepcopy(cd.get('exclusive_groups',{}));exclusive_groups.update(helper_groups)
    graph=dict(base_name=base_name,nodes=[r['seg_id'] for r in rows],edges=edges,exclusive_groups=exclusive_groups);topo_sort(graph['nodes'],[(e['src'],e['dst']) for e in edges])
    execution=[r for r in rows if r['kind'] not in CONTROL and not r.get('source_only')];tids={t['template_id'] for t in templates}
    for r in execution:
        fn=r['function'];r['source_segments']=[b['seg_id'] for b in bases if b['function']==fn and max(b['start_line'],r['start_line'])<=min(b['end_line'],r['end_line'])]
        r['source_position_ids']=['LOOPSRC:'+fn+':line:'+str(n) for n in range(r['start_line'],r['end_line']+1) if lines[n-1].strip() and lines[n-1].strip() not in ('{','}')]
        r['generation_origin']=[n for n,l in raw.get('node_locations',{}).items() if not is_ignored_compiler_fn(n) and l['function']==fn and r['start_line']<=l['line']<=r['end_line']] or None
        if not r.get('template_id'):
            tid='C4ATPL:'+r['seg_id'];require(tid not in tids,'C4a duplicate template');tids.add(tid);r['template_id']=tid;templates.append(dict(template_id=tid,function=fn,role='execution',start_line=r['start_line'],end_line=r['end_line']))
    for row in execution:
        internal=[copy.deepcopy(w) for w in absorbed_intra if w['edge']['src'] in row['source_segments'] and w['edge']['dst'] in row['source_segments']]
        if internal:row['internal_dependencies']=internal
    graph['edges']=_expand_absorbed_intra(execution,graph['edges'],absorbed_intra)
    lookup={r['seg_id']:r for r in execution}
    for group in groups:
        group['members']=[dict(copy.deepcopy(lookup[sid]['lock_source']),member_node_id=sid,member_segment_id=sid,execution_segment=copy.deepcopy(lookup[sid])) for sid in group['member_segment_ids']]
    pre=copy.deepcopy(graph);pre.update(nodes=[r['seg_id'] for r in execution],edges=_expand_absorbed_intra(execution,_project(execution,graph),absorbed_intra));groups,project,dd=_contract_loop_groups(pre,groups);topo_sort(dd['nodes'],[(e['src'],e['dst']) for e in dd['edges']])
    grouped={sid for g in groups for sid in g['member_segment_ids']};final=[copy.deepcopy(r) for r in execution if r['seg_id'] not in grouped]
    for g in groups:
        parts=[m['execution_segment'] for m in g['members']];final.append(dict(copy.deepcopy(g),seg_id=g['group_id'],kind='lock_group',role='iteration',function=parts[0]['function'],start_line=min(r['start_line'] for r in parts),end_line=max(r['end_line'] for r in parts),source_segments=_ordered_unique(s for r in parts for s in r['source_segments']),source_position_ids=_ordered_unique(s for r in parts for s in r['source_position_ids']),generation_origin=_ordered_unique(n for r in parts for n in (r['generation_origin'] or [])) or None))
    oldmap={b['seg_id']:_ordered_unique(project[r['seg_id']] for r in execution if b['seg_id'] in r['source_segments']) for b in bases};positions={}
    for key,targets in raw.get('mappings',{}).get('source_position_to_execution_segments',{}).items():positions[key]=_ordered_unique(project[r['seg_id']] for r in execution if any(t in r['source_segments'] for t in targets) and (not key.startswith('LOOPSRC:') or key in r['source_position_ids']))
    for r in execution:
        for key in r['source_position_ids']:positions.setdefault(key,[]).append(project[r['seg_id']])
    # These are real source templates, including when the Cartesian iteration
    # product is zero. Preserve only their declared source-position identities.
    for template in source_components:
        for key in template['source_position_ids']:positions.setdefault(key,[])
    for row in helper_rows:
        for key in row.get('source_position_ids',[]):positions.setdefault(key,[])
    source_targets={t['template_id']:[] for t in source_components}
    require(len(source_targets)==len(source_components),'C4a duplicate source component template')
    for row in execution:
        if 'source_template_id' not in row:continue
        tid=row['source_template_id'];target=project[row['seg_id']]
        require(tid in source_targets and target in dd['nodes'],'C4a unknown source template or final target')
        if target not in source_targets[tid]:source_targets[tid].append(target)
    template_targets=_template_targets(templates,execution,project,set(dd['nodes']))
    require(not set(template_targets)&set(source_targets),'C4a conflicting template identities')
    template_targets.update(source_targets)
    gen={n:(_ordered_unique(project[r['seg_id']] for r in execution if n in (r['generation_origin'] or [])) if n in raw.get('node_locations',{}) else oldmap[sid].copy()) for n,sid in raw.get('node_to_segment',{}).items() if not is_ignored_compiler_fn(n)}
    mappings=copy.deepcopy(raw.get('mappings',{}));mappings.update(generation_node_to_execution_segments=gen,original_segment_to_execution_segments=oldmap,source_position_to_execution_segments={k:_ordered_unique(v) for k,v in positions.items()},template_to_execution_segments=template_targets,grouped_node_to_member_segments={g['group_id']:g['member_segment_ids'] for g in groups},source_lock_region_to_instances={f"{l['function']}:{l['lock_range'][0]}":[g['group_id'] for g in groups if any(m['function']==l['function'] and m['lock_range']==l['lock_range'] for m in g['members'])] for l in lock_templates})
    if helper_conditions:
        mappings['source_only_segment_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_segment_to_struct.items())}
        mappings['source_position_to_structural_nodes']={k:_ordered_unique(v) for k,v in sorted(helper_position_to_struct.items())}
    condition_regions=copy.deepcopy(cs['condition_regions'])
    if helper_conditions:condition_regions=sorted(condition_regions+helper_conditions,key=lambda r:(r['function'],r['entry_line'],r['exit_line'],r['condition_id']))
    common=dict(base_name=base_name,rule_name=RULE,level='level2',source_file=raw['source_file'],source_sha256=hashlib.sha256(source_text.encode()).hexdigest(),thread_entry_functions=copy.deepcopy(raw.get('thread_entry_functions',[])),loops=loops,condition_regions=condition_regions,templates=templates,source_component_templates=source_components,lock_groups=groups,lock_templates=lock_templates,iteration_instances=instances,absorbed_intra_dependencies=copy.deepcopy(absorbed_intra),mappings=mappings,composition_edge_provenance=[dict(base_edge_index=i,edge=copy.deepcopy(e)) for i,e in enumerate(dag['edges'])],composition_input_sha256=digest([raw,dag]),capabilities=dict(scheduler_compatible=True,timing_compatible=False,instrumentation_compatible=False,analysis_only=True))
    if helper_conditions:common['source_only_segments']=copy.deepcopy(helper_segment_rows)
    return dict(copy.deepcopy(common),view='struct',segments=rows),graph,dict(common,view='sched',segments=final),dd,dict(rule_name=RULE,level='level2',scheduler_compatible=True,analysis_only=True)
