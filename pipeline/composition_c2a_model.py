"""Local C2a adapter: branch-local mutex quotients, never cross-branch sums."""
import copy
import json
import re
from .composition_model import require, _mask, build_composition_views
from .condition_model import _sanitize_c, _match_pair, _skip_ws
from .lock_regions import match_lock_regions, LockRegionError

RULE='composition_c2a'


def build_branch_lock_groups(*,source_text,base_segments_json):
    """C2a parses direct locks without the standalone lock_grouped policy.

    The lock_grouped model requires two complete regions and exactly one create
    per worker function.  C2a's branch-local groups are per Condition branch and
    may contain one region, and one worker function may be created more than
    once (generated @instanceN rows).  Placement is validated separately by
    validate_lock_placement; here only source syntax and raw-segment ownership
    are checked.
    """
    clean=_sanitize_c(source_text)
    declarations=list(re.finditer(r'^\s*(?:static\s+)?pthread_mutex_t\s+(\w+)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;',clean,re.M))
    names=[m.group(1) for m in declarations]
    require(names and len(set(names))==len(names) and len(names)==len(re.findall(r'\bpthread_mutex_t\b',clean)),
            'C2a requires direct statically initialized mutexes')
    remainder=_mask(clean,[(m.start(),m.end()) for m in declarations])
    remainder=re.sub(r'pthread_mutex_(?:lock|unlock)\s*\(\s*&\s*\w+\s*\)\s*;','',remainder)
    require(not any(re.search(r'\b'+re.escape(name)+r'\b',remainder) for name in names),
            'C2a rejects mutex aliases or extra uses')
    ranges={}
    for row in base_segments_json['segments']:
        a,b=ranges.get(row['function'],(row['start_line'],row['end_line']))
        ranges[row['function']]=(min(a,row['start_line']),max(b,row['end_line']))
    try:regions=match_lock_regions(source_text,ranges)
    except LockRegionError as exc:require(False,'C2a '+str(exc))
    require(regions,'C2a missing matched Lock')
    entries=set(base_segments_json.get('thread_entry_functions',[]))
    for region in regions:
        require(region['function'] in entries,'C2a lock must belong to one direct thread function')
        a,b=region['segment_range']
        owners=[row for row in base_segments_json['segments'] if row['function']==region['function']
                and row['kind']=='mutex_cs' and row['start_line']<=a and b<=row['end_line']]
        require(len(owners)==1,'C2a critical region must be one complete old mutex segment')
        region['member_node_id']=region['member_segment_id']=owners[0]['seg_id']
    groups=[]
    for mutex in names:
        members=[copy.deepcopy(region) for region in regions if region['mutex_id']==mutex]
        groups.append(dict(group_id='LOCKGROUP:'+mutex,mutex_id=mutex,members=members,
                           member_node_ids=[m['member_node_id'] for m in members],
                           member_segment_ids=[m['member_segment_id'] for m in members],
                           aggregation_method='fieldwise_sum_of_member_weights',analysis_only=True))
    return groups


def flatten_condition_headers(text,analysis):
    clean=_sanitize_c(text);spans=[]
    for region in analysis.regions:
        close=_match_pair(clean,_skip_ws(clean,region.start_offset+2),'(',')')
        spans.append((region.start_offset,close+1))
        # Retain business statements, masking only this condition's syntax.
        for branch in region.branches:
            if branch.start_offset is not None:
                if clean[branch.start_offset-1:branch.start_offset]=='{':
                    spans.extend([(branch.start_offset-1,branch.start_offset),(branch.end_offset,branch.end_offset+1)])
        for m in re.finditer(r'\belse\b',clean[region.start_offset:region.end_offset]):
            spans.append((region.start_offset+m.start(),region.start_offset+m.end()))
    return _mask(text,spans)


def validate_lock_placement(a,b,analysis,loops):
    require(not any(max(a,c)<min(b,d) for c,d in loops),'C2a rejects Lock/Loop overlap')
    touched=[r for r in analysis.regions if max(a,r.start_offset)<min(b,r.end_offset)]
    for r in touched:
        branches=[br for br in r.branches if br.start_offset is not None and br.start_offset<=a and b<=br.end_offset]
        require(len(branches)==1,'C2a lock must stay inside one complete Condition branch; crossing/Condition-in-Lock unsupported')


def branch_local_groups(groups,execution):
    result=[]
    for original in groups:
        buckets={}
        for old in original['members']:
            a,b=old['segment_range']
            found=[r for r in execution if r['function']==old['function'] and r['kind']=='mutex_cs' and r['start_line']==a and r['end_line']==b]
            require(len(found)==1,'C2a lost complete mutex execution segment')
            row=found[0];path=row.get('condition_memberships',[])
            key=json.dumps(path,sort_keys=True);bucket=buckets.setdefault(key,[])
            member=copy.deepcopy(old);member['member_node_id']=member['member_segment_id']=row['seg_id'];member['condition_memberships']=copy.deepcopy(path);bucket.append(member)
        for key,member_list in sorted(buckets.items()):
            path=json.loads(key);suffix='|'.join(p['exclusive_group']+':'+p['branch_id'] for p in path) or 'unconditional'
            group=copy.deepcopy(original);group.update(group_id=original['group_id']+'@'+suffix,members=member_list,member_node_ids=[m['member_node_id'] for m in member_list],member_segment_ids=[m['member_segment_id'] for m in member_list],condition_memberships=path)
            result.append(group)
    require(any(g['condition_memberships'] for g in result),'C2a requires a branch-local Lock')
    return result


def contract_branch_groups(dag,groups):
    groups=copy.deepcopy(groups);owner={};ids=set(dag['nodes'])
    for g in groups:
        require(g['group_id'] not in ids,'C2a duplicate group ID')
        for m in g['member_segment_ids']:
            require(m in ids and m not in owner,'C2a duplicate or missing lock member');owner[m]=g['group_id']
    projection={sid:owner.get(sid,sid) for sid in dag['nodes']};edges={};removed=[]
    for i,edge in enumerate(dag['edges']):
        a,b=projection[edge['src']],projection[edge['dst']];origin=dict(base_edge_index=i,edge=copy.deepcopy(edge))
        if a==b and edge['src'] in owner and edge['dst'] in owner:removed.append(origin);continue
        e=copy.deepcopy(edge);e.update(src=a,dst=b)
        key=json.dumps(e,sort_keys=True)
        if key not in edges:edges[key]=dict(e,provenance=[])
        edges[key]['provenance'].append(origin)
    result=copy.deepcopy(dag);result.update(nodes=[n for n in dag['nodes'] if n not in owner]+[g['group_id'] for g in groups],edges=list(edges.values()),removed_internal_edges=removed)
    return groups,projection,result


def build_branch_lock_views(**kwargs):
    return build_composition_views(**kwargs,_branch_locks=True)


def boundary_edge_replacements(edge,left,right,by_id,rows,dag,analysis,clean_lines):
    """Trace syntax-only raw endpoints to existing directional Condition edges.

    These are witnesses of a lexical edge replacement, never new execution
    edges. In particular a closing then port and an opening else port are two
    different control relations, not a path from then to else.
    """
    boundary_regions=[]
    for sid,ports in [(edge['src'],left),(edge['dst'],right)]:
        if ports:continue
        raw=by_id[sid]
        text='\n'.join(clean_lines[raw['start_line']-1:raw['end_line']])
        require(raw['kind']=='compute' and not re.sub(r'\belse\b|[{}\s]', '',text),
                'C2a unresolved endpoint is not a Condition syntax boundary: '+sid)
        regions=[r for r in analysis.regions if r.function==raw['function'] and r.entry_line<=raw['start_line']<=raw['end_line']<=r.exit_line]
        require(regions,'C2a syntax boundary has no Condition owner: '+sid)
        boundary_regions.extend(r.condition_id for r in regions)
    require(edge['kind']=='intra','C2a cannot reinterpret a non-intra boundary dependency')
    candidates=[e for e in dag['edges'] if e['kind'] in ('condition_branch','condition_merge') and e['exclusive_group'] in boundary_regions]
    if left:candidates=[e for e in candidates if e['kind']=='condition_merge' and e['src']==left[-1]['seg_id']]
    if right:candidates=[e for e in candidates if e['kind']=='condition_branch' and e['dst']==right[0]['seg_id']]
    require(candidates,'C2a boundary has no matching directional Condition relation')
    return candidates
