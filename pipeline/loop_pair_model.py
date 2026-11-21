"""Loop L2: compose exactly two sequential L1 regions without overlapping windows."""
import copy
import json
import re
from .condition_model import _sanitize_c,_skip_ws,_match_pair,ConditionModelError
from .loop_model import analyze_static_loop,build_loop_views,LoopModelError,require
from .algo.common import topo_sort


def analyze_loop_pair(source_text):
    clean=_sanitize_c(source_text);keywords=list(re.finditer(r'\b(?:for|while|do)\b',clean))
    if len(keywords)<=1:
        analyze_static_loop(source_text)
        return None
    require(len(keywords)==2 and all(m.group()=='for' for m in keywords),'Loop L2 requires exactly two sequential nonnested static for loops')
    spans=[]
    try:
        for match in keywords:
            opening=_skip_ws(clean,match.end());closing=_match_pair(clean,opening,'(',')');brace=_skip_ws(clean,closing+1)
            end=_match_pair(clean,brace,'{','}')+1;spans.append((match.start(),end))
    except ConditionModelError as exc:
        raise LoopModelError('Loop L2 requires complete braced L1 headers and bodies') from exc
    require(spans[0][1]<=spans[1][0],'Loop L2 rejects nested or overlapping loops')
    regions=[]
    for index in range(2):
        masked=list(source_text);a,b=spans[1-index]
        for offset in range(a,b):
            if masked[offset] not in '\r\n':masked[offset]=' '
        masked=''.join(masked);region=analyze_static_loop(masked)
        require(region is not None,'Loop L2 missing loop region')
        regions.append((region,masked))
    return regions


def build_loop_pair_views(*,base_name,source_text,base_segments_json,base_dag_json):
    regions=analyze_loop_pair(source_text)
    if regions is None:
        return build_loop_views(base_name=base_name,source_text=source_text,base_segments_json=base_segments_json,base_dag_json=base_dag_json)
    bases=base_segments_json['segments'];owners=[]
    for region,_ in regions:
        candidates=[s for s in bases if s['start_line']<=region['header_line'] and region['exit_line']<=s['end_line']]
        require(len(candidates)==1,'Loop L2 requires each complete loop in one existing Level-2 segment')
        owners.append(candidates[0])
    require(owners[0]['function']==owners[1]['function'],'Loop L2 requires both loops in the same function')
    parts={};children=[];locations=base_segments_json.get('node_locations',{})
    for index,((region,masked),owner) in enumerate(zip(regions,owners)):
        window=copy.deepcopy(owner)
        if owners[0]['seg_id']==owners[1]['seg_id']:
            if index==0:window['end_line']=regions[1][0]['header_line']-1
            else:window['start_line']=region['header_line']
        low,high=window['start_line'],window['end_line'];fn=window['function']
        selected_locations={n:copy.deepcopy(l) for n,l in locations.items() if l['function']==fn and low<=l['line']<=high}
        selected_mapping={n:s for n,s in base_segments_json.get('node_to_segment',{}).items() if s==owner['seg_id'] and (n in selected_locations or n not in locations)}
        payload=dict(base_segments_json,segments=[window],node_locations=selected_locations,node_to_segment=selected_mapping)
        views=build_loop_views(base_name=base_name,source_text=masked,base_segments_json=payload,base_dag_json={'nodes':[window['seg_id']],'edges':[]})
        children.append(views);parts.setdefault(owner['seg_id'],[]).append(views)
    struct_rows=[];exec_rows=[];edges=[];replacement={}
    for base in bases:
        sid=base['seg_id']
        if sid not in parts:
            struct_rows.append(copy.deepcopy(base));exec_rows.append(copy.deepcopy(base));continue
        chain=[]
        for child in parts[sid]:
            st,sd,ss,dd,_=child
            if chain:edges.append(dict(src=chain[-1],dst=sd['nodes'][0],kind='intra',source='sequential_loop_windows'))
            chain.extend(sd['nodes']);struct_rows.extend(copy.deepcopy(st['segments']));exec_rows.extend(copy.deepcopy(ss['segments']));edges.extend(copy.deepcopy(sd['edges']))
        replacement[sid]=chain
    for raw in base_dag_json['edges']:
        edge=copy.deepcopy(raw)
        if edge['src'] in replacement:edge['src']=replacement[edge['src']][-1]
        if edge['dst'] in replacement:edge['dst']=replacement[edge['dst']][0]
        edges.append(edge)
    # Normalize only the ordinary source interval between the two complete loops.
    # A single existing middle segment is left byte-compatible with the old view.
    templates=[t for c in children for t in c[2]['templates']]
    middle_redirect={};middle_node=None;middle_trace=None
    clean_middle=_sanitize_c(source_text).splitlines()
    low=regions[0][0]['exit_line']+1;high=regions[1][0]['header_line']-1
    effective=[line for line in range(low,high+1) if clean_middle[line-1].strip()]
    if effective:
        low,high=effective[0],effective[-1];fn=owners[0]['function']
        fragments=sorted([r for r in exec_rows if r['function']==fn and r['start_line']<=high and low<=r['end_line']],key=lambda r:(r['start_line'],r['end_line'],r['seg_id']))
        require(bool(fragments),'Loop L2 middle source interval has no execution segment')
        if len(fragments)>1:
            require(all(low<=r['start_line']<=r['end_line']<=high for r in fragments),'Loop L2 cannot merge a middle fragment extending into a loop')
            require(all(any(r['start_line']<=line<=r['end_line'] for r in fragments) for line in effective),'Loop L2 middle source coverage is incomplete')
            members={r['seg_id'] for r in fragments};lid=children[0][2]['loops'][0]['loop_id'];mid=lid+':after'
            require(mid in members or not any(r['seg_id']==mid for r in struct_rows),'Loop L2 middle ID conflicts with another node')
            middle_node=copy.deepcopy(next((r for r in fragments if r['seg_id']==mid),{}))
            contributors=[r['seg_id'] for r in sorted(bases,key=lambda r:(r['start_line'],r['end_line'],r['seg_id'])) if r['function']==fn and r['start_line']<=high and low<=r['end_line']]
            origins=[]
            for r in fragments:
                for origin in r.get('generation_origin') or []:
                    if origin not in origins:origins.append(origin)
            for origin,location in sorted(locations.items(),key=lambda item:(item[1]['line'],item[0])):
                if location['function']==fn and low<=location['line']<=high and origin not in origins:origins.append(origin)
            origins.sort(key=lambda origin:(locations.get(origin,{}).get('line',high+1),origin))
            middle_node.update(seg_id=mid,function=fn,kind='compute',start_line=low,end_line=high,loop_id=lid,role='after',iteration_index=None,template_id=mid,source_segments=list(dict.fromkeys(contributors)),generation_origin=origins or None,source_position_ids=[f'LOOPSRC:{fn}:line:{line}' for line in effective],source_fragment_provenance=copy.deepcopy(fragments))
            middle_redirect={sid:mid for sid in members}
            def normalized_rows(rows):
                result=[];inserted=False
                for r in rows:
                    if r['seg_id'] in members:
                        if not inserted:result.append(copy.deepcopy(middle_node));inserted=True
                    else:result.append(r)
                return result
            struct_rows=normalized_rows(struct_rows);exec_rows=normalized_rows(exec_rows)
            contracted=[];seen_edges=set();internal=[];duplicates=0
            for original in edges:
                edge=copy.deepcopy(original);edge['src']=middle_redirect.get(edge['src'],edge['src']);edge['dst']=middle_redirect.get(edge['dst'],edge['dst'])
                if original['src'] in members and original['dst'] in members:
                    internal.append(copy.deepcopy(original));continue
                identity=json.dumps(edge,sort_keys=True)
                if identity not in seen_edges:seen_edges.add(identity);contracted.append(edge)
                else:duplicates+=1
            edges=contracted
            absorbed_templates={r.get('template_id') for r in fragments if r.get('template_id')}
            provenance=[copy.deepcopy(t) for t in templates if t['template_id'] in absorbed_templates]
            template=dict(template_id=mid,role='after',start_line=low,end_line=high,source_template_provenance=provenance)
            normalized=[];inserted=False
            for t in templates:
                if t['template_id'] in absorbed_templates:
                    if not inserted:normalized.append(template);inserted=True
                else:normalized.append(t)
            if not inserted:
                second_body=children[1][2]['loops'][0]['loop_id']+':body_template'
                index=next(i for i,t in enumerate(normalized) if t['template_id']==second_body);normalized.insert(index,template)
            templates=normalized
            middle_trace=dict(absorbed_execution_segments=[r['seg_id'] for r in fragments],removed_internal_edges=internal,identical_projected_edges_removed=duplicates)
    nodes=[r['seg_id'] for r in struct_rows];scheduled=[r['seg_id'] for r in exec_rows]
    require(len(nodes)==len(set(nodes)) and len(scheduled)==len(set(scheduled)),'Loop L2 duplicate execution or control ID')
    topo_sort(nodes,[(e['src'],e['dst']) for e in edges]);adj={n:[] for n in nodes}
    for edge in edges:adj[edge['src']].append(edge)
    first_exit=children[0][0]['loops'][0]['loop_id']+':exit';second_entry=children[1][0]['loops'][0]['loop_id']+':entry'
    pending=[first_exit];seen=set()
    while pending:
        node=pending.pop()
        if node not in seen:seen.add(node);pending.extend(e['dst'] for e in adj[node])
    require(second_entry in seen,'Loop L2 requires an existing same-function sequential dependency between loop regions')
    executable=set(scheduled);projected=[]
    for node in scheduled:
        pending=[(e['dst'],[e]) for e in adj[node]]
        while pending:
            target,path=pending.pop()
            if target in executable:
                projected.append(copy.deepcopy(path[0]) if len(path)==1 else dict(src=node,dst=target,kind='loop_flow',control_projection=copy.deepcopy(path)))
            else:pending.extend((e['dst'],path+[e]) for e in adj[target])
    topo_sort(scheduled,[(e['src'],e['dst']) for e in projected])
    generation={n:[] for n in locations};legacy={r['seg_id']:[] if r['seg_id'] in parts else [r['seg_id']] for r in bases};positions={}
    clean_lines=_sanitize_c(source_text).splitlines()
    for row in bases:
        if row['seg_id'] not in parts and row['function']==owners[0]['function']:
            for lineno in range(row['start_line'],row['end_line']+1):
                if clean_lines[lineno-1].strip():
                    positions[f"LOOPSRC:{row['function']}:line:{lineno}"]=[row['seg_id']]
    for n,sid in base_segments_json.get('node_to_segment',{}).items():
        generation[n]=[] if sid in parts else [sid]
    for child in children:
        m=child[2]['mappings']
        for target,key in [(generation,'generation_node_to_execution_segments'),(legacy,'original_segment_to_execution_segments'),(positions,'source_position_to_execution_segments')]:
            for original,ids in m[key].items():
                target.setdefault(original,[])
                for sid in ids:
                    if sid not in target[original]:target[original].append(sid)
    if middle_redirect:
        for mapping in (generation,legacy,positions):
            for original,ids in mapping.items():
                mapping[original]=list(dict.fromkeys(middle_redirect.get(sid,sid) for sid in ids))
        for origin in middle_node['generation_origin'] or []:generation[origin]=[middle_node['seg_id']]
        for identity in middle_node['source_position_ids']:positions[identity]=[middle_node['seg_id']]
    mappings=copy.deepcopy(base_segments_json.get('mappings',{}));mappings.update(generation_node_to_execution_segments=generation,original_segment_to_execution_segments=legacy,source_position_to_execution_segments=positions)
    payload=copy.deepcopy(base_segments_json);payload.pop('node_to_segment',None)
    payload.update(rule_name='loop_static_pair',loops=[c[2]['loops'][0] for c in children],templates=templates,mappings=mappings,capabilities=dict(scheduler_compatible=True,runtime_instrumentation_supported=False),timing_policy='explicit per-loop template copy; middle owned once by first loop after')
    if middle_trace is not None:payload['middle_merge_provenance']=middle_trace
    template_ids=[t['template_id'] for t in payload['templates']];require(len(template_ids)==len(set(template_ids)),'Loop L2 duplicate template ID')
    return dict(payload,segments=struct_rows,view='struct'),dict(base_name=base_name,nodes=nodes,edges=edges),dict(payload,segments=exec_rows,view='sched'),dict(base_name=base_name,nodes=scheduled,edges=projected),dict(rule_name='loop_static_pair',level='level2',loop_count=2,scheduler_compatible=True,runtime_instrumentation_supported=False)
