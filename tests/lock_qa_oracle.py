"""Test-only oracle. No production imports or contraction helpers."""
import copy
from collections import Counter, deque
import random

SEED = 2026103101
KINDS = ('intra', 'create', 'join', 'sem_dep')


def acyclic(nodes, edges):
    successors = {n:set() for n in nodes}
    indegree = {n:0 for n in nodes}
    for e in edges:
        a,b=e['src'],e['dst']
        if b not in successors[a]:
            successors[a].add(b);indegree[b]+=1
    todo=deque(n for n in nodes if indegree[n]==0);seen=0
    while todo:
        n=todo.popleft();seen+=1
        for nxt in successors[n]:
            indegree[nxt]-=1
            if indegree[nxt]==0:todo.append(nxt)
    return seen==len(nodes)


def reference(dag, groups):
    ownership={}
    for group in groups:
        for node in group['member_node_ids']:
            if node in ownership:raise ValueError('oracle: overlapping membership')
            ownership[node]=group['group_id']
    mapping={node:ownership.get(node,node) for node in dag['nodes']}
    output_nodes=set(mapping.values());projected={};internal=[]
    for index,edge in enumerate(dag['edges']):
        origin={'base_edge_index':index,'edge':copy.deepcopy(edge)}
        a,b=mapping[edge['src']],mapping[edge['dst']]
        if edge['src'] in ownership and edge['dst'] in ownership and a==b:
            internal.append(origin)
        else:
            projected.setdefault((a,b,edge['kind']),[]).append(origin)
    edges=[{'src':a,'dst':b,'kind':kind,'provenance':origins}
           for (a,b,kind),origins in sorted(projected.items())]
    return {'nodes':output_nodes,'mapping':mapping,'edges':edges,'internal':internal,
            'compatible':acyclic(output_nodes,edges)}


def assert_candidate(dag, groups, mapping, candidate):
    """Reject lost/forged edges, altered types, incomplete provenance and wrong membership."""
    expected=reference(dag,groups)
    assert set(candidate['nodes'])==expected['nodes'], 'node projection mismatch'
    assert mapping==expected['mapping'], 'member mapping mismatch'
    actual={(e['src'],e['dst'],e['kind']):e['provenance'] for e in candidate['edges']}
    wanted={(e['src'],e['dst'],e['kind']):e['provenance'] for e in expected['edges']}
    assert len(actual)==len(candidate['edges']), 'duplicate projected edge'
    assert actual==wanted, 'edge/provenance mismatch'
    assert candidate['removed_internal_edges']==expected['internal'], 'internal edge mismatch'
    origins=[o['base_edge_index'] for values in actual.values() for o in values]
    origins += [o['base_edge_index'] for o in candidate['removed_internal_edges']]
    assert Counter(origins)==Counter(range(len(dag['edges']))), 'edge source partition mismatch'
    assert len(candidate['nodes'])==len(dag['nodes'])-sum(len(g['member_node_ids'])-1 for g in groups), 'node formula mismatch'
    return expected


def assert_weights(groups, original, actual):
    for g in groups:
        for field in ('total_ns','count','avg_ns','min_ns','max_ns'):
            expected=sum(original[m][field] for m in g['member_segment_ids'])
            assert actual[g['group_id']][field]==expected, 'weight sum mismatch'


def random_case(index):
    rng=random.Random(SEED+index)
    n=rng.randint(5,50);k=rng.randint(1,min(5,(n-1)//2))
    sizes=[2]*k;budget=n-1-2*k
    for j in range(k):
        extra=rng.randint(0,min(4,budget));sizes[j]+=extra;budget-=extra
    nodes=[f'N{i:02d}' for i in range(n)]
    pool=list(range(n-1))
    if index%2:rng.shuffle(pool)
    assigned={};cursor=0
    for j,size in enumerate(sizes):
        for i in pool[cursor:cursor+size]:assigned[nodes[i]]=f'm{j}'
        cursor+=size
    groups=[];regions=[];lines=[];rows=[];locations={};node_map={}
    for node in nodes:
        start=len(lines)+1;fn='f_'+node
        if node in assigned:
            mutex=assigned[node];lines += [f'pthread_mutex_lock(&{mutex});','work();',f'pthread_mutex_unlock(&{mutex});']
            r={'mutex_id':mutex,'function':fn,'lock_range':[start,start],'unlock_range':[start+2,start+2],
               'critical_range':[start+1,start+1],'segment_range':[start,start+2]}
            regions.append(r)
            for api,line in [('pthread_mutex_lock',start),('pthread_mutex_unlock',start+2)]:
                name=fn+'/'+api+'1';locations[name]={'function':fn,'line':line};node_map[name]=node
            rows.append({'seg_id':node,'function':fn,'kind':'mutex_cs','start_line':start,'end_line':start+2})
        else:
            lines+=['ordinary_work();'];rows.append({'seg_id':node,'function':fn,'kind':'compute','start_line':start,'end_line':start,'custom':{'id':node}})
            name=fn+'/ordinary_work1';locations[name]={'function':fn,'line':start};node_map[name]=node
    for j in range(k):
        mutex=f'm{j}';members=sorted(node for node,m in assigned.items() if m==mutex)
        sources=[dict(r,member_node_id=node,member_segment_id=node) for node in members for r in regions if r['function']=='f_'+node]
        groups.append({'mutex_id':mutex,'group_id':'LOCKGROUP:'+mutex,'member_node_ids':members,
                       'member_segment_ids':list(members),'members':sources,'analysis_only':True,'aggregation_method':'fieldwise_sum_of_member_weights'})
    edges=[];present=set()
    def add(a,b,kind):
        if (a,b,kind) not in present:
            present.add((a,b,kind));edges.append({'src':a,'dst':b,'kind':kind,'source_tag':f'{index}:{len(edges)}','custom':{'seed':SEED+index}})
    for i in range(n):
        for j in range(i+1,n):
            if rng.random()<0.09:add(nodes[i],nodes[j],rng.choice(KINDS))
    # Guaranteed multiple kinds, internal edges and duplicate projections, all forward in the original DAG.
    for kind in KINDS:add(nodes[0],nodes[-1],kind)
    for g in groups:
        a,b=g['member_node_ids'][:2];add(a,b,'intra');add(a,nodes[-1],'create');add(b,nodes[-1],'create')
    source='\n'.join(lines)+'\n'
    segments={'base_name':'qa','source_file':'<graph-only synthetic metadata>','segments':rows,
              'thread_entry_functions':[r['function'] for r in regions],'node_locations':locations,
              'node_to_segment':node_map,'mappings':{'qa_preserved':{'custom':True}}}
    return {'seed':SEED+index,'index':index,'source':source,'regions':regions,'mutexes':[f'm{i}' for i in range(k)],
            'segments':segments,'dag':{'nodes':nodes,'edges':edges},'groups':groups}


def weights_for(case):
    rng=random.Random(case['seed']+10000);weights={}
    for i,node in enumerate(case['dag']['nodes']):
        avg=0 if i%4==0 else rng.randrange(1,2**80)
        count=rng.randint(1,9)
        weights[node]={'total_ns':avg*count,'count':count,'avg_ns':avg,'min_ns':avg//2,'max_ns':avg*2}
    return {'base_name':'qa','weights':weights,'repeats':1}
