"""Single-if/else simulation and Pareto-search core; not response-time analysis."""
from __future__ import annotations
import argparse
import copy
from dataclasses import dataclass
import itertools
import json
from pathlib import Path
import random
import sys
import time

BRANCHES = ('then', 'else')
ALGOS = ('FIFO', 'LPF', 'heft', 't_level', 'zhao2020')

@dataclass
class Model:
    nodes: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    membership: dict[str, str]
    weights: dict[str, dict[str, int]]
    cores: int
    baseline_segments: dict | None = None
    baseline_dag: dict | None = None

    def validate(self):
        if self.cores < 1 or len(set(self.nodes)) != len(self.nodes) or not 1 <= len(self.nodes) <= 99:
            raise ValueError('Require 1..99 unique nodes and a positive explicit core count')
        if set(self.membership.values()) != set(BRANCHES) or not set(self.membership) <= set(self.nodes):
            raise ValueError('Require exactly one nonempty then/else group')
        if any(u not in self.nodes or v not in self.nodes for u,v in self.edges):
            raise ValueError('Unknown edge endpoint')
        for u,v in self.edges:
            if u in self.membership and v in self.membership and self.membership[u] != self.membership[v]:
                raise ValueError('Edges between mutually exclusive branches are invalid')
        for branch in BRANCHES:
            active=self.active(branch)
            if set(self.weights[branch]) != active:
                raise ValueError('Need independently specified durations for all active scenario nodes')
            if any(type(w) is not int or w < 0 for w in self.weights[branch].values()):
                raise ValueError('Durations must be nonnegative integers')
            completed=set()
            while completed != active:
                ready={v for v in active-completed if all(u in completed for u,w in self.edges if w==v and u in active)}
                if not ready: raise ValueError('Scenario has a cycle')
                completed |= ready

    def active(self,branch):
        if branch not in BRANCHES: raise ValueError('Unknown branch')
        return {n for n in self.nodes if n not in self.membership or self.membership[n]==branch}

    def simulate(self, priorities, branch, trace=False):
        if set(priorities) != set(self.nodes) or any(type(p) is not int or not 1<=p<=99 for p in priorities.values()):
            raise ValueError('One integer priority in 1..99 is required for every execution node')
        active=self.active(branch)
        pred={v:{u for u,w in self.edges if w==v and u in active} for v in active}
        remaining=dict(self.weights[branch]);done=set();now=0;slices=[]
        while done != active:
            # Zero-duration control/blocker nodes complete as soon as ready.
            while True:
                zero={n for n in active-done if remaining[n]==0 and pred[n]<=done}
                if not zero:break
                done |= zero
            if done == active:break
            ready=[n for n in active-done if remaining[n]>0 and pred[n]<=done]
            if not ready:raise ValueError('Deadlocked scenario')
            selected=sorted(ready,key=lambda n:(-priorities[n],n))[:self.cores]
            dt=min(remaining[n] for n in selected)
            if trace:
                for n in selected:slices.append({'node':n,'start':now,'end':now+dt})
            for n in selected:remaining[n]-=dt
            now+=dt
        return {'makespan':now,'slices':slices,'active':sorted(active)}

    def evaluate(self,priorities):
        return tuple(self.simulate(priorities,b)['makespan'] for b in BRANCHES)


def dominates(candidate, current):
    return all(a<=b for a,b in zip(candidate,current)) and any(a<b for a,b in zip(candidate,current))


def optimize(model, seed_priorities, max_passes=12, objective='conditional'):
    """Deterministic pair-swap Pareto search, retaining one static full priority map.

    This is a bounded heuristic. No optimality, WCET or response-bound claim.
    Mutually exclusive vertices are never scheduled together or counted as peers.
    """
    model.validate()
    # Unique priorities avoid pretending lexical tie breaks are Linux FIFO order.
    if len(set(seed_priorities.values())) != len(model.nodes):
        raise ValueError('This initial prototype requires distinct priorities')
    if objective == 'conditional':
        evaluate = model.evaluate
    elif objective == 'union_ablation':
        # Deliberately incorrect execution model used ONLY as an ablation:
        # ordinary DAG search acts as if both branches exist in the same run.
        pooled={n:sum(model.weights[b][n] for b in BRANCHES if n in model.weights[b]) // sum(n in model.weights[b] for b in BRANCHES) for n in model.nodes}
        union=Model(model.nodes,model.edges,{}, {b:dict(pooled) for b in BRANCHES},model.cores)
        evaluate=union.evaluate
    else:
        raise ValueError('Unknown search objective')
    current=dict(seed_priorities);initial=model.evaluate(current);objective_initial=evaluate(current);value=objective_initial
    history=[];evaluations=1;start=time.perf_counter_ns();converged=False
    for iteration in range(max_passes):
        best=None
        for u,v in itertools.combinations(sorted(model.nodes),2):
            # These vertices never co-execute; exchanging them cannot change
            # their direct competition. Other swaps remain freely considered.
            # Do NOT skip the pair: exchanging their numeric ranks can still
            # change each branch's relation to common vertices.
            candidate=dict(current);candidate[u],candidate[v]=candidate[v],candidate[u]
            score=evaluate(candidate);evaluations+=1
            if dominates(score,value):
                key=(sum(score),score,u,v)
                if best is None or key<best[0]:best=(key,candidate,score,u,v)
        if best is None:
            converged=True;break
        _,current,next_value,u,v=best
        history.append({'swap':[u,v],'before':list(value),'after':list(next_value)})
        value=next_value
    return {'priorities':current,'initial':list(initial),'final':list(model.evaluate(current)),
            'search_objective':objective,'objective_initial':list(objective_initial),'objective_final':list(value),
            'history':history,'evaluations':evaluations,'search_ns':time.perf_counter_ns()-start,
            'pairwise_local_optimum':converged,'pass_limit':max_passes,
            'scenario_traces':{b:model.simulate(current,b,trace=True) for b in BRANCHES}}


def payload(model):
    segments=[]
    for i,n in enumerate(model.nodes):
        row={'seg_id':n,'function':'thread_'+n,'kind':'compute','start_line':i+1,'end_line':i+1}
        if n in model.membership:
            row['condition_memberships']=[{'exclusive_group':'single_if','branch_id':model.membership[n],'guard':'true' if model.membership[n]=='then' else 'false'}]
        segments.append(row)
    # Common-node durations are identical in synthetic cases; real measurements
    # are pooled per node only to supply ordinary baselines their existing input.
    weights={}
    for n in model.nodes:
        values=[model.weights[b][n] for b in BRANCHES if n in model.weights[b]]
        weights[n]={'avg_ns':sum(values)//len(values)}
    return (copy.deepcopy(model.baseline_segments) if model.baseline_segments is not None else {'base_name':'condition_research','segments':segments},
            copy.deepcopy(model.baseline_dag) if model.baseline_dag is not None else {'nodes':list(model.nodes),'edges':[{'src':u,'dst':v,'kind':'intra'} for u,v in model.edges]}, {'weights':weights})

