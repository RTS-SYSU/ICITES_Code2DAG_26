from __future__ import annotations

from ..condition_priority import Model, optimize
from ..errors import ValidationError
from .common import make_schedule, validate_priorities

ORDINARY = ('FIFO', 'LPF', 'heft', 't_level', 'zhao2020')


def profile_model(dag_json, segments_json, timing_json):
    config = timing_json.get('condition_config')
    profiles = timing_json.get('condition_profiles')
    if isinstance(config, dict):
        if type(config.get('cores')) is not int or config['cores'] < 1:
            raise ValidationError('positive integer processor count required')
        if type(config.get('max_passes', 12)) is not int or not 1 <= config.get('max_passes', 12) <= 100:
            raise ValidationError('search pass limit must be 1..100')
    if not isinstance(config, dict) or not isinstance(profiles, dict):
        raise ValidationError('conditional algorithms require condition-timing with both measured paths and explicit cores')
    if segments_json.get('capabilities', {}).get('scheduler_compatible') is False:
        raise ValidationError('condition model is not scheduler-compatible')
    regions = segments_json.get('condition_regions', [])
    if len(regions) != 1 or regions[0].get('parent_id') is not None:
        raise ValidationError('exactly one nonnested if/else is required')
    membership = {}
    rows = segments_json.get('segments', [])
    for row in rows:
        members = row.get('condition_memberships', [])
        if members:
            if len(members) != 1 or members[0]['exclusive_group'] != regions[0]['condition_id']:
                raise ValidationError('unexpected conditional membership')
            membership[row['seg_id']] = members[0]['branch_id']
    weights = {}
    for branch in ('then', 'else'):
        if branch not in profiles:
            raise ValidationError('missing measured path: ' + branch)
        measured = profiles[branch].get('weights', {})
        weights[branch] = {}
        for row in rows:
            n = row['seg_id']
            sample = measured.get(n)
            if not isinstance(sample, dict):
                raise ValidationError('missing timing measurement: ' + n)
            if n in membership and membership[n] != branch:
                if sample.get('count', 0) != 0:
                    raise ValidationError('unselected branch executed during profiling: ' + n)
                continue
            if n in membership and sample.get('count', 0) < 1:
                raise ValidationError('active branch has no measured timing sample: ' + n)
            weights[branch][n] = int(sample['avg_ns'])
    model = Model(tuple(row['seg_id'] for row in rows),
                  tuple((edge['src'], edge['dst']) for edge in dag_json['edges']),
                  membership, weights, int(config['cores']), segments_json, dag_json)
    try:
        model.validate()
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc
    return model, config


class ConditionalPriorityAlgo:
    def __init__(self, union_ablation=False):
        self.union_ablation = union_ablation

    def algo_id(self):
        return 'condition_union' if self.union_ablation else 'condition_pareto'

    def compute(self, *, dag_json, segments_json, timing_json):
        from ..algo_registry import get_algo
        model, config = profile_model(dag_json, segments_json, timing_json)
        seed_name = config.get('seed_algo', 'FIFO')
        if seed_name not in ORDINARY:
            raise ValidationError('seed algorithm must be an ordinary DAG algorithm')
        seed = get_algo(seed_name).compute(dag_json=dag_json, segments_json=segments_json,
                                           timing_json=timing_json)['priorities']
        result = optimize(model, seed, max_passes=int(config.get('max_passes', 12)),
                          objective='union_ablation' if self.union_ablation else 'conditional')
        schedule = make_schedule(algo_name=self.algo_id(), base_name=segments_json.get('base_name', ''),
                                 priorities=result['priorities'],
                                 meta={'seed_algo': seed_name, 'processor_count': model.cores,
                                       'method': 'pair_swap_pareto', 'is_response_time_bound': False,
                                       'simulation': {k:v for k,v in result.items() if k != 'priorities'}})
        self.validate(schedule)
        return schedule

    def validate(self, schedule_json):
        validate_priorities(schedule_json)
