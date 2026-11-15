from __future__ import annotations

from typing import Dict

from .rules import ConditionalBranchesRule, Level1Stage1Rule, Level2EffectiveLineRule, Level3PlaceholderRule
from .rules.lock_rule import LockGroupedRule
from .rules.loop_rule import LoopStaticRule
from .rules.loop_pair_rule import LoopStaticPairRule
from .rules.loop_nested_rule import LoopStaticNestedRule
from .rules.composition_rule import CompositionC1Rule
from .rules.composition_c2a_rule import CompositionC2aRule
from .rules.composition_c2b_rule import CompositionC2bRule
from .rules.composition_c3a_rule import CompositionC3aRule
from .rules.composition_c4a_rule import CompositionC4aRule
from .rules.composition_c4b_rule import CompositionC4bRule
from .rules.composition_c5a_rule import CompositionC5aRule
from .rules.composition_c5b_rule import CompositionC5bRule
from .rules.composition_c5e_rule import CompositionC5eRule
from .rules.composition_c5f_rule import CompositionC5fRule
from .rules.composition_c5c_rule import CompositionC5cRule
from .rules.composition_c5d_rule import CompositionC5dRule
from .rules.composition_c3b_rule import CompositionC3bRule
from .rules.base import RulePlugin

RULES_REGISTRY: Dict[str, Dict[str, RulePlugin]] = {
    "level1": {"stage1_create_join": Level1Stage1Rule()},
    "level2": {
        "effective_line_merge": Level2EffectiveLineRule(),
        "conditional_branches": ConditionalBranchesRule(),
        "composition_c1": CompositionC1Rule(),
        "composition_c2a": CompositionC2aRule(),
        "composition_c2b": CompositionC2bRule(),
        "composition_c3a": CompositionC3aRule(),
        "composition_c4a": CompositionC4aRule(),
        "composition_c4b": CompositionC4bRule(),
        "composition_c5a": CompositionC5aRule(),
        "composition_c5b": CompositionC5bRule(),
        "composition_c5e": CompositionC5eRule(),
        "composition_c5f": CompositionC5fRule(),
        "composition_c5c": CompositionC5cRule(),
        "composition_c5d": CompositionC5dRule(),
        "composition_c3b": CompositionC3bRule(),
        "lock_grouped": LockGroupedRule(),
        "loop_static": LoopStaticRule(),
        "loop_static_pair": LoopStaticPairRule(),
        "loop_static_nested": LoopStaticNestedRule(),
    },
    "level3": {"placeholder_level3": Level3PlaceholderRule()},
}


def list_rules(level: str) -> Dict[str, RulePlugin]:
    return dict(RULES_REGISTRY.get(level, {}))


def get_rule(level: str, rule_name: str) -> RulePlugin:
    rules = RULES_REGISTRY.get(level, {})
    if rule_name not in rules:
        known = ", ".join(sorted(rules.keys())) if rules else "(none)"
        raise KeyError(f"unknown rule '{rule_name}' for {level}; known: {known}")
    return rules[rule_name]

