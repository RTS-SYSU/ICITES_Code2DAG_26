"""Compatibility view for the common error-checked pthread sync idiom.

Many ordinary C programs write:

    if (pthread_mutex_lock(&m) != 0) {
        perror(...);
        exit(...);
    }
    ...
    if (pthread_mutex_unlock(&m) != 0) { ... }

The Level-2 base builder already recognizes the mutex critical section, but
the stricter lock-region rule rejects the surrounding error-check ``if``.  This
module accepts only that narrow wrapper shape and reuses the existing base
segments/DAG as the final graph.  It does not run a second graph builder.
"""
from __future__ import annotations

import copy
import hashlib
import re
from collections import OrderedDict
from typing import Dict, List, Sequence, Tuple

from ..core.func_ranges import _strip_comments_and_strings

RULE = "compatibility_error_checked_sync"


def _clean(text: str) -> str:
    return _strip_comments_and_strings(text)


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def _wrapper_pattern(call: str) -> str:
    return r"if\s*\(\s*pthread_mutex_" + call + r"\s*\([^)]*\)\s*!=\s*0\s*\)"


def parse_error_checked_case(text: str) -> Dict:
    clean = _clean(text)
    if re.search(r"^\s*#\s*(?!include\b)\w+", clean, re.M):
        raise ValueError(
            "error-checked sync compatibility rejects active preprocessor directives"
        )
    lock_match = re.search(_wrapper_pattern("lock"), clean)
    unlock_match = re.search(_wrapper_pattern("unlock"), clean)
    if lock_match is None or unlock_match is None:
        raise ValueError(
            "error-checked sync compatibility requires lock and unlock error-check wrappers"
        )
    if not re.search(r"\bpthread_create\s*\(", clean) or not re.search(
        r"\bpthread_join\s*\(", clean
    ):
        raise ValueError(
            "error-checked sync compatibility requires a create/join program"
        )
    declarations = list(
        dict.fromkeys(
            re.findall(
                r"^\s*(?:static\s+)?pthread_mutex_t\s+([A-Za-z_]\w*)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;",
                clean,
                re.M,
            )
        )
    )
    if len(declarations) != 1:
        raise ValueError(
            "error-checked sync compatibility requires exactly one static mutex"
        )
    calls: List[Dict] = []
    for kind in ("lock", "unlock"):
        for match in re.finditer(r"\bpthread_mutex_" + kind + r"\s*\(", clean):
            calls.append(
                {
                    "kind": kind,
                    "line": _line_of(clean, match.start()),
                    "wrapped": bool(
                        re.search(
                            r"if\s*\(\s*pthread_mutex_"
                            + kind
                            + r"\s*\([^)]*\)\s*!=\s*0\s*\)",
                            clean[max(0, match.start() - 80) : match.start() + 160],
                        )
                    ),
                }
            )
    return {
        "mutex": declarations[0],
        "lock_wrapper_line": _line_of(clean, lock_match.start()),
        "unlock_wrapper_line": _line_of(clean, unlock_match.start()),
        "calls": calls,
        "source_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def is_error_checked_sync_case(text: str) -> bool:
    try:
        parse_error_checked_case(text)
    except ValueError:
        return False
    return True


def _edge_acyclic(nodes: Sequence[str], edges: Sequence[Dict]) -> None:
    from .algo.common import topo_sort

    node_set = set(nodes)
    pairs = []
    for edge in edges:
        src, dst = str(edge["src"]), str(edge["dst"])
        if src not in node_set or dst not in node_set:
            raise ValueError(f"error-checked compatibility dangling edge {src}->{dst}")
        if src == dst:
            raise ValueError(f"error-checked compatibility self edge {src}")
        pairs.append((src, dst))
    topo_sort(list(nodes), pairs)


def build_error_checked_views(
    *,
    base_name: str,
    source_text: str,
    base_segments_json: Dict,
    base_dag_json: Dict,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    case = parse_error_checked_case(source_text)
    raw = copy.deepcopy(base_segments_json)
    dag = copy.deepcopy(base_dag_json)
    rows = list(raw.get("segments", []))
    by_id = {str(row["seg_id"]): row for row in rows}
    node_ids = [str(node) for node in dag.get("nodes", [])]
    if len(by_id) != len(rows) or set(by_id) != set(node_ids):
        raise ValueError("error-checked compatibility requires one identity row per base node")
    _edge_acyclic(node_ids, dag.get("edges", []))
    if not any(row.get("kind") == "mutex_cs" for row in rows):
        raise ValueError("error-checked compatibility requires a base mutex segment")
    if not any(edge.get("kind") == "create" for edge in dag.get("edges", [])):
        raise ValueError("error-checked compatibility requires a base create edge")
    if not any(edge.get("kind") == "join" for edge in dag.get("edges", [])):
        raise ValueError("error-checked compatibility requires a base join edge")

    segment_ids = [str(row["seg_id"]) for row in rows]
    mutex_rows = [str(row["seg_id"]) for row in rows if row.get("kind") == "mutex_cs"]
    mappings = copy.deepcopy(raw.get("mappings") or {})
    mappings.update(
        {
            "original_segment_to_execution_segments": {
                sid: [sid] for sid in segment_ids
            },
            "generation_node_to_execution_segments": {
                str(node): [str(base_id)]
                for node, base_id in (raw.get("node_to_segment") or {}).items()
                if str(base_id) in by_id
            },
            "structural_node_to_execution_segments": {
                sid: [sid] for sid in segment_ids
            },
            "error_checked_sync": copy.deepcopy(case),
        }
    )
    lock_groups = [
        {
            "group_id": "LOCKGROUP:" + str(case["mutex"]),
            "mutex_id": str(case["mutex"]),
            "member_segment_ids": mutex_rows,
            "candidate_segment_ids": mutex_rows,
        }
    ]
    common = {
        "schema_version": "1.0",
        "base_name": base_name,
        "level": "level2",
        "rule_name": RULE,
        "source_file": raw.get("source_file"),
        "source_sha256": case["source_sha256"],
        "thread_entry_functions": copy.deepcopy(raw.get("thread_entry_functions", [])),
        "lock_groups": lock_groups,
        "mappings": mappings,
        "capabilities": {
            "scheduler_compatible": True,
            "timing_compatible": False,
            "instrumentation_compatible": False,
            "analysis_only": True,
        },
    }
    struct_segments = dict(
        copy.deepcopy(common), view="struct", segments=copy.deepcopy(rows)
    )
    sched_segments = dict(
        copy.deepcopy(common), view="sched", segments=copy.deepcopy(rows)
    )
    meta = {
        "rule_name": RULE,
        "level": "level2",
        "scheduler_compatible": True,
        "analysis_only": True,
        "mutex": case["mutex"],
        "wrapper_lines": [case["lock_wrapper_line"], case["unlock_wrapper_line"]],
        "notes": "error-checked pthread sync wrappers abstracted over the Level-2 base graph",
    }
    return (
        struct_segments,
        copy.deepcopy(dag),
        sched_segments,
        copy.deepcopy(dag),
        meta,
    )
