"""Core-compatible helper-mutex call views.

The Level-2 base builder already records the mutex critical section for helper
functions (for example ``MU:munge#001`` and ``MU:munge@instance1#001``), but it
does not record the call/return relationship between a helper invocation and
its caller.  This module injects that missing relationship into the base
segments and DAG; it does not parse or rebuild a second AST topology.

Accepted shape:

* a helper function with a ``pthread_mutex_t *`` parameter;
* that parameter is used directly by a complete lock/unlock pair;
* the helper is called from ``main`` and/or the direct worker entry;
* every call argument names a statically declared global mutex.

The mutex identity of each helper execution comes from its call argument, not
from a guessed pointer alias.
"""
from __future__ import annotations

import copy
import hashlib
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Sequence, Tuple

from ..core.func_ranges import _strip_comments_and_strings

RULE = "compatibility_helper_mutex"
_CONTROL_WORDS = {"if", "for", "while", "switch", "do", "else", "return"}


def _clean(text: str) -> str:
    return _strip_comments_and_strings(text)


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def _func_base(name: str) -> str:
    return name.split("@", 1)[0]


def _match_brace(text: str, opening: int) -> Optional[int]:
    if opening < 0 or opening >= len(text) or text[opening] != "{":
        return None
    depth = 0
    for index in range(opening, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    return None


def _function_spans(clean: str) -> List[Dict]:
    spans: List[Dict] = []
    pattern = re.compile(
        r"(?m)^[ \t]*(?:static[ \t]+)?(?:[A-Za-z_]\w*[ \t]+)+"
        r"[ \t]*\*?[ \t]*([A-Za-z_]\w*)[ \t]*\(([^;{}]*)\)[ \t]*\{"
    )
    for match in pattern.finditer(clean):
        name = match.group(1)
        if name in _CONTROL_WORDS:
            continue
        opening = clean.find("{", match.end() - 1)
        closing = _match_brace(clean, opening)
        if closing is None:
            continue
        spans.append(
            {
                "name": name,
                "params": match.group(2),
                "start": match.start(),
                "body_start": opening,
                "body_end": closing,
                "signature_end": opening,
            }
        )
    return spans


def _containing_function(spans: Sequence[Dict], offset: int) -> Optional[str]:
    for span in spans:
        if span["start"] <= offset <= span["body_end"]:
            return span["name"]
    return None


def parse_helper_mutex_case(text: str) -> Dict:
    clean = _clean(text)
    spans = _function_spans(clean)
    helper: Optional[Dict] = None
    param: Optional[str] = None
    for span in spans:
        match = re.search(r"pthread_mutex_t\s*\*\s*([A-Za-z_]\w*)", span["params"])
        if match is None:
            continue
        parameter = match.group(1)
        body = clean[span["body_start"] : span["body_end"] + 1]
        lock_re = re.compile(
            r"pthread_mutex_lock\s*\(\s*(?:&\s*)?" + re.escape(parameter) + r"\s*\)"
        )
        unlock_re = re.compile(
            r"pthread_mutex_unlock\s*\(\s*(?:&\s*)?" + re.escape(parameter) + r"\s*\)"
        )
        if lock_re.search(body) and unlock_re.search(body):
            helper = span
            param = parameter
            break
    if helper is None or param is None:
        raise ValueError(
            "helper-mutex compatibility requires a pthread_mutex_t * lock/unlock helper"
        )

    declarations = OrderedDict(
        (name, _line_of(clean, match.start()))
        for match in re.finditer(
            r"^\s*(?:static\s+)?pthread_mutex_t\s+([A-Za-z_]\w*)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;",
            clean,
            re.M,
        )
        for name in [match.group(1)]
    )
    call_pattern = re.compile(
        r"\b" + re.escape(helper["name"]) + r"\s*\(\s*&\s*([A-Za-z_]\w*)\s*\)"
    )
    calls: List[Dict] = []
    for match in call_pattern.finditer(clean):
        if helper["start"] <= match.start() <= helper["body_end"]:
            continue
        caller = _containing_function(spans, match.start())
        if caller is None or caller == helper["name"]:
            continue
        mutex = match.group(1)
        if mutex not in declarations:
            raise ValueError(
                f"helper-mutex compatibility requires global mutex argument, got {mutex}"
            )
        calls.append(
            {
                "caller": caller,
                "mutex": mutex,
                "line": _line_of(clean, match.start()),
                "offset": match.start(),
            }
        )
    if not calls:
        raise ValueError("helper-mutex compatibility requires at least one helper call")
    for name in declarations:
        if not re.search(r"\b" + re.escape(name) + r"\b", clean):
            continue
    return {
        "helper": helper["name"],
        "parameter": param,
        "calls": calls,
        "mutexes": list(declarations),
        "source_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def is_helper_mutex_case(text: str) -> bool:
    clean = _clean(text)
    if not re.search(r"pthread_mutex_t\s*\*\s*[A-Za-z_]\w*", clean):
        return False
    try:
        case = parse_helper_mutex_case(text)
    except ValueError:
        return False
    return bool(case["calls"])


def _row_covering(rows: Sequence[Dict], line: int) -> Optional[Dict]:
    for row in rows:
        if int(row["start_line"]) <= line <= int(row["end_line"]):
            return row
    return None


def _row_at_or_after(rows: Sequence[Dict], line: int) -> Optional[Dict]:
    candidates = [row for row in rows if int(row["start_line"]) >= line]
    if not candidates:
        return None
    return min(candidates, key=lambda row: (int(row["start_line"]), int(row["end_line"])))


def _clone_helper(row: Dict, new_function: str) -> Dict:
    clone = copy.deepcopy(row)
    old_id = str(row["seg_id"])
    old_function = str(row["function"])
    clone["seg_id"] = old_id.replace(old_function, new_function, 1)
    if clone["seg_id"] == old_id:
        clone["seg_id"] = f"{old_id}@{new_function}"
    clone["function"] = new_function
    clone["source_segment"] = old_id
    clone["source_segments"] = [old_id]
    clone["helper_instance"] = new_function
    return clone


def _edge(src: str, dst: str, kind: str, **extra) -> Dict:
    out = {"src": src, "dst": dst, "kind": kind}
    out.update(extra)
    return out


def _assign_helper_rows(
    *,
    helper_rows: Sequence[Dict],
    main_calls: Sequence[Dict],
    worker_calls: Sequence[Dict],
) -> Dict[Tuple[str, int], Dict]:
    """Assign one base/helper execution row to every call site.

    The generator commonly names the main helper execution ``helper@instanceN``
    and the worker execution ``helper``.  Use that provenance when available;
    otherwise assign rows in source order.  Clones are allowed when the base
    graph has fewer helper executions than source call sites.
    """
    rows = [copy.deepcopy(row) for row in helper_rows]
    assignment: Dict[Tuple[str, int], Dict] = {}
    used = set()

    def take(prefer_instance: bool) -> Optional[Dict]:
        candidates = [
            row
            for index, row in enumerate(rows)
            if index not in used
            and (("@instance" in str(row["function"])) == prefer_instance)
        ]
        if not candidates:
            candidates = [row for index, row in enumerate(rows) if index not in used]
        if not candidates:
            return None
        chosen = candidates[0]
        used.add(rows.index(chosen))
        return chosen

    ordered = list(main_calls) + list(worker_calls)
    for call in ordered:
        is_main = str(call["caller"]) == "main"
        row = take(prefer_instance=is_main)
        if row is None:
            template = rows[-1]
            clone = _clone_helper(
                template, f"{_func_base(str(template['function']))}@site{len(assignment) + 1}"
            )
            rows.append(clone)
            used.add(len(rows) - 1)
            row = clone
        assignment[(str(call["caller"]), int(call["line"]))] = row
    return assignment


def build_helper_mutex_views(
    *,
    base_name: str,
    source_text: str,
    base_segments_json: Dict,
    base_dag_json: Dict,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    case = parse_helper_mutex_case(source_text)
    raw = copy.deepcopy(base_segments_json)
    dag = copy.deepcopy(base_dag_json)
    bases = list(raw.get("segments", []))
    by_id = {str(row["seg_id"]): row for row in bases}
    if len(by_id) != len(bases) or set(by_id) != set(str(n) for n in dag.get("nodes", [])):
        raise ValueError("helper-mutex compatibility requires one identity row per base node")

    clean = _clean(source_text)
    create_match = re.search(
        r"\bpthread_create\s*\(\s*&?\s*[A-Za-z_]\w*\s*,\s*[^,]+,\s*([A-Za-z_]\w*)",
        clean,
    )
    join_match = re.search(r"\bpthread_join\s*\(", clean)
    if create_match is None or join_match is None:
        raise ValueError("helper-mutex compatibility requires one direct create/join pair")
    entry = create_match.group(1)
    create_line = _line_of(clean, create_match.start())
    join_line = _line_of(clean, join_match.start())

    main_rows = [row for row in bases if _func_base(str(row["function"])) == "main"]
    worker_rows = [row for row in bases if _func_base(str(row["function"])) == entry]
    helper_rows_all = [
        row for row in bases if _func_base(str(row["function"])) == case["helper"]
    ]
    if not main_rows or not worker_rows or not helper_rows_all:
        raise ValueError(
            "helper-mutex compatibility requires base segments for main, worker, and helper"
        )
    create_row = _row_covering(main_rows, create_line)
    join_row = _row_covering(main_rows, join_line)
    if create_row is None or join_row is None:
        raise ValueError("helper-mutex compatibility requires base create/join carriers")

    main_calls = [call for call in case["calls"] if _func_base(str(call["caller"])) == "main"]
    worker_calls = [
        call for call in case["calls"] if _func_base(str(call["caller"])) == entry
    ]
    if not main_calls or not worker_calls:
        raise ValueError(
            "helper-mutex compatibility requires one helper call in main and one in the worker"
        )
    if len(main_calls) != 1 or len(worker_calls) != 1:
        raise ValueError(
            "helper-mutex compatibility currently requires one call per caller execution"
        )

    assignment = _assign_helper_rows(
        helper_rows=helper_rows_all,
        main_calls=main_calls,
        worker_calls=worker_calls,
    )
    main_call = main_calls[0]
    worker_call = worker_calls[0]
    main_call_row = _row_covering(main_rows, int(main_call["line"]))
    worker_call_row = _row_covering(worker_rows, int(worker_call["line"]))
    if main_call_row is None or worker_call_row is None:
        raise ValueError("helper-mutex compatibility requires call-site carrier segments")

    main_helper = assignment[("main", int(main_call["line"]))]
    worker_helper = assignment[(str(worker_call["caller"]), int(worker_call["line"]))]
    main_helper["mutex_id"] = main_call["mutex"]
    main_helper["helper_caller"] = "main"
    main_helper["helper_call_line"] = int(main_call["line"])
    worker_helper["mutex_id"] = worker_call["mutex"]
    worker_helper["helper_caller"] = entry
    worker_helper["helper_call_line"] = int(worker_call["line"])

    # Keep only the base rows used by this execution view.  Helper rows beyond
    # the two source call sites are not published as disconnected nodes.
    helper_keep = {str(main_helper["seg_id"]), str(worker_helper["seg_id"])}
    final_rows: List[Dict] = []
    for row in bases:
        row_id = str(row["seg_id"])
        if _func_base(str(row["function"])) == case["helper"] and row_id not in helper_keep:
            continue
        if row_id == str(main_helper["seg_id"]):
            final_rows.append(main_helper)
        elif row_id == str(worker_helper["seg_id"]):
            final_rows.append(worker_helper)
        else:
            final_rows.append(copy.deepcopy(row))
    final_by_id = {str(row["seg_id"]): row for row in final_rows}
    if str(main_helper["seg_id"]) not in final_by_id:
        final_rows.append(main_helper)
        final_by_id[str(main_helper["seg_id"])] = main_helper
    if str(worker_helper["seg_id"]) not in final_by_id:
        final_rows.append(worker_helper)
        final_by_id[str(worker_helper["seg_id"])] = worker_helper

    worker_tail: Optional[str] = None
    for edge in dag.get("edges", []):
        if (
            edge.get("kind") == "join"
            and str(edge.get("dst")) == str(join_row["seg_id"])
            and str(edge.get("src")) in {str(row["seg_id"]) for row in worker_rows}
        ):
            worker_tail = str(edge["src"])
            break
    if worker_tail is None:
        worker_tail = str(worker_call_row["seg_id"])

    removed_keys = {
        (str(main_call_row["seg_id"]), str(join_row["seg_id"]), "intra"),
        (str(worker_tail), str(join_row["seg_id"]), "join"),
        (str(worker_call_row["seg_id"]), str(join_row["seg_id"]), "join"),
    }
    edges: List[Dict] = []
    for edge in dag.get("edges", []):
        key = (str(edge.get("src")), str(edge.get("dst")), str(edge.get("kind")))
        if key in removed_keys:
            continue
        if str(edge.get("src")) not in final_by_id or str(edge.get("dst")) not in final_by_id:
            continue
        edges.append(copy.deepcopy(edge))

    # Main call: call edge to its helper execution, then return to the join
    # carrier.  Worker call: call edge to its helper execution, then the helper
    # execution is the worker tail that supplies the join edge.
    edges.extend(
        [
            _edge(
                str(main_call_row["seg_id"]),
                str(main_helper["seg_id"]),
                "call",
                helper=case["helper"],
                mutex_id=main_call["mutex"],
                source_line=int(main_call["line"]),
                provenance="helper-mutex compatibility call",
            ),
            _edge(
                str(main_helper["seg_id"]),
                str(join_row["seg_id"]),
                "return",
                helper=case["helper"],
                mutex_id=main_call["mutex"],
                source_line=int(main_call["line"]),
                provenance="helper-mutex compatibility return",
            ),
            _edge(
                str(worker_call_row["seg_id"]),
                str(worker_helper["seg_id"]),
                "call",
                helper=case["helper"],
                mutex_id=worker_call["mutex"],
                source_line=int(worker_call["line"]),
                provenance="helper-mutex compatibility call",
            ),
            _edge(
                str(worker_helper["seg_id"]),
                str(join_row["seg_id"]),
                "join",
                helper=case["helper"],
                mutex_id=worker_call["mutex"],
                thread_instance=entry,
                source_line=join_line,
                provenance="worker helper execution is the join tail",
            ),
        ]
    )

    # Deduplicate preserving first occurrence.
    deduped: List[Dict] = []
    seen = set()
    for edge in edges:
        key = (str(edge["src"]), str(edge["dst"]), str(edge.get("kind")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(edge)

    helper_calls = [
        {
            "caller": "main",
            "call_site_segment": str(main_call_row["seg_id"]),
            "helper_segment": str(main_helper["seg_id"]),
            "mutex_id": main_call["mutex"],
            "line": int(main_call["line"]),
        },
        {
            "caller": entry,
            "call_site_segment": str(worker_call_row["seg_id"]),
            "helper_segment": str(worker_helper["seg_id"]),
            "mutex_id": worker_call["mutex"],
            "line": int(worker_call["line"]),
        },
    ]
    mutex_identity: Dict[str, Dict] = {}
    for call, helper in (
        (main_call, main_helper),
        (worker_call, worker_helper),
    ):
        group = mutex_identity.setdefault(
            str(call["mutex"]),
            {"mutex_id": str(call["mutex"]), "helper_segment_ids": []},
        )
        group["helper_segment_ids"].append(str(helper["seg_id"]))

    lock_groups = [
        {
            "group_id": "LOCKGROUP:" + mutex,
            "mutex_id": mutex,
            "member_segment_ids": list(payload["helper_segment_ids"]),
            "candidate_segment_ids": list(payload["helper_segment_ids"]),
        }
        for mutex, payload in mutex_identity.items()
    ]

    original_to_execution: Dict[str, List[str]] = {}
    for row in bases:
        row_id = str(row["seg_id"])
        if row_id in final_by_id:
            original_to_execution[row_id] = [row_id]
    for helper in (main_helper, worker_helper):
        source_id = str(helper.get("source_segment") or helper["seg_id"])
        original_to_execution.setdefault(source_id, [])
        if str(helper["seg_id"]) not in original_to_execution[source_id]:
            original_to_execution[source_id].append(str(helper["seg_id"]))

    source_positions: Dict[str, List[str]] = {}
    for row in final_rows:
        for line in range(int(row["start_line"]), int(row["end_line"]) + 1):
            source_positions.setdefault(
                f"SRC:{_func_base(str(row['function']))}:line:{line}", []
            ).append(str(row["seg_id"]))
    for call, helper in ((main_call, main_helper), (worker_call, worker_helper)):
        source_positions[f"SRC:{call['caller']}:line:{call['line']}"] = [str(helper["seg_id"])]
        for line in range(int(helper["start_line"]), int(helper["end_line"]) + 1):
            key = f"SRC:{case['helper']}:line:{line}"
            source_positions.setdefault(key, [])
            if str(helper["seg_id"]) not in source_positions[key]:
                source_positions[key].append(str(helper["seg_id"]))

    generation_map: Dict[str, List[str]] = {}
    for node, base_row_id in (raw.get("node_to_segment") or {}).items():
        if str(base_row_id) in final_by_id:
            generation_map[str(node)] = [str(base_row_id)]

    thread_instances = {
        entry: {
            "entry": str(worker_rows[0]["seg_id"]),
            "exit": str(worker_helper["seg_id"]),
            "create_segment": str(create_row["seg_id"]),
            "join_segment": str(join_row["seg_id"]),
            "source_function": entry,
            "helper_exit": str(worker_helper["seg_id"]),
        }
    }
    mappings = copy.deepcopy(raw.get("mappings") or {})
    mappings.update(
        {
            "original_segment_to_execution_segments": original_to_execution,
            "generation_node_to_execution_segments": generation_map,
            "source_position_to_execution_segments": source_positions,
            "structural_node_to_execution_segments": {
                str(row["seg_id"]): [str(row["seg_id"])] for row in final_rows
            },
            "helper_calls": helper_calls,
            "helper_call_to_execution_segments": {
                str(call["call_site_segment"]): [str(call["helper_segment"])]
                for call in helper_calls
            },
            "mutex_identity": mutex_identity,
            "thread_instances": copy.deepcopy(thread_instances),
        }
    )
    common = {
        "schema_version": "1.0",
        "base_name": base_name,
        "level": "level2",
        "rule_name": RULE,
        "source_file": raw.get("source_file"),
        "source_sha256": case["source_sha256"],
        "thread_entry_functions": copy.deepcopy(raw.get("thread_entry_functions", [])),
        "thread_instances": copy.deepcopy(thread_instances),
        "lock_groups": lock_groups,
        "mappings": mappings,
        "capabilities": {
            "scheduler_compatible": True,
            "timing_compatible": False,
            "instrumentation_compatible": False,
            "analysis_only": True,
        },
    }
    dag_payload = {
        "schema_version": "1.0",
        "base_name": base_name,
        "nodes": [str(row["seg_id"]) for row in final_rows],
        "edges": deduped,
    }
    meta = {
        "rule_name": RULE,
        "level": "level2",
        "scheduler_compatible": True,
        "analysis_only": True,
        "helper": case["helper"],
        "helper_calls": len(helper_calls),
        "mutexes": len(mutex_identity),
        "notes": "helper call/return edges injected into the Level-2 base graph",
    }
    return (
        dict(copy.deepcopy(common), view="struct", segments=final_rows),
        copy.deepcopy(dag_payload),
        dict(copy.deepcopy(common), view="sched", segments=copy.deepcopy(final_rows)),
        copy.deepcopy(dag_payload),
        meta,
    )
