"""Core-compatible expansion for statically bounded thread arrays.

This module does not build a second source/AST graph.  It consumes the Level-2
segments and DAG produced by ``build_level2_segments_and_dag`` and expands the
single worker execution recorded by that base graph into the statically known
number of loop iterations.  The base graph remains the source of truth for
segment ownership, source ranges, create/join carriers, and mutex segments.

The supported shape is deliberately narrow:

* ``pthread_t name[<literal N>]``;
* create and join loops with the same literal trip count ``N``;
* one direct worker entry function;
* a create carrier and a join carrier already present in the base graph.

Anything else is rejected instead of guessed.
"""
from __future__ import annotations

import copy
import hashlib
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from ..core.func_ranges import _strip_comments_and_strings

RULE = "compatibility_thread_array"


def _clean(text: str) -> str:
    return _strip_comments_and_strings(text)


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def _func_base(name: str) -> str:
    return name.split("@", 1)[0]


def _macro_values(text: str) -> Dict[str, int]:
    """Resolve only the simple literal ``#define NAME N`` form."""
    clean = _clean(text)
    values: Dict[str, int] = {}
    for match in re.finditer(
        r"^\s*#\s*define\s+([A-Za-z_]\w*)\s+(\d+)\s*$", clean, re.M
    ):
        values[match.group(1)] = int(match.group(2))
    return values


def _resolve_count(token: str, macros: Dict[str, int]) -> Optional[int]:
    if token.isdigit():
        return int(token)
    return macros.get(token)


def _local_int_aliases(clean: str, macros: Dict[str, int]) -> Dict[str, int]:
    """Resolve simple local aliases such as ``children = CHILDREN;``."""
    values = dict(macros)
    assignment = re.compile(r"\b([A-Za-z_]\w*)\s*=\s*(\d+|[A-Za-z_]\w*)\s*;")
    for _ in range(4):
        changed = False
        for match in assignment.finditer(clean):
            name, token = match.group(1), match.group(2)
            value = _resolve_count(token, values)
            if value is not None and values.get(name) != value:
                values[name] = value
                changed = True
        if not changed:
            break
    return values


def _array_decl(text: str) -> Optional[Tuple[str, str, bool]]:
    match = re.search(
        r"\bpthread_t\s*(\*)?\s*([A-Za-z_]\w*)\s*\[\s*(\d+|[A-Za-z_]\w*)\s*\]",
        _clean(text),
    )
    if match is None:
        return None
    return match.group(2), match.group(3), bool(match.group(1))


def _parse_loop_count(header: str, macros: Dict[str, int]) -> Optional[int]:
    match = re.search(
        r"([A-Za-z_]\w*)\s*=\s*(\d+)\s*;\s*\1\s*(<=|<)\s*(\d+|[A-Za-z_]\w*)",
        header,
    )
    if match is None:
        return None
    start = int(match.group(2))
    bound = _resolve_count(match.group(4), macros)
    if bound is None:
        return None
    if match.group(3) == "<=":
        return bound - start + 1
    return bound - start


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


def _loop_call(
    text: str, array_name: str, callee: str, macros: Dict[str, int]
) -> Optional[Tuple[int, int, int]]:
    """Return (source line of call, loop count, call offset)."""
    clean = _clean(text)
    array_arg = (
        r"(?:&\s*|\*\s*)?" + re.escape(array_name) + r"\s*\["
    )
    call_pattern = re.compile(re.escape(callee) + r"\s*\(\s*" + array_arg)
    for header_match in re.finditer(r"\bfor\s*\(([^)]*)\)", clean):
        count = _parse_loop_count(header_match.group(1), macros)
        if count is None:
            continue
        cursor = header_match.end()
        while cursor < len(clean) and clean[cursor].isspace():
            cursor += 1
        if cursor < len(clean) and clean[cursor] == "{":
            closing = _match_brace(clean, cursor)
            if closing is None:
                continue
            body = clean[cursor : closing + 1]
            body_offset = cursor
        else:
            # Unbraced single statement: bounded by the next semicolon.
            semicolon = clean.find(";", cursor)
            if semicolon < 0:
                continue
            body = clean[cursor : semicolon + 1]
            body_offset = cursor
        call = call_pattern.search(body)
        if call is None:
            continue
        call_offset = body_offset + call.start()
        return _line_of(clean, call_offset), count, call_offset
    return None


def parse_thread_array_case(text: str) -> Dict:
    """Parse the supported public thread-array shape.

    Raises ``ValueError`` with a compatibility-boundary message when the source
    does not have one unambiguous static expansion.
    """
    decl = _array_decl(text)
    if decl is None:
        raise ValueError(
            "thread-array compatibility requires pthread_t or pthread_t * name[N]"
        )
    array_name, count_token, pointer_array = decl
    clean = _clean(text)
    if re.search(r"^\s*#\s*(?!include\b|define\b)\w+", clean, re.M):
        raise ValueError(
            "thread-array compatibility rejects active preprocessor directives other than #define/#include"
        )
    macros = _local_int_aliases(clean, _macro_values(text))
    array_count = _resolve_count(count_token, macros)
    if array_count is None:
        raise ValueError(
            "thread-array compatibility requires a literal array size or simple #define"
        )

    create_calls = re.findall(r"\bpthread_create\s*\(", clean)
    join_calls = re.findall(r"\bpthread_join\s*\(", clean)
    if not create_calls or not join_calls:
        raise ValueError(
            "thread-array compatibility requires a create call and a join call"
        )

    create = _loop_call(text, array_name, "pthread_create", macros)
    join = _loop_call(text, array_name, "pthread_join", macros)
    if create is None or join is None:
        raise ValueError(
            "thread-array compatibility requires create and join inside static loops"
        )
    if create[1] != join[1]:
        raise ValueError(
            "thread-array compatibility requires equal create and join trip counts"
        )
    if create[1] != array_count:
        raise ValueError(
            "thread-array compatibility requires array size == create/join trip count"
        )
    if not 1 <= array_count <= 1024:
        raise ValueError("thread-array compatibility requires 1 <= N <= 1024")

    entry_match = re.search(
        r"\bpthread_create\s*\(\s*(?:&\s*|\*\s*)?"
        + re.escape(array_name)
        + r"\s*\[[^\]]*\]\s*,\s*[^,]+,\s*([A-Za-z_]\w*)",
        clean,
    )
    if entry_match is None:
        raise ValueError("thread-array compatibility requires a direct worker entry name")
    entry = entry_match.group(1)

    mutexes = re.findall(
        r"^\s*(?:static\s+)?pthread_mutex_t\s+([A-Za-z_]\w*)\s*=\s*PTHREAD_MUTEX_INITIALIZER\s*;",
        clean,
        re.M,
    )
    return {
        "array_name": array_name,
        "count": array_count,
        "pointer_array": pointer_array,
        "create_line": create[0],
        "join_line": join[0],
        "entry": entry,
        "mutexes": list(OrderedDict.fromkeys(mutexes)),
        "source_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def is_thread_array_case(text: str) -> bool:
    """Cheap, non-throwing selector predicate for the static shape."""
    try:
        parse_thread_array_case(text)
    except ValueError:
        return False
    return True


def _row_covering(rows: List[Dict], line: int) -> Optional[Dict]:
    for row in rows:
        if int(row["start_line"]) <= line <= int(row["end_line"]):
            return row
    return None


def _clone_row(row: Dict, *, new_function: str, suffix: str) -> Dict:
    clone = copy.deepcopy(row)
    old_id = str(row["seg_id"])
    old_function = str(row["function"])
    new_id = old_id.replace(old_function, new_function, 1)
    if new_id == old_id:
        new_id = f"{old_id}{suffix}"
    clone["seg_id"] = new_id
    clone["function"] = new_function
    clone["source_segment"] = old_id
    clone["source_segments"] = [old_id]
    clone["thread_instance"] = new_function
    return clone


def _edge(src: str, dst: str, kind: str, **extra) -> Dict:
    out = {"src": src, "dst": dst, "kind": kind}
    out.update(extra)
    return out


def build_thread_array_views(
    *,
    base_name: str,
    source_text: str,
    base_segments_json: Dict,
    base_dag_json: Dict,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    """Expand base worker execution into static loop iterations."""
    info = parse_thread_array_case(source_text)
    raw = copy.deepcopy(base_segments_json)
    dag = copy.deepcopy(base_dag_json)
    bases = list(raw.get("segments", []))
    by_id = {str(row["seg_id"]): row for row in bases}
    node_ids = [str(node) for node in dag.get("nodes", [])]
    if len(by_id) != len(bases) or set(by_id) != set(node_ids):
        raise ValueError("thread-array compatibility requires one identity row per base node")

    entry = str(info["entry"])
    main_rows = [row for row in bases if _func_base(str(row["function"])) == "main"]
    worker_rows = [row for row in bases if _func_base(str(row["function"])) == entry]
    if not main_rows or not worker_rows:
        raise ValueError(
            f"thread-array compatibility requires base segments for main and worker {entry}"
        )
    if len({str(row["function"]) for row in worker_rows}) != 1:
        raise ValueError(
            "thread-array compatibility requires one base execution of the worker"
        )

    create_row = _row_covering(main_rows, int(info["create_line"]))
    join_row = _row_covering(main_rows, int(info["join_line"]))
    if create_row is None or join_row is None:
        raise ValueError(
            "thread-array compatibility requires create/join carriers in base segments"
        )
    if create_row["seg_id"] == join_row["seg_id"]:
        raise ValueError(
            "thread-array compatibility requires distinct create and join carriers"
        )

    main_id_set = {str(row["seg_id"]) for row in main_rows}
    worker_id_set = {str(row["seg_id"]) for row in worker_rows}
    final_rows: List[Dict] = [copy.deepcopy(row) for row in main_rows]

    base_worker_edges = [
        edge
        for edge in dag.get("edges", [])
        if str(edge.get("src")) in worker_id_set and str(edge.get("dst")) in worker_id_set
    ]
    base_main_edges = [
        edge
        for edge in dag.get("edges", [])
        if str(edge.get("src")) in main_id_set and str(edge.get("dst")) in main_id_set
    ]

    thread_instances: Dict[str, Dict] = {}
    create_edges: List[Dict] = []
    join_edges: List[Dict] = []
    worker_edges: List[Dict] = []
    original_to_execution: Dict[str, List[str]] = {
        str(row["seg_id"]): [str(row["seg_id"])] for row in main_rows
    }

    for iteration in range(int(info["count"])):
        suffix = f"@iter{iteration}"
        new_function = f"{entry}{suffix}"
        clones: List[Dict] = []
        id_map: Dict[str, str] = {}
        for row in worker_rows:
            clone = _clone_row(row, new_function=new_function, suffix=suffix)
            clone["iteration"] = iteration
            clone["thread_handle"] = f"{info['array_name']}[{iteration}]"
            clones.append(clone)
            id_map[str(row["seg_id"])] = str(clone["seg_id"])
        final_rows.extend(clones)
        original_to_execution.setdefault(str(worker_rows[0]["seg_id"]), [])
        for row in worker_rows:
            original_to_execution.setdefault(str(row["seg_id"]), [])
            original_to_execution[str(row["seg_id"])].append(id_map[str(row["seg_id"])])

        first_id = str(clones[0]["seg_id"])
        last_id = str(clones[-1]["seg_id"])
        create_edges.append(
            _edge(
                str(create_row["seg_id"]),
                first_id,
                "create",
                thread_handle=f"{info['array_name']}[{iteration}]",
                thread_instance=new_function,
                source_line=int(info["create_line"]),
                provenance="thread-array compatibility expansion",
            )
        )
        join_edges.append(
            _edge(
                last_id,
                str(join_row["seg_id"]),
                "join",
                thread_handle=f"{info['array_name']}[{iteration}]",
                thread_instance=new_function,
                source_line=int(info["join_line"]),
                provenance="thread-array compatibility expansion",
            )
        )
        for edge in base_worker_edges:
            worker_edges.append(
                dict(
                    copy.deepcopy(edge),
                    src=id_map[str(edge["src"])],
                    dst=id_map[str(edge["dst"])],
                    provenance="base intra edge cloned per iteration",
                )
            )
        thread_instances[new_function] = {
            "entry": first_id,
            "exit": last_id,
            "create_segment": str(create_row["seg_id"]),
            "join_segment": str(join_row["seg_id"]),
            "source_function": entry,
            "iteration": iteration,
            "thread_handle": f"{info['array_name']}[{iteration}]",
        }

    edges: List[Dict] = []
    seen_edges = set()
    for edge in base_main_edges + create_edges + join_edges + worker_edges:
        key = (str(edge["src"]), str(edge["dst"]), str(edge.get("kind")))
        if key in seen_edges:
            continue
        seen_edges.add(key)
        edges.append(edge)

    generation_map: Dict[str, List[str]] = {}
    for node, loc in (raw.get("node_locations") or {}).items():
        if not isinstance(loc, dict):
            continue
        if _func_base(str(loc.get("function", ""))) == entry:
            generation_map[str(node)] = [
                str(row["seg_id"])
                for row in final_rows
                if _func_base(str(row["function"])) == entry
            ]
            continue
        base_segment = (raw.get("node_to_segment") or {}).get(str(node))
        if str(base_segment) in main_id_set:
            generation_map[str(node)] = [str(base_segment)]

    source_positions: Dict[str, List[str]] = {}
    for row in final_rows:
        for line in range(int(row["start_line"]), int(row["end_line"]) + 1):
            source_positions[f"SRC:{_func_base(str(row['function']))}:line:{line}"] = [str(row["seg_id"])]
    for row in worker_rows:
        for clone in final_rows:
            if clone.get("source_segment") == str(row["seg_id"]):
                for line in range(int(row["start_line"]), int(row["end_line"]) + 1):
                    key = f"SRC:{entry}:line:{line}"
                    source_positions.setdefault(key, [])
                    if str(clone["seg_id"]) not in source_positions[key]:
                        source_positions[key].append(str(clone["seg_id"]))

    mutex_rows = [
        str(row["seg_id"]) for row in final_rows if row.get("kind") == "mutex_cs"
    ]
    mutexes = list(info.get("mutexes") or [])
    mutex_identity = {
        mutex: {"mutex_id": mutex, "segment_ids": list(mutex_rows)}
        for mutex in mutexes
    }
    lock_groups = []
    for mutex in mutexes:
        lock_groups.append(
            {
                "group_id": "LOCKGROUP:" + mutex,
                "mutex_id": mutex,
                "member_segment_ids": list(mutex_rows),
                "candidate_segment_ids": list(mutex_rows),
            }
        )

    mappings = copy.deepcopy(raw.get("mappings") or {})
    mappings.update(
        {
            "original_segment_to_execution_segments": original_to_execution,
            "generation_node_to_execution_segments": generation_map,
            "source_position_to_execution_segments": source_positions,
            "structural_node_to_execution_segments": {
                str(row["seg_id"]): [str(row["seg_id"])] for row in final_rows
            },
            "thread_instances": copy.deepcopy(thread_instances),
            "thread_array": {
                "name": info["array_name"],
                "count": int(info["count"]),
                "worker_entry": entry,
                "create_segment": str(create_row["seg_id"]),
                "join_segment": str(join_row["seg_id"]),
            },
            "mutex_identity": mutex_identity,
        }
    )
    common = {
        "schema_version": "1.0",
        "base_name": base_name,
        "level": "level2",
        "rule_name": RULE,
        "source_file": raw.get("source_file"),
        "source_sha256": info["source_sha256"],
        "thread_entry_functions": copy.deepcopy(raw.get("thread_entry_functions", [])),
        "thread_instances": copy.deepcopy(thread_instances),
        "thread_array": copy.deepcopy(mappings["thread_array"]),
        "lock_groups": lock_groups,
        "mappings": mappings,
        "capabilities": {
            "scheduler_compatible": True,
            "timing_compatible": False,
            "instrumentation_compatible": False,
            "analysis_only": True,
        },
    }
    node_list = [str(row["seg_id"]) for row in final_rows]
    dag_payload = {
        "schema_version": "1.0",
        "base_name": base_name,
        "nodes": node_list,
        "edges": edges,
    }
    meta = {
        "rule_name": RULE,
        "level": "level2",
        "scheduler_compatible": True,
        "analysis_only": True,
        "thread_instances": len(thread_instances),
        "create_edges": len(create_edges),
        "join_edges": len(join_edges),
        "notes": "static thread-array expansion over the Level-2 base segments",
    }
    struct_segments = dict(copy.deepcopy(common), view="struct", segments=final_rows)
    sched_segments = dict(copy.deepcopy(common), view="sched", segments=copy.deepcopy(final_rows))
    return (
        struct_segments,
        copy.deepcopy(dag_payload),
        sched_segments,
        copy.deepcopy(dag_payload),
        meta,
    )
