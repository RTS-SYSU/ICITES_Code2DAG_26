from __future__ import annotations

"""Phase-1 source model for structured C ``if``/``else`` constructs.

The structural view uses paired entry/exit nodes around mutually exclusive
branches.  The scheduling view keeps the executable nodes of each branch
separate, while branch selection remains controlled by the original C
condition at runtime.
"""

import bisect
import copy
import json
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .constants import SCHEMA_VERSION
from ..core.func_ranges import normalize_func_name


class ConditionModelError(ValueError):
    pass


@dataclass
class Branch:
    branch_id: str
    guard: str
    start_offset: Optional[int]
    end_offset: Optional[int]
    start_line: Optional[int] = None
    end_line: Optional[int] = None


@dataclass
class ConditionalRegion:
    condition_id: str
    function: str
    start_offset: int
    end_offset: int
    entry_line: int
    exit_line: int
    branches: List[Branch]
    parent_id: Optional[str] = None
    parent_branch_id: Optional[str] = None
    condition_text: str = ""


@dataclass
class Analysis:
    regions: List[ConditionalRegion]
    diagnostics: List[Dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class _Statement:
    start: int
    end: int
    content_start: Optional[int]
    content_end: Optional[int]


def _sanitize_c(text: str) -> str:
    """Hide comments, literals, and preprocessor directives, preserving offsets."""

    out = list(text)
    i = 0
    state = "normal"
    line_start = True
    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""
        if state == "line_comment":
            if ch == "\\" and nxt == "\n":
                out[i] = " "
                i += 2
                continue
            if ch == "\\" and nxt == "\r" and text[i + 2:i + 3] == "\n":
                out[i] = " "
                i += 3
                continue
            if ch == "\n":
                state = "normal"
                line_start = True
            else:
                out[i] = " "
            i += 1
            continue
        if state == "block_comment":
            if ch == "*" and nxt == "/":
                out[i] = out[i + 1] = " "
                i += 2
                state = "normal"
            else:
                if ch != "\n":
                    out[i] = " "
                i += 1
            continue
        if state in {"string", "char"}:
            quote = '"' if state == "string" else "'"
            if ch == "\\" and i + 1 < len(text):
                out[i] = " "
                out[i + 1] = nxt if nxt == "\n" or (nxt == "\r" and text[i + 2:i + 3] == "\n") else " "
                i += 2
                continue
            if ch == quote:
                out[i] = " "
                state = "normal"
            elif ch != "\n":
                out[i] = " "
            i += 1
            continue

        if ch == "\n":
            line_start = True
            i += 1
            continue
        if line_start and ch.isspace():
            i += 1
            continue
        if line_start and ch == "#":
            while i < len(text) and text[i] != "\n":
                out[i] = " "
                i += 1
            continue
        line_start = False
        if ch == "/" and nxt == "/":
            out[i] = out[i + 1] = " "
            i += 2
            state = "line_comment"
        elif ch == "/" and nxt == "*":
            out[i] = out[i + 1] = " "
            i += 2
            state = "block_comment"
        elif ch == '"':
            out[i] = " "
            i += 1
            state = "string"
        elif ch == "'":
            out[i] = " "
            i += 1
            state = "char"
        else:
            i += 1
    return "".join(out)


def _line_starts(text: str) -> List[int]:
    starts = [0]
    starts.extend(i + 1 for i, ch in enumerate(text) if ch == "\n")
    return starts


def _line_of(starts: Sequence[int], offset: int) -> int:
    return bisect.bisect_right(starts, max(0, offset))


def _skip_ws(text: str, pos: int) -> int:
    while pos < len(text) and text[pos].isspace():
        pos += 1
    return pos


def _keyword_at(text: str, pos: int, keyword: str) -> bool:
    if not text.startswith(keyword, pos):
        return False
    before = text[pos - 1] if pos > 0 else ""
    after_pos = pos + len(keyword)
    after = text[after_pos] if after_pos < len(text) else ""
    return not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_")


def _match_pair(text: str, pos: int, opening: str, closing: str) -> int:
    if pos >= len(text) or text[pos] != opening:
        raise ConditionModelError(f"expected '{opening}' at offset {pos}")
    depth = 0
    for i in range(pos, len(text)):
        if text[i] == opening:
            depth += 1
        elif text[i] == closing:
            depth -= 1
            if depth == 0:
                return i
    raise ConditionModelError(f"unmatched '{opening}' at offset {pos}")


def _parse_statement(clean: str, pos: int) -> _Statement:
    pos = _skip_ws(clean, pos)
    if pos >= len(clean):
        raise ConditionModelError("missing statement after condition")
    if clean[pos] == "{":
        close = _match_pair(clean, pos, "{", "}")
        return _Statement(pos, close + 1, pos + 1, close)
    if _keyword_at(clean, pos, "if"):
        _condition_close, then_stmt, else_stmt, end = _parse_if(clean, pos)
        _ = _condition_close, then_stmt, else_stmt
        return _Statement(pos, end, pos, end)
    for keyword in ("for", "while", "switch", "do"):
        if _keyword_at(clean, pos, keyword):
            raise ConditionModelError(
                f"phase-1 condition parser requires braces around a branch starting with '{keyword}' at offset {pos}"
            )

    paren = bracket = brace = 0
    i = pos
    while i < len(clean):
        ch = clean[i]
        if ch == "(":
            paren += 1
        elif ch == ")":
            paren = max(0, paren - 1)
        elif ch == "[":
            bracket += 1
        elif ch == "]":
            bracket = max(0, bracket - 1)
        elif ch == "{":
            brace += 1
        elif ch == "}":
            if brace == 0:
                raise ConditionModelError(f"statement has no terminating ';' near offset {pos}")
            brace -= 1
        elif ch == ";" and paren == bracket == brace == 0:
            return _Statement(pos, i + 1, pos, i + 1)
        i += 1
    raise ConditionModelError(f"unterminated statement near offset {pos}")


def _parse_if(clean: str, pos: int) -> Tuple[int, _Statement, Optional[_Statement], int]:
    if not _keyword_at(clean, pos, "if"):
        raise ConditionModelError(f"expected if at offset {pos}")
    cond_open = _skip_ws(clean, pos + 2)
    if cond_open >= len(clean) or clean[cond_open] != "(":
        raise ConditionModelError(f"if at offset {pos} has no parenthesized condition")
    cond_close = _match_pair(clean, cond_open, "(", ")")
    then_stmt = _parse_statement(clean, cond_close + 1)
    cursor = _skip_ws(clean, then_stmt.end)
    else_stmt: Optional[_Statement] = None
    end = then_stmt.end
    if _keyword_at(clean, cursor, "else"):
        else_stmt = _parse_statement(clean, cursor + 4)
        end = else_stmt.end
    # Looking ahead for else must not consume the next statement's line.
    return cond_close, then_stmt, else_stmt, end


def _content_lines(clean: str, starts: Sequence[int], stmt: Optional[_Statement]) -> Tuple[Optional[int], Optional[int]]:
    if stmt is None or stmt.content_start is None or stmt.content_end is None:
        return None, None
    left = stmt.content_start
    right = stmt.content_end
    while left < right and clean[left].isspace():
        left += 1
    while right > left and clean[right - 1].isspace():
        right -= 1
    if left >= right:
        return None, None
    return _line_of(starts, left), _line_of(starts, right - 1)


def analyze_conditionals(source_text: str, function_ranges: Dict[str, Tuple[int, int]]) -> Analysis:
    clean = _sanitize_c(source_text)
    starts = _line_starts(clean)
    parsed: List[ConditionalRegion] = []
    diagnostics: List[Dict[str, object]] = []

    for match in re.finditer(r"\bif\b", clean):
        line = _line_of(starts, match.start())
        function = next((fn for fn, (a, b) in function_ranges.items() if a <= line <= b), None)
        if function is None:
            continue
        try:
            cond_close, then_stmt, else_stmt, end = _parse_if(clean, match.start())
        except ConditionModelError as exc:
            diagnostics.append({"severity": "error", "function": function, "line": line, "message": str(exc)})
            continue
        exit_line = _line_of(starts, max(match.start(), end - 1))
        fn_end = function_ranges[function][1]
        if exit_line > fn_end:
            diagnostics.append(
                {"severity": "error", "function": function, "line": line, "message": "conditional extends beyond modeled function range"}
            )
            continue
        then_start, then_end = _content_lines(clean, starts, then_stmt)
        else_start, else_end = _content_lines(clean, starts, else_stmt)
        parsed.append(
            ConditionalRegion(
                condition_id="",
                function=function,
                start_offset=match.start(),
                end_offset=end,
                entry_line=line,
                exit_line=exit_line,
                condition_text=source_text[clean.find("(", match.start(), cond_close + 1) + 1 : cond_close].strip(),
                branches=[
                    Branch("then", "true", then_stmt.content_start, then_stmt.content_end, then_start, then_end),
                    Branch(
                        "else",
                        "false",
                        else_stmt.content_start if else_stmt else None,
                        else_stmt.content_end if else_stmt else None,
                        else_start,
                        else_end,
                    ),
                ],
            )
        )

    parsed.sort(key=lambda r: (r.function, r.start_offset, -r.end_offset))
    counters: Dict[str, int] = {}
    for region in parsed:
        counters[region.function] = counters.get(region.function, 0) + 1
        region.condition_id = f"COND:{region.function}#{counters[region.function]:03d}"

    for child in parsed:
        candidates: List[Tuple[int, ConditionalRegion, Branch]] = []
        for parent in parsed:
            if parent is child or parent.function != child.function:
                continue
            for branch in parent.branches:
                if branch.start_offset is None or branch.end_offset is None:
                    continue
                if branch.start_offset <= child.start_offset and child.end_offset <= branch.end_offset:
                    candidates.append((parent.end_offset - parent.start_offset, parent, branch))
        if candidates:
            _size, parent, branch = min(candidates, key=lambda item: item[0])
            child.parent_id = parent.condition_id
            child.parent_branch_id = branch.branch_id

    return Analysis(parsed, diagnostics)


def _trim_range(clean_lines: Sequence[str], start: int, end: int) -> Optional[Tuple[int, int]]:
    def effective(line: str) -> bool:
        compact = re.sub(r"\s+", "", line)
        return compact not in {"", "{", "}", "};", "else", "else{"}

    while start <= end and (start < 1 or start > len(clean_lines) or not effective(clean_lines[start - 1])):
        start += 1
    while end >= start and (end < 1 or end > len(clean_lines) or not effective(clean_lines[end - 1])):
        end -= 1
    return (start, end) if start <= end else None


def _overlap(a: int, b: int, c: int, d: int) -> bool:
    return max(a, c) <= min(b, d)


def _dedupe_edges(edges: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    seen = set()
    for edge in edges:
        if edge.get("src") == edge.get("dst"):
            continue
        key = json.dumps(edge, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(edge)
    return out


def build_condition_views(
    *,
    base_name: str,
    source_text: str,
    base_segments_json: Dict,
    base_dag_json: Dict,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    base_segments = [s for s in base_segments_json.get("segments", []) if isinstance(s, dict)]
    ranges: Dict[str, Tuple[int, int]] = {}
    by_fn: Dict[str, List[Dict]] = {}
    for segment in base_segments:
        fn = segment.get("function")
        start = segment.get("start_line")
        end = segment.get("end_line")
        if not isinstance(fn, str) or not isinstance(start, int) or not isinstance(end, int):
            continue
        by_fn.setdefault(fn, []).append(segment)
        if fn not in ranges:
            ranges[fn] = (start, end)
        else:
            ranges[fn] = (min(ranges[fn][0], start), max(ranges[fn][1], end))
    for segments in by_fn.values():
        segments.sort(key=lambda s: (int(s["start_line"]), int(s["end_line"]), str(s["seg_id"])))

    analysis = analyze_conditionals(source_text, ranges)
    # A source condition belongs to every execution instance of the function
    # that contains it.  Ranges here come from the actual base rows, so clones
    # without execution rows are never materialized.  Without this, a cloned
    # worker's branch rows would lose their exclusive-group membership.
    by_span: Dict[Tuple[int, int], List[str]] = {}
    for fn, (a, b) in ranges.items():
        if fn in by_fn:
            by_span.setdefault((a, b), []).append(fn)
    if any(len(names) > 1 for names in by_span.values()):
        expanded: List[ConditionalRegion] = []
        for region in analysis.regions:
            source_name = normalize_func_name(region.function)
            names = [fn for fn in by_span.get(ranges.get(region.function), [])
                     if normalize_func_name(fn) == source_name]
            if region.function not in names:
                expanded.append(region)
                continue
            prefix = "COND:" + region.function + "#"
            for fn in names:
                if fn == region.function:
                    clone = region
                else:
                    clone = copy.deepcopy(region)
                    clone.function = fn
                    if clone.condition_id.startswith(prefix):
                        clone.condition_id = "COND:" + fn + "#" + clone.condition_id[len(prefix):]
                    if clone.parent_id and clone.parent_id.startswith(prefix):
                        clone.parent_id = "COND:" + fn + "#" + clone.parent_id[len(prefix):]
                expanded.append(clone)
        analysis = Analysis(expanded, analysis.diagnostics)
    fatal = [d for d in analysis.diagnostics if d.get("severity") == "error"]
    if fatal:
        details = "; ".join(f"{d.get('function')}:{d.get('line')}: {d.get('message')}" for d in fatal)
        raise ConditionModelError(details)

    clean_source = _sanitize_c(source_text)
    clean_lines = clean_source.splitlines()
    source_line_starts = _line_starts(clean_source)
    outer_by_fn: Dict[str, List[ConditionalRegion]] = {}
    for region in analysis.regions:
        if region.parent_id is None:
            outer_by_fn.setdefault(region.function, []).append(region)

    structural_segments: List[Dict] = []
    structural_edges: List[Dict[str, object]] = []
    structural_by_fn: Dict[str, List[Dict]] = {}
    struct_counter: Dict[str, int] = {}

    def add_struct(fn: str, kind: str, start: int, end: int, **extra: object) -> str:
        struct_counter[fn] = struct_counter.get(fn, 0) + 1
        prefix = {"condition_entry": "CE", "condition_exit": "CX"}.get(kind, "BR")
        seg_id = f"{prefix}:{fn}#{struct_counter[fn]:03d}@{start}-{end}"
        item: Dict[str, object] = {
            "seg_id": seg_id,
            "function": fn,
            "kind": kind,
            "start_line": start,
            "end_line": end,
        }
        item.update(extra)
        structural_segments.append(item)
        structural_by_fn.setdefault(fn, []).append(item)
        return seg_id

    def normal_struct_nodes(fn: str, start: int, end: int, branch_id: Optional[str]) -> List[str]:
        nodes: List[str] = []
        for base in by_fn.get(fn, []):
            a = max(start, int(base["start_line"]))
            b = min(end, int(base["end_line"]))
            if a > b:
                continue
            trimmed = _trim_range(clean_lines, a, b)
            if not trimmed:
                continue
            extra: Dict[str, object] = {"source_segments": [str(base["seg_id"])]}
            if branch_id:
                extra["branch_id"] = branch_id
            nodes.append(add_struct(fn, str(base.get("kind", "compute")), trimmed[0], trimmed[1], **extra))
        return nodes

    def build_sequence(fn: str, start: int, end: int, parent_id: Optional[str], branch_context: Optional[str]) -> List[str]:
        children = [
            r
            for r in analysis.regions
            if r.function == fn and r.parent_id == parent_id and start <= r.entry_line and r.exit_line <= end
        ]
        children.sort(key=lambda r: r.entry_line)
        first: Optional[str] = None
        tail: Optional[str] = None

        def append_linear(nodes: List[str]) -> None:
            nonlocal first, tail
            for left, right in zip(nodes, nodes[1:]):
                structural_edges.append({"src": left, "dst": right, "kind": "intra"})
            if tail and nodes:
                structural_edges.append({"src": tail, "dst": nodes[0], "kind": "intra"})
            if first is None and nodes:
                first = nodes[0]
            if nodes:
                tail = nodes[-1]

        cursor = start
        for region in children:
            before = normal_struct_nodes(fn, cursor, region.entry_line - 1, branch_context)
            append_linear(before)

            entry = add_struct(
                fn,
                "condition_entry",
                region.entry_line,
                region.entry_line,
                condition_id=region.condition_id,
                synthetic=True,
            )
            exit_node = add_struct(
                fn,
                "condition_exit",
                region.exit_line,
                region.exit_line,
                condition_id=region.condition_id,
                synthetic=True,
            )
            if tail:
                structural_edges.append({"src": tail, "dst": entry, "kind": "intra"})
            if first is None:
                first = entry
            for branch in region.branches:
                branch_nodes: List[str] = []
                if branch.start_line is not None and branch.end_line is not None:
                    branch_nodes = build_sequence(
                        fn,
                        branch.start_line,
                        branch.end_line,
                        region.condition_id,
                        f"{region.condition_id}:{branch.branch_id}",
                    )
                branch_meta: Dict[str, object] = {
                    "exclusive_group": region.condition_id,
                    "branch_id": branch.branch_id,
                    "guard": branch.guard,
                }
                if branch_nodes:
                    structural_edges.append({"src": entry, "dst": branch_nodes[0], "kind": "condition_branch", **branch_meta})
                    structural_edges.append({"src": branch_nodes[-1], "dst": exit_node, "kind": "condition_merge", **branch_meta})
                else:
                    structural_edges.append({"src": entry, "dst": exit_node, "kind": "condition_branch", **branch_meta})
            tail = exit_node
            cursor = region.exit_line + 1

        after = normal_struct_nodes(fn, cursor, end, branch_context)
        append_linear(after)
        if first is None or tail is None:
            return []
        return [first] if first == tail else [first, tail]

    for fn, (start, end) in sorted(ranges.items()):
        build_sequence(fn, start, end, None, None)

    base_to_struct: Dict[str, List[str]] = {}
    for base in base_segments:
        base_id = str(base.get("seg_id"))
        fn = str(base.get("function"))
        a = int(base.get("start_line", 0))
        b = int(base.get("end_line", 0))
        # Source order, including control boundaries: a base segment that is
        # entirely conditional must receive dependencies at entry and release
        # them at exit, rather than choosing the first/last branch by accident.
        candidates = sorted(
            (item for item in structural_by_fn.get(fn, [])
             if _overlap(a, b, int(item["start_line"]), int(item["end_line"]))),
            key=lambda item: (
                int(item["start_line"]),
                {"condition_entry": 0, "condition_exit": 2}.get(str(item["kind"]), 1),
                int(item["end_line"]),
            ),
        )
        base_to_struct[base_id] = [str(item["seg_id"]) for item in candidates]

    for edge in base_dag_json.get("edges", []):
        if not isinstance(edge, dict) or edge.get("kind", "intra") == "intra":
            continue
        srcs = base_to_struct.get(str(edge.get("src")), [])
        dsts = base_to_struct.get(str(edge.get("dst")), [])
        if srcs and dsts:
            structural_edges.append({"src": srcs[-1], "dst": dsts[0], "kind": str(edge.get("kind", "dep"))})
    structural_edges = _dedupe_edges(structural_edges)

    # Scheduling operates on executable source ranges only.  Entry/exit nodes
    # remain in the structural view, while their paths are contracted into
    # conditional edges between executable nodes.  Both branches therefore
    # receive their own priorities, but the original C condition still decides
    # which branch actually runs.
    schedule_segments: List[Dict] = []
    base_to_schedule: Dict[str, List[str]] = {}
    for item in structural_segments:
        if item.get("synthetic"):
            continue
        scheduled = dict(item)
        scheduled["source_structural_node"] = str(item["seg_id"])
        memberships: List[Dict[str, str]] = []
        item_start = int(item["start_line"])
        item_end = int(item["end_line"])
        for region in analysis.regions:
            if region.function != item.get("function"):
                continue
            for branch in region.branches:
                if branch.start_line is None or branch.end_line is None:
                    continue
                if branch.start_line <= item_start and item_end <= branch.end_line:
                    memberships.append(
                        {
                            "exclusive_group": region.condition_id,
                            "branch_id": branch.branch_id,
                            "guard": branch.guard,
                        }
                    )
        if memberships:
            scheduled["condition_memberships"] = memberships
        schedule_segments.append(scheduled)
        for source_id in scheduled.get("source_segments", []):
            base_to_schedule.setdefault(str(source_id), []).append(str(scheduled["seg_id"]))

    executable_ids = {str(item["seg_id"]) for item in schedule_segments}
    outgoing: Dict[str, List[Dict[str, object]]] = {}
    for edge in structural_edges:
        outgoing.setdefault(str(edge["src"]), []).append(edge)

    def extend_transition(
        transitions: Tuple[Tuple[str, str, str, str], ...], edge: Dict[str, object]
    ) -> Tuple[Tuple[str, str, str, str], ...]:
        kind = str(edge.get("kind", "intra"))
        if kind not in {"condition_branch", "condition_merge"}:
            return transitions
        return transitions + (
            (
                kind,
                str(edge.get("exclusive_group", "")),
                str(edge.get("branch_id", "")),
                str(edge.get("guard", "")),
            ),
        )

    schedule_edges: List[Dict[str, object]] = []
    for source_id in sorted(executable_ids):
        stack: List[Tuple[str, Tuple[Tuple[str, str, str, str], ...], Optional[str]]] = [
            (source_id, tuple(), None)
        ]
        seen_paths = set()
        while stack:
            current, transitions, dependency_kind = stack.pop()
            state = (current, transitions, dependency_kind)
            if state in seen_paths:
                continue
            seen_paths.add(state)
            for edge in outgoing.get(current, []):
                next_id = str(edge["dst"])
                next_transitions = extend_transition(transitions, edge)
                edge_kind = str(edge.get("kind", "intra"))
                next_dependency_kind = dependency_kind
                if edge_kind not in {"intra", "condition_branch", "condition_merge"}:
                    next_dependency_kind = edge_kind
                if next_id in executable_ids:
                    out_edge: Dict[str, object] = {
                        "src": source_id,
                        "dst": next_id,
                        "kind": next_dependency_kind or edge_kind,
                    }
                    if next_transitions:
                        kind, group, branch_id, guard = next_transitions[-1]
                        out_edge.update(
                            {
                                "kind": next_dependency_kind or kind,
                                "exclusive_group": group,
                                "branch_id": branch_id,
                                "guard": guard,
                            }
                        )
                        if len(next_transitions) > 1 or next_dependency_kind is not None:
                            out_edge["condition_transitions"] = [
                                {
                                    "kind": transition_kind,
                                    "exclusive_group": transition_group,
                                    "branch_id": transition_branch,
                                    "guard": transition_guard,
                                }
                                for transition_kind, transition_group, transition_branch, transition_guard in next_transitions
                            ]
                    schedule_edges.append(out_edge)
                else:
                    stack.append((next_id, next_transitions, next_dependency_kind))
    schedule_edges = _dedupe_edges(schedule_edges)

    def segment_for_line(segments: Sequence[Dict], fn: str, line: int, *, prefer_executable: bool = True) -> Optional[str]:
        candidates = [s for s in segments if s.get("function") == fn and int(s["start_line"]) <= line <= int(s["end_line"])]
        if prefer_executable:
            executable = [s for s in candidates if not s.get("synthetic")]
            if executable:
                candidates = executable
        if not candidates:
            return None
        candidates.sort(key=lambda s: (int(s["end_line"]) - int(s["start_line"]), str(s["seg_id"])))
        return str(candidates[0]["seg_id"])

    node_locations = base_segments_json.get("node_locations", {})
    original_to_schedule: Dict[str, str] = {}
    original_to_struct: Dict[str, str] = {}
    if isinstance(node_locations, dict):
        for node, location in node_locations.items():
            if not isinstance(node, str) or not isinstance(location, dict):
                continue
            fn = location.get("function")
            line = location.get("line")
            if not isinstance(fn, str) or not isinstance(line, int):
                continue
            sched = segment_for_line(schedule_segments, fn, line)
            struct = segment_for_line(structural_segments, fn, line)
            if sched:
                original_to_schedule[node] = sched
            if struct:
                original_to_struct[node] = struct

    structural_to_schedule: Dict[str, str] = {
        segment_id: segment_id for segment_id in executable_ids
    }

    early_exit_re = re.compile(r"\b(return|break|continue|goto)\b")
    early_exit_conditions = [
        r.condition_id
        for r in analysis.regions
        if early_exit_re.search("\n".join(clean_lines[r.entry_line - 1 : r.exit_line]))
    ]
    overlapping_branch_conditions: List[str] = []
    for region in analysis.regions:
        concrete = [
            branch
            for branch in region.branches
            if branch.start_line is not None and branch.end_line is not None
        ]
        if any(
            _overlap(
                int(left.start_line),
                int(left.end_line),
                int(right.start_line),
                int(right.end_line),
            )
            for index, left in enumerate(concrete)
            for right in concrete[index + 1 :]
        ):
            overlapping_branch_conditions.append(region.condition_id)
    def is_c3_control_prefix(region: ConditionalRegion, branch: Branch, prefix: str) -> bool:
        # A bare else-if is an else arm containing another conditional, not an
        # executable range starting on the `} else if` control header itself.
        # Exempt only C3: one top-level if, one else-if, three braced bodies.
        if region.parent_id is not None or branch.branch_id != "else":
            return False
        if re.fullmatch(r"\s*}\s*else\s*", prefix) is None:
            return False
        same_function = [r for r in analysis.regions if r.function == region.function]
        if len(same_function) != 2:
            return False
        child = next((r for r in same_function if r.parent_id == region.condition_id
                      and r.parent_branch_id == "else"
                      and r.start_offset == branch.start_offset
                      and r.end_offset == branch.end_offset), None)
        if child is None:
            return False
        if any(b.start_line is None for b in [region.branches[0], *child.branches]):
            return False
        _, outer_then, _, _ = _parse_if(clean_source, region.start_offset)
        _, inner_then, final_else, _ = _parse_if(clean_source, child.start_offset)
        return all(stmt is not None and clean_source[stmt.start] == "{"
                   for stmt in (outer_then, inner_then, final_else))

    shared_control_boundary_conditions: List[str] = []
    for region in analysis.regions:
        for branch in region.branches:
            if branch.start_line is None or branch.end_line is None:
                continue
            if branch.start_offset is None or branch.end_offset is None:
                continue
            first_line_start = source_line_starts[branch.start_line - 1]
            last_line_end = (
                source_line_starts[branch.end_line] - 1
                if branch.end_line < len(source_line_starts) else len(clean_source)
            )
            # Whole-line probes must not cross the branch's opening/closing
            # control boundary, including a multiline condition ending here.
            prefix = clean_source[first_line_start:branch.start_offset] if branch.start_offset > first_line_start else ""
            suffix = clean_source[branch.end_offset:last_line_end] if branch.end_offset < last_line_end else ""
            if (prefix.strip() and not is_c3_control_prefix(region, branch, prefix)) or suffix.strip():
                shared_control_boundary_conditions.append(region.condition_id)
                break
    scheduler_compatible = not (
        early_exit_conditions or overlapping_branch_conditions or shared_control_boundary_conditions
    )
    reasons: List[str] = []
    if early_exit_conditions:
        reasons.append("conditional branch contains return/break/continue/goto")
    if overlapping_branch_conditions:
        reasons.append("conditional branches share source lines and cannot be instrumented independently")
    if shared_control_boundary_conditions:
        reasons.append("conditional branch code shares a control boundary line and cannot be instrumented independently")

    regions_payload = [
        {
            "condition_id": r.condition_id,
            "function": r.function,
            "entry_line": r.entry_line,
            "exit_line": r.exit_line,
            "parent_id": r.parent_id,
            "parent_branch_id": r.parent_branch_id,
            "branches": [
                {
                    "branch_id": b.branch_id,
                    "guard": b.guard,
                    "start_line": b.start_line,
                    "end_line": b.end_line,
                }
                for b in r.branches
            ],
        }
        for r in analysis.regions
    ]
    mappings = {
        "original_node_to_execution_segment": original_to_schedule,
        "original_node_to_structural_node": original_to_struct,
        "original_segment_to_execution_segments": base_to_schedule,
        "structural_node_to_execution_segment": structural_to_schedule,
    }
    capabilities = {
        "scheduler_compatible": scheduler_compatible,
        "scheduler_compatibility_mode": "mutually_exclusive_branch_nodes",
        "blocked_reasons": reasons,
    }

    common = {
        "schema_version": SCHEMA_VERSION,
        "base_name": base_name,
        "condition_model_version": "phase1",
        "condition_regions": regions_payload,
        "mappings": mappings,
        "capabilities": capabilities,
    }
    struct_segments_json = {**common, "view": "struct", "segments": structural_segments}
    struct_dag_json = {
        "schema_version": SCHEMA_VERSION,
        "base_name": base_name,
        "view": "struct",
        "nodes": [str(s["seg_id"]) for s in structural_segments],
        "edges": structural_edges,
        "exclusive_groups": {
            r.condition_id: [f"{r.condition_id}:{branch.branch_id}" for branch in r.branches] for r in analysis.regions
        },
    }
    sched_segments_json = {**common, "view": "sched", "segments": schedule_segments}
    sched_dag_json = {
        "schema_version": SCHEMA_VERSION,
        "base_name": base_name,
        "view": "sched",
        "nodes": [str(s["seg_id"]) for s in schedule_segments],
        "edges": schedule_edges,
        "exclusive_groups": {
            r.condition_id: [f"{r.condition_id}:{branch.branch_id}" for branch in r.branches] for r in analysis.regions
        },
    }
    meta = {
        "condition_count": len(analysis.regions),
        "outer_condition_count": sum(len(v) for v in outer_by_fn.values()),
        "scheduler_compatible": scheduler_compatible,
        "blocked_reasons": reasons,
        "separate_views": True,
    }
    return struct_segments_json, struct_dag_json, sched_segments_json, sched_dag_json, meta


def render_condition_dot(segments_json: Dict, dag_json: Dict) -> str:
    segments = {str(s["seg_id"]): s for s in segments_json.get("segments", []) if isinstance(s, dict)}
    lines = ["digraph ConditionalDAG {", "  rankdir=LR;", '  node [fontname="Consolas", fontsize=10];']
    for seg_id, segment in sorted(segments.items()):
        kind = str(segment.get("kind", ""))
        if kind == "condition_entry":
            shape, fill = "diamond", "#FFE0B2"
        elif kind == "condition_exit":
            shape, fill = "circle", "#FFE0B2"
        else:
            shape, fill = "box", "#E3F2FD"
        label = f"{seg_id}\\n{kind} @{segment.get('start_line')}-{segment.get('end_line')}"
        lines.append(f'  "{seg_id}" [shape={shape}, style="rounded,filled", fillcolor="{fill}", label="{label}"];')
    for edge in dag_json.get("edges", []):
        if not isinstance(edge, dict):
            continue
        src, dst = edge.get("src"), edge.get("dst")
        if src not in segments or dst not in segments:
            continue
        kind = str(edge.get("kind", "intra"))
        attrs: List[str] = []
        if kind == "condition_branch":
            attrs.extend(['color="#EF6C00"', "penwidth=2.0"])
            attrs.append(f'label="{edge.get("branch_id")}:{edge.get("guard")}"')
        elif kind == "condition_merge":
            attrs.extend(['color="#EF6C00"', 'style="dashed"'])
        elif kind != "intra":
            attrs.extend(['color="#C62828"', "penwidth=2.0"])
        suffix = f" [{', '.join(attrs)}]" if attrs else ""
        lines.append(f'  "{src}" -> "{dst}"{suffix};')
    lines.append("}")
    return "\n".join(lines) + "\n"
