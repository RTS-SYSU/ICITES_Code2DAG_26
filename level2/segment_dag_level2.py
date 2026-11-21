from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ..level1.segment_dag import _iter_stage1_cut_points  # type: ignore
from ..level1.segment_dag import _load_functions_full, _load_functions_ranges, _load_internal_meta  # type: ignore
from ..core.func_ranges import normalize_func_name, _strip_comments_and_strings


@dataclass(frozen=True)
class Segment:
    seg_id: str
    function: str
    kind: str  # compute|mutex_cs
    start_line: int
    end_line: int


_CREATE_NODE_RE = re.compile(r"(^|/)pthread_create(\d+)?$")
_JOIN_NODE_RE = re.compile(r"(^|/)pthread_join(\d+)?$")
_SEM_POST_NODE_RE = re.compile(r"(^|/)sem_post(\d+)?$")
_SEM_WAIT_NODE_RE = re.compile(r"(^|/)sem_wait(\d+)?$")
_MU_LOCK_NODE_RE = re.compile(r"(^|/)pthread_mutex_lock(\d+)?$")
_MU_UNLOCK_NODE_RE = re.compile(r"(^|/)pthread_mutex_unlock(\d+)?$")
_DOT_EDGE_RE = re.compile(r'^\s*"([^"]+)"\s*->\s*"([^"]+)"\s*;')

def _segment_id(fn: str, idx: int, kind: str, start: int, end: int) -> str:
    if kind == "mutex_cs":
        return f"MU:{fn}#{idx:03d}@{start}-{end}"
    return f"SEG:{fn}#{idx:03d}@{start}-{end}"


def _build_node_owner_and_order(
    internal_meta: Dict[str, Dict[str, Dict]],
) -> Tuple[Dict[str, Tuple[str, int]], Dict[str, List[str]]]:
    node_owner_line: Dict[str, Tuple[str, int]] = {}
    ordered_nodes_by_fn: Dict[str, List[str]] = {}
    for fn, meta_map in internal_meta.items():
        if not isinstance(meta_map, dict) or not meta_map:
            continue
        ordered_nodes_by_fn[fn] = list(meta_map.keys())
        for node, meta in meta_map.items():
            if not isinstance(node, str) or not isinstance(meta, dict):
                continue
            line = meta.get("line")
            if not isinstance(line, int):
                continue
            # Assumption (confirmed by user): internal_meta keys preserve call order and are globally unique.
            node_owner_line[node] = (fn, line)
    return node_owner_line, ordered_nodes_by_fn


def _pair_mutex_intervals(
    *,
    internal_meta: Dict[str, Dict[str, Dict]],
) -> Dict[str, List[Tuple[int, int]]]:
    """Pair lock/unlock per function using internal-meta order (stack-based)."""
    intervals: Dict[str, List[Tuple[int, int]]] = {}
    for fn, meta_map in internal_meta.items():
        if not isinstance(meta_map, dict) or not meta_map:
            continue
        stack: List[int] = []
        for node, meta in meta_map.items():
            if not isinstance(node, str) or not isinstance(meta, dict):
                continue
            line = meta.get("line")
            if not isinstance(line, int):
                continue
            if _MU_LOCK_NODE_RE.search(node):
                stack.append(line)
            elif _MU_UNLOCK_NODE_RE.search(node):
                if not stack:
                    continue
                lock_line = stack.pop()
                if lock_line <= line:
                    intervals.setdefault(fn, []).append((lock_line, line))
    for fn in list(intervals.keys()):
        intervals[fn].sort()
    return intervals


def _parse_circle_sem_calls(circle_path: Path) -> List[Dict[str, object]]:
    """Return every sem_post/sem_wait record, preserving multiplicity.

    The legacy pair parser deliberately kept only one post and one wait per
    semaphore variable.  This collector keeps all source occurrences so the
    build can distinguish a determinate relation from an ambiguous one.
    """
    calls: List[Dict[str, object]] = []
    block = None
    for line in circle_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s:
            continue
        if s == "mutex":
            block = "mutex"
            continue
        if s == "semaphore":
            block = "sem"
            continue
        if block != "sem":
            continue
        parts = s.split()
        # Example: c1_fn/sem_post9 sem sem51 142 zhang1.c
        if len(parts) < 3:
            continue
        node, _tag, idx = parts[0], parts[1], parts[2]
        line_no: Optional[int] = None
        if len(parts) >= 4:
            try:
                line_no = int(parts[3])
            except ValueError:
                line_no = None
        if "sem_post" in node:
            kind = "post"
        elif "sem_wait" in node:
            kind = "wait"
        else:
            continue
        calls.append({"node": node, "kind": kind, "idx": idx, "line": line_no})
    return calls


def _sem_pairs_from_calls(calls: Sequence[Dict[str, object]]) -> List[Tuple[str, str]]:
    """Return one deterministic (last post, last wait) pair per semaphore.

    This is only a representative witness for rules that project the
    relation per branch/iteration.  Determinacy is checked separately before
    any graph is published.
    """
    groups: Dict[str, Dict[str, str]] = {}
    for call in calls:
        idx = str(call["idx"])
        kind = str(call["kind"])
        groups.setdefault(idx, {})[kind] = str(call["node"])
    return [(g["post"], g["wait"]) for g in groups.values() if g.get("post") and g.get("wait")]


def _parse_circle_sem_pairs(circle_path: Path) -> List[Tuple[str, str]]:
    """Return list of (post_node, wait_node) parsed from circle.txt."""
    return _sem_pairs_from_calls(_parse_circle_sem_calls(circle_path))


def _sem_paths_compatible(left: Sequence[Tuple[str, str]], right: Sequence[Tuple[str, str]]) -> bool:
    """True when two source positions can occur on one feasible branch path."""
    left_map = dict(left)
    right_map = dict(right)
    return all(left_map[key] == right_map[key] for key in set(left_map) & set(right_map))


def _sem_call_branch_path(
    *,
    function: str,
    line: int,
    regions: Sequence[object],
) -> Tuple[Tuple[str, str], ...]:
    path: List[Tuple[str, str]] = []
    for region in regions:
        if getattr(region, "function", None) != function:
            continue
        for branch in getattr(region, "branches", ()):  # type: ignore[attr-defined]
            start = getattr(branch, "start_line", None)
            end = getattr(branch, "end_line", None)
            if not isinstance(start, int) or not isinstance(end, int):
                continue
            if start <= line <= end:
                path.append((str(getattr(region, "condition_id")), str(getattr(branch, "branch_id"))))
    return tuple(path)


def _validate_semaphore_determinacy(
    *,
    sem_calls: Sequence[Dict[str, object]],
    source_text: str,
    fn_range: Dict[str, Tuple[int, int]],
    node_owner_line: Dict[str, Tuple[str, int]],
) -> None:
    """Reject semaphore relations with two same-kind calls on one path.

    A semaphore variable is determinate when every feasible execution path
    contains at most one post and at most one wait for that variable.  A
    common post with waits in mutually exclusive branches is determinate; two
    posts (or two waits) that can execute on the same path are not.  Calls in
    different functions have no common branch context and are treated as
    potentially concurrent, so two posts (or two waits) in different functions
    are also rejected unless the source structure proves otherwise.
    """
    groups: Dict[str, List[Dict[str, object]]] = {}
    for call in sem_calls:
        groups.setdefault(str(call["idx"]), []).append(call)
    ambiguous_groups = {
        idx: calls for idx, calls in groups.items()
        if len([c for c in calls if c["kind"] == "post"]) > 1
        or len([c for c in calls if c["kind"] == "wait"]) > 1
    }
    if not ambiguous_groups:
        return
    regions: Sequence[object] = ()
    analysis_error: Optional[Exception] = None
    try:
        from ..pipeline.condition_model import analyze_conditionals
        analysis = analyze_conditionals(source_text, fn_range)
        fatal = [d for d in analysis.diagnostics if d.get("severity") == "error"]
        if fatal:
            raise ValueError("condition analysis failed")
        regions = analysis.regions
    except Exception as exc:  # noqa: BLE001 - conservatively reject on analysis failure
        analysis_error = exc

    for idx, calls in ambiguous_groups.items():
        paths: Dict[str, Tuple[Tuple[str, str], ...]] = {}
        for call in calls:
            node = str(call["node"])
            owner = node_owner_line.get(node)
            if owner is not None:
                function, line = owner
            else:
                function = node.split("/", 1)[0]
                line_value = call.get("line")
                if not isinstance(line_value, int):
                    raise ValueError(
                        f"ambiguous semaphore '{idx}': cannot locate source line for {node}; "
                        "refusing to publish a sem_dep relation"
                    )
                line = line_value
            if analysis_error is not None or function not in fn_range:
                raise ValueError(
                    f"ambiguous semaphore '{idx}': cannot determine branch context for {node}; "
                    "refusing to publish a sem_dep relation"
                )
            paths[node] = _sem_call_branch_path(function=function, line=line, regions=regions)
        for kind in ("post", "wait"):
            same_kind = [str(c["node"]) for c in calls if c["kind"] == kind]
            for i, left in enumerate(same_kind):
                for right in same_kind[i + 1:]:
                    if _sem_paths_compatible(paths[left], paths[right]):
                        detail = ", ".join(f"{n}@{next((c.get('line') for c in calls if c['node'] == n), '?')}" for n in (left, right))
                        raise ValueError(
                            f"ambiguous semaphore '{idx}': {kind} calls {detail} can execute on the same path; "
                            "refusing to publish a sem_dep relation"
                        )


def _validate_semaphore_branch_pairing(
    *,
    sem_calls: Sequence[Dict[str, object]],
    source_text: str,
    fn_range: Dict[str, Tuple[int, int]],
    node_owner_line: Dict[str, Tuple[str, int]],
) -> None:
    """Reject post/wait pairs that cannot occur on a compatible wait path.

    A wait may only be paired with a post that is guaranteed earlier on every
    candidate path containing the wait.  For calls in one execution function,
    this means the post's condition membership must be a subset of the wait's
    membership and the source order must be post-before-wait.  For calls in
    different execution functions, the post must be unconditional; different
    threads' branch labels cannot be compared.

    Calls outside all modeled conditions have empty paths.  When both calls
    are unconditional in one function, the existing same-segment/final-cycle
    validation remains responsible for source order; this validator does not
    change that legacy rejection path.

    The phase-1 condition model represents structured if/else regions only.
    A switch can select mutually exclusive cases and a goto can jump over a
    post or into a wait without appearing in any region, so any function that
    owns a call of a paired semaphore and contains ``switch`` or ``goto`` is
    rejected conservatively.
    """
    groups: Dict[str, List[Dict[str, object]]] = {}
    for call in sem_calls:
        groups.setdefault(str(call["idx"]), []).append(call)
    paired = {
        idx: calls
        for idx, calls in groups.items()
        if any(str(c.get("kind")) == "post" for c in calls)
        and any(str(c.get("kind")) == "wait" for c in calls)
    }
    if not paired:
        return

    try:
        from ..pipeline.condition_model import analyze_conditionals

        analysis = analyze_conditionals(source_text, fn_range)
        fatal = [d for d in analysis.diagnostics if d.get("severity") == "error"]
        if fatal:
            raise ValueError("condition analysis failed")
        regions = analysis.regions
    except Exception as exc:  # noqa: BLE001 - conservatively reject on missing proof
        raise ValueError(
            "cannot determine condition paths for a paired semaphore; "
            "refusing to publish a sem_dep relation"
        ) from exc

    for idx, calls in paired.items():
        paths: Dict[str, Tuple[Tuple[str, str], ...]] = {}
        owners: Dict[str, Tuple[str, int]] = {}
        for call in calls:
            node = str(call["node"])
            owner = node_owner_line.get(node)
            if owner is None:
                value = call.get("line")
                if not isinstance(value, int):
                    raise ValueError(
                        f"semaphore '{idx}': cannot locate source line for {node}; "
                        "refusing to publish a sem_dep relation"
                    )
                function = node.split("/", 1)[0]
                line_no = value
            else:
                function, line_no = owner
            if function not in fn_range:
                raise ValueError(
                    f"semaphore '{idx}': cannot determine function context for {node}; "
                    "refusing to publish a sem_dep relation"
                )
            owners[node] = (function, line_no)
            paths[node] = _sem_call_branch_path(
                function=function, line=line_no, regions=regions
            )

        posts = [str(c["node"]) for c in calls if str(c.get("kind")) == "post"]
        waits = [str(c["node"]) for c in calls if str(c.get("kind")) == "wait"]

        # The phase-1 condition model covers structured if/else regions only.
        # A switch may select a post/wait case that is mutually exclusive with
        # the paired call, and a goto may jump over a post (or into a wait).
        # Neither construct is represented in analysis.regions, so a path
        # proof that ignores them would accept a pairing it cannot justify.
        # Reject conservatively for every function that owns a call of this
        # semaphore.
        clean_lines = _strip_comments_and_strings(source_text).splitlines()
        for function in sorted({owners[node][0] for node in posts + waits}):
            start, end = fn_range[function]
            body = "\n".join(clean_lines[max(start - 1, 0):end])
            for token, pattern in (
                ("switch", r"\bswitch\s*\("),
                ("goto", r"\bgoto\b"),
            ):
                if re.search(pattern, body):
                    raise ValueError(
                        f"semaphore '{idx}': function {function} contains unmodeled "
                        f"'{token}' control flow; cannot prove post/wait branch "
                        "reachability; refusing to publish a sem_dep relation"
                    )

        for wait in waits:
            wait_fn, wait_line = owners[wait]
            wait_path = paths[wait]
            dominating = []
            for post in posts:
                post_fn, post_line = owners[post]
                post_path = paths[post]
                if post_fn != wait_fn:
                    # Different threads' condition labels are not comparable.
                    # Only an unconditional post guarantees a token on every
                    # possible wait path.
                    if not post_path:
                        dominating.append(post)
                    continue
                if not set(post_path) <= set(wait_path):
                    continue
                if post_path == () and wait_path == ():
                    # Preserve the existing same-segment/final-cycle handling
                    # for unconditional same-function calls.
                    dominating.append(post)
                    continue
                if post_line < wait_line:
                    dominating.append(post)
                    continue
                if post_line > wait_line:
                    continue
                post_order = _sem_call_source_offset(
                    node=post,
                    kind="post",
                    function=post_fn,
                    line_no=post_line,
                    source_text=source_text,
                    sem_calls=sem_calls,
                    node_owner_line=node_owner_line,
                )
                wait_order = _sem_call_source_offset(
                    node=wait,
                    kind="wait",
                    function=wait_fn,
                    line_no=wait_line,
                    source_text=source_text,
                    sem_calls=sem_calls,
                    node_owner_line=node_owner_line,
                )
                if post_order is not None and wait_order is not None and post_order < wait_order:
                    dominating.append(post)
            if not dominating:
                raise ValueError(
                    f"semaphore '{idx}': wait {wait} has no post on a compatible "
                    "candidate branch path with post-before-wait order; "
                    "refusing to publish a sem_dep relation"
                )


def _validate_semaphore_dynamic_instances(
    *,
    sem_calls: Sequence[Dict[str, object]],
    internal_meta: Dict[str, Dict[str, Dict]],
    node_owner_line: Dict[str, Tuple[str, int]],
) -> None:
    """Reject paired semaphore calls owned by multiple execution instances.

    The per-function internal metadata is the earliest place where the source
    call identity and the concrete execution instance (``worker`` versus
    ``worker@instance1``) are both known.  The later deduplicated
    ``node_owner_line`` map deliberately keeps only the last clone, so it
    cannot be used to prove a unique dynamic pairing.
    """
    source_owners: Dict[Tuple[str, int, str], Set[str]] = {}
    node_keys: Dict[str, Set[Tuple[str, int, str]]] = {}
    for fn, meta_map in internal_meta.items():
        if not isinstance(meta_map, dict):
            continue
        base_fn = normalize_func_name(fn)
        for node, meta in meta_map.items():
            if not isinstance(node, str) or not isinstance(meta, dict):
                continue
            line = meta.get("line")
            if not isinstance(line, int):
                continue
            if _SEM_POST_NODE_RE.search(node):
                kind = "post"
            elif _SEM_WAIT_NODE_RE.search(node):
                kind = "wait"
            else:
                continue
            key = (base_fn, line, kind)
            source_owners.setdefault(key, set()).add(fn)
            node_keys.setdefault(node, set()).add(key)

    groups: Dict[str, List[Dict[str, object]]] = {}
    for call in sem_calls:
        groups.setdefault(str(call["idx"]), []).append(call)
    for idx, calls in groups.items():
        kinds = {str(call["kind"]) for call in calls}
        if not {"post", "wait"} <= kinds:
            continue
        for call in calls:
            node = str(call["node"])
            keys = set(node_keys.get(node, set()))
            if not keys:
                owner = node_owner_line.get(node)
                if owner is not None:
                    keys.add((normalize_func_name(owner[0]), owner[1], str(call["kind"])))
            for key in keys:
                owners = source_owners.get(key, set())
                if len(owners) > 1:
                    base_fn, line, kind = key
                    raise ValueError(
                        f"semaphore '{idx}': {kind} source call {node} at "
                        f"{base_fn}:{line} executes in multiple dynamic thread "
                        f"instances {sorted(owners)}; cannot prove a unique dynamic "
                        "pairing; refusing to publish a sem_dep relation"
                    )


_SEM_POST_SOURCE_RE = re.compile(r"\bsem_post\s*\(\s*&\s*([A-Za-z_]\w*)\s*\)")
_SEM_WAIT_SOURCE_RE = re.compile(r"\bsem_wait\s*\(\s*&\s*([A-Za-z_]\w*)\s*\)")
_SEM_INIT_SOURCE_RE = re.compile(
    r"\bsem_init\s*\(\s*&\s*([A-Za-z_]\w*)\s*,\s*[^,]+,\s*([^)]+)\)"
)


def _sem_call_source_variables(
    *,
    calls: Sequence[Dict[str, object]],
    all_calls: Sequence[Dict[str, object]],
    source_lines: Sequence[str],
    node_owner_line: Dict[str, Tuple[str, int]],
) -> Set[str]:
    """Resolve source variable names for one circle.txt semaphore group.

    Circle.txt omits the source column.  Several calls may share one source
    line (the DAG fixtures intentionally do this), so select the same
    occurrence in the source line as the call's order within that line and
    kind.
    """
    variables: Set[str] = set()
    for call in calls:
        node = str(call["node"])
        owner = node_owner_line.get(node)
        if owner is not None:
            function, line_no = owner
        else:
            function = node.split("/", 1)[0]
            value = call.get("line")
            if not isinstance(value, int):
                return set()
            line_no = value
        if not (1 <= line_no <= len(source_lines)):
            return set()
        kind = str(call["kind"])
        pattern = _SEM_POST_SOURCE_RE if kind == "post" else _SEM_WAIT_SOURCE_RE
        matches = pattern.findall(source_lines[line_no - 1])
        if not matches:
            # The call argument may be split over several source lines, e.g.
            # sem_wait(\n    &sem_x\n);  Keep the window narrow and reject if
            # it is still ambiguous.
            start = max(0, line_no - 2)
            end = min(len(source_lines), line_no + 3)
            matches = pattern.findall("\n".join(source_lines[start:end]))
            if len(matches) != 1:
                return set()
            variables.add(matches[0])
            continue
        if len(matches) == 1:
            variables.add(matches[0])
            continue
        same_line = []
        for other in all_calls:
            other_node = str(other["node"])
            other_owner = node_owner_line.get(other_node)
            if other_owner is not None:
                other_function, other_line = other_owner
            else:
                other_value = other.get("line")
                if not isinstance(other_value, int):
                    continue
                other_function, other_line = other_node.split("/", 1)[0], other_value
            if (
                other_function == function
                and other_line == line_no
                and str(other["kind"]) == kind
            ):
                same_line.append(other)
        position = next(
            (index for index, other in enumerate(same_line) if other is call),
            None,
        )
        if position is None or position >= len(matches):
            return set()
        variables.add(matches[position])
    return variables


def _sem_call_source_offset(
    *,
    node: str,
    kind: str,
    function: str,
    line_no: int,
    source_text: str,
    sem_calls: Sequence[Dict[str, object]],
    node_owner_line: Dict[str, Tuple[str, int]],
) -> Optional[Tuple[int, int]]:
    """Return (line, column) for one concrete same-line semaphore call.

    The base metadata records only source lines, not columns.  For a same-line
    post/wait pair the line order is therefore insufficient: resolve the
    particular node to its occurrence on that line using the circle call order.
    Any ambiguity returns None so callers can reject instead of guessing.
    """
    clean = _strip_comments_and_strings(source_text)
    lines = clean.splitlines()
    if not (1 <= line_no <= len(lines)):
        return None
    pattern = _SEM_POST_SOURCE_RE if kind == "post" else _SEM_WAIT_SOURCE_RE
    matches = list(pattern.finditer(lines[line_no - 1]))
    if not matches:
        return None
    same_kind = []
    for call in sem_calls:
        if str(call.get("kind")) != kind:
            continue
        other_node = str(call.get("node"))
        owner = node_owner_line.get(other_node)
        if owner is None:
            other_value = call.get("line")
            if not isinstance(other_value, int):
                continue
            other_function = other_node.split("/", 1)[0]
            other_line = other_value
        else:
            other_function, other_line = owner
        if other_function == function and other_line == line_no:
            same_kind.append(other_node)
    positions = [index for index, value in enumerate(same_kind) if value == node]
    if len(positions) != 1 or positions[0] >= len(matches):
        return None
    return line_no, matches[positions[0]].start()


def _sem_init_initial_counts(source_text: str) -> Dict[str, List[Optional[int]]]:
    """Parse literal sem_init initial counts per source semaphore variable.

    Only decimal integer literals are recognized.  Expressions, macros and
    missing/multiple initializers are intentionally left undecidable.
    """
    values: Dict[str, List[Optional[int]]] = {}
    for match in _SEM_INIT_SOURCE_RE.finditer(source_text):
        var = match.group(1)
        raw = match.group(2).strip()
        if re.fullmatch(r"0+[uUlL]*", raw):
            value: Optional[int] = 0
        elif re.fullmatch(r"[1-9]\d*[uUlL]*", raw):
            value = int(re.match(r"\d+", raw).group(0))  # type: ignore[union-attr]
        else:
            value = None
        values.setdefault(var, []).append(value)
    return values


def _validate_semaphore_initial_counts(
    *,
    sem_calls: Sequence[Dict[str, object]],
    source_text: str,
    node_owner_line: Dict[str, Tuple[str, int]],
) -> None:
    """Reject paired semaphores whose wait may not need the matched post.

    Only a literal zero initial count is accepted.  A non-zero count can let
    sem_wait consume the initial token and proceed without the matched post;
    an unknown initializer has the same risk.  This explicitly does not add
    counting-semaphore semantics.  A same-segment zero-count pair remains an
    internal witness and never becomes a final scheduling self-loop.
    """
    groups: Dict[str, List[Dict[str, object]]] = {}
    for call in sem_calls:
        groups.setdefault(str(call["idx"]), []).append(call)
    source_lines = source_text.splitlines()
    initial_counts = _sem_init_initial_counts(source_text)

    for idx, calls in groups.items():
        posts = [str(c["node"]) for c in calls if c["kind"] == "post"]
        waits = [str(c["node"]) for c in calls if c["kind"] == "wait"]
        if not posts or not waits:
            continue

        variables = _sem_call_source_variables(
            calls=calls,
            all_calls=sem_calls,
            source_lines=source_lines,
            node_owner_line=node_owner_line,
        )
        if len(variables) != 1:
            raise ValueError(
                f"semaphore '{idx}': cannot resolve the source semaphore variable; "
                "refusing to publish a cross-segment sem_dep relation"
            )
        var = next(iter(variables))
        counts = initial_counts.get(var, [])
        if len(counts) != 1:
            raise ValueError(
                f"semaphore '{idx}': cannot determine a unique literal sem_init "
                f"initial count for '{var}'; refusing to publish a cross-segment "
                "sem_dep relation"
            )
        initial = counts[0]
        if initial is None:
            raise ValueError(
                f"semaphore '{idx}': sem_init initial count for '{var}' is not "
                "a literal integer; refusing to publish a cross-segment sem_dep relation"
            )
        if initial != 0:
            raise ValueError(
                f"semaphore '{idx}': sem_init initial count {initial} for '{var}' "
                "may let sem_wait proceed without the matched sem_post; refusing "
                "to publish a cross-segment sem_dep relation"
            )


def _parse_sem_pairs_from_dot(dot_path: Path) -> List[Tuple[str, str]]:
    """Fallback: parse explicit sem_post -> sem_wait edges from dag.dot."""
    if not dot_path.exists():
        return []
    out: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for raw in dot_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = _DOT_EDGE_RE.match(raw)
        if not m:
            continue
        src, dst = m.group(1), m.group(2)
        if _SEM_POST_NODE_RE.search(src) and _SEM_WAIT_NODE_RE.search(dst):
            pair = (src, dst)
            if pair not in seen:
                seen.add(pair)
                out.append(pair)
    return out


def _owner_from_dot_node(node: str) -> str:
    """Best-effort owner function name for a legacy dag.dot node."""
    # Common format: "<fn>/<api><seq>"
    if "/" in node:
        return node.split("/", 1)[0]
    return node


def _fallback_edges_from_dot(*, segs_by_fn: Dict[str, List[Segment]], dot_path: Path) -> List[Tuple[str, str, str]]:
    """Fallback edge projection: collapse dag.dot *callsite->callee* edges onto per-function first segment.

    Important: legacy dag.dot includes continuation edges (callee -> next caller node). Those edges can
    introduce cycles at function-level. This fallback only keeps edges where:
    - src is a callsite node (contains '/')
    - dst is a callee function node (does NOT contain '/')
    """
    if not dot_path.exists():
        return []
    fn_first_seg: Dict[str, str] = {fn: segs[0].seg_id for fn, segs in segs_by_fn.items() if segs}
    edges: List[Tuple[str, str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for raw in dot_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = _DOT_EDGE_RE.match(raw)
        if not m:
            continue
        src_node, dst_node = m.group(1), m.group(2)
        if "/" not in src_node:
            continue
        if "/" in dst_node:
            continue
        src_fn = _owner_from_dot_node(src_node)
        dst_fn = dst_node
        s = fn_first_seg.get(src_fn)
        d = fn_first_seg.get(dst_fn)
        if not s or not d:
            continue
        pair = (s, d)
        if pair in seen:
            continue
        seen.add(pair)
        edges.append((s, d, "call"))
    return edges


def _line_has_effective_code(raw_line: str, *, in_block_comment: bool) -> Tuple[bool, bool]:
    """Return (has_effective_code, next_in_block_comment)."""
    line = raw_line
    i = 0
    n = len(line)
    has_code = False
    while i < n:
        if in_block_comment:
            end = line.find("*/", i)
            if end < 0:
                return has_code, True
            i = end + 2
            in_block_comment = False
            continue

        if i + 1 < n and line[i] == "/" and line[i + 1] == "/":
            break
        if i + 1 < n and line[i] == "/" and line[i + 1] == "*":
            in_block_comment = True
            i += 2
            continue
        if not line[i].isspace():
            has_code = True
        i += 1
    return has_code, in_block_comment


def _build_effective_line_neighbors(
    *,
    source_lines: Sequence[str],
    fn_range: Dict[str, Tuple[int, int]],
) -> Tuple[Dict[str, Dict[int, Optional[int]]], Dict[str, Dict[int, Optional[int]]]]:
    """Build prev/next effective code line mapping for each function range."""
    line_is_effective: Dict[int, bool] = {}
    in_block_comment = False
    for lineno, raw in enumerate(source_lines, start=1):
        has_code, in_block_comment = _line_has_effective_code(raw, in_block_comment=in_block_comment)
        line_is_effective[lineno] = has_code

    prev_map_by_fn: Dict[str, Dict[int, Optional[int]]] = {}
    next_map_by_fn: Dict[str, Dict[int, Optional[int]]] = {}
    for fn, (start, end) in fn_range.items():
        effective_lines = [ln for ln in range(start, end + 1) if line_is_effective.get(ln, False)]
        prev_map: Dict[int, Optional[int]] = {}
        next_map: Dict[int, Optional[int]] = {}
        for ln in range(start, end + 1):
            prev_ln = None
            next_ln = None
            for candidate in reversed(effective_lines):
                if candidate < ln:
                    prev_ln = candidate
                    break
            for candidate in effective_lines:
                if candidate > ln:
                    next_ln = candidate
                    break
            prev_map[ln] = prev_ln
            next_map[ln] = next_ln
        prev_map_by_fn[fn] = prev_map
        next_map_by_fn[fn] = next_map
    return prev_map_by_fn, next_map_by_fn


def _build_segments_for_function(
    fn: str,
    *,
    start_line: int,
    end_line: int,
    cut_after: Set[int],
    mutex_intervals: Sequence[Tuple[int, int]],
) -> List[Segment]:
    if start_line <= 0 or end_line < start_line:
        return []

    lock_lines = sorted({a for (a, _b) in mutex_intervals})
    boundaries = sorted({k for k in cut_after if start_line <= k <= end_line})

    spans: List[Tuple[int, int]] = []
    cur = start_line
    for k in boundaries:
        if cur <= k:
            spans.append((cur, k))
            cur = k + 1
    if cur <= end_line:
        spans.append((cur, end_line))

    out: List[Segment] = []
    idx = 0
    for a, b in spans:
        kind = "compute"
        if any(a <= l <= b for l in lock_lines):
            kind = "mutex_cs"
        idx += 1
        out.append(Segment(_segment_id(fn, idx, kind, a, b), fn, kind, a, b))
    return out


def _find_segment_index_covering_line(segs: Sequence[Segment], line: int) -> Optional[int]:
    for i, s in enumerate(segs):
        if s.start_line <= line <= s.end_line:
            return i
    return None


def build_level2_segments_and_dag(
    *,
    base_dir: Path,
    base_name: str,
    source_file: Path,
) -> Tuple[Dict, Dict]:
    """Build Level-2 segments + segment DAG.

    Inputs:
    - merged Level-2 DAG dot: intermediate_results/<base>/level2/merge_post_wait/dag_level2_sem.dot (preferred)
    - internal meta:          intermediate_results/<base>/dag_generation/debug/mycalls_meta_internal.json
    - functions ranges:       intermediate_results/<base>/dag_generation/functions_ranges.json
    - functions full:         intermediate_results/<base>/dag_generation/functions_full.json

    Segmentation rules (Level-2, per user doc):
    - create/sem_post: cut AFTER line (belongs to previous segment)
    - join/sem_wait:   cut BEFORE line (belongs to next segment)
    - mutex: cut BEFORE lock, cut AFTER unlock; cuts inside critical section are ignored
    - merge rules:
        * create: if previous block is mutex_cs, remove mutex_cs's unlock cut to merge
        * join:   if next block is mutex_cs, remove mutex_cs's lock cut to merge
    - thread boundary rules (only for main + thread entry functions):
        * start: if first call is not lock/wait, draw start line (no-op for ranges; kept for consistency)
        * end:   if last call is not unlock/post/create, cut at function end
      First/last call is derived from internal_meta order.
    """
    gen_root = base_dir / "intermediate_results" / base_name / "dag_generation"
    level2_merge_root = base_dir / "intermediate_results" / base_name / "level2" / "merge_post_wait"

    functions_full_path = gen_root / "functions_full.json"
    ranges_path = gen_root / "functions_ranges.json"
    internal_meta_path = gen_root / "debug" / "mycalls_meta_internal.json"

    merged_dot_path = level2_merge_root / "dag_level2_sem.dot"
    if not merged_dot_path.exists():
        merged_dot_path = gen_root / "dag.dot"

    functions_full = _load_functions_full(functions_full_path)
    ranges = _load_functions_ranges(ranges_path)
    internal_meta = _load_internal_meta(internal_meta_path)

    node_owner_line, ordered_nodes_by_fn = _build_node_owner_and_order(internal_meta)
    mutex_intervals_by_fn = _pair_mutex_intervals(internal_meta=internal_meta)

    cuts = _iter_stage1_cut_points(functions_full=functions_full, internal_meta=internal_meta)

    thread_entry_fns: Set[str] = {"main"}
    for c in cuts:
        if c.kind == "create" and c.entry_fn:
            thread_entry_fns.add(c.entry_fn)

    # Build function ranges:
    # - exclude trailing return/brace from segmentation
    # - prefer semantic statement boundaries over function closing brace
    fn_range: Dict[str, Tuple[int, int]] = {}
    for fn, info in ranges.items():
        s = info.get("level1_start_line") or info.get("first_stmt_line") or info.get("start_line")
        last_return = info.get("last_return_line")
        last_stmt = info.get("last_stmt_line")
        level1_end = info.get("level1_end_line")
        end_line = info.get("end_line")

        e: Optional[int] = None
        if isinstance(last_return, int) and last_return > 0:
            # If return exists, do not include return in segment range.
            e = last_return - 1
        elif isinstance(last_stmt, int) and last_stmt > 0:
            # Prefer last statement line (normally before "}").
            e = last_stmt
        elif isinstance(level1_end, int) and level1_end > 0:
            e = level1_end
        elif isinstance(end_line, int) and end_line > 0:
            # Final fallback: avoid including closing brace when only end_line is available.
            e = end_line - 1

        if isinstance(s, int) and isinstance(e, int) and s > 0 and e >= s:
            fn_range[fn] = (s, e)

    # Collect per-function cut-after positions
    cut_after_by_fn: Dict[str, Set[int]] = {}
    create_lines_by_fn: Dict[str, List[int]] = {}
    sem_post_lines_by_fn: Dict[str, List[int]] = {}
    join_lines_by_fn: Dict[str, List[int]] = {}
    sem_wait_lines_by_fn: Dict[str, List[int]] = {}
    lock_lines_by_fn: Dict[str, Set[int]] = {}
    unlock_lines_by_fn: Dict[str, Set[int]] = {}

    source_lines = source_file.read_text(encoding="utf-8", errors="replace").splitlines()
    prev_effective_by_fn, next_effective_by_fn = _build_effective_line_neighbors(
        source_lines=source_lines,
        fn_range=fn_range,
    )

    # Cut points are per source function.  Shared node names of @instanceN
    # clones are globally deduplicated in node_owner_line (last clone wins),
    # so using that map here would give only one clone the lock/unlock cuts.
    # Read each function's own internal_meta entries instead.
    for owner_fn, meta_map in internal_meta.items():
        if owner_fn not in fn_range or not isinstance(meta_map, dict):
            continue
        start_line, _end_line = fn_range[owner_fn]
        for node, meta in meta_map.items():
            if not isinstance(node, str) or not isinstance(meta, dict):
                continue
            line = meta.get("line")
            if not isinstance(line, int):
                continue

            if _CREATE_NODE_RE.search(node):
                cut_after_by_fn.setdefault(owner_fn, set()).add(line)
                create_lines_by_fn.setdefault(owner_fn, []).append(line)
            elif _SEM_POST_NODE_RE.search(node):
                cut_after_by_fn.setdefault(owner_fn, set()).add(line)
                sem_post_lines_by_fn.setdefault(owner_fn, []).append(line)
            elif _MU_UNLOCK_NODE_RE.search(node):
                cut_after_by_fn.setdefault(owner_fn, set()).add(line)
                unlock_lines_by_fn.setdefault(owner_fn, set()).add(line)

            if _JOIN_NODE_RE.search(node):
                if line - 1 >= start_line:
                    cut_after_by_fn.setdefault(owner_fn, set()).add(line - 1)
                join_lines_by_fn.setdefault(owner_fn, []).append(line)
            elif _SEM_WAIT_NODE_RE.search(node):
                if line - 1 >= start_line:
                    cut_after_by_fn.setdefault(owner_fn, set()).add(line - 1)
                sem_wait_lines_by_fn.setdefault(owner_fn, []).append(line)
            elif _MU_LOCK_NODE_RE.search(node):
                if line - 1 >= start_line:
                    cut_after_by_fn.setdefault(owner_fn, set()).add(line - 1)
                lock_lines_by_fn.setdefault(owner_fn, set()).add(line)

    # Thread boundary rules
    entry_cut_applied: Dict[str, bool] = {}
    exit_cut_applied: Dict[str, bool] = {}
    for fn in sorted(thread_entry_fns):
        if fn not in fn_range:
            continue
        nodes = ordered_nodes_by_fn.get(fn, [])
        if not nodes:
            continue
        start, end = fn_range[fn]
        first = nodes[0]
        last = nodes[-1]
        if not (_MU_LOCK_NODE_RE.search(first) or _SEM_WAIT_NODE_RE.search(first)):
            cut_after_by_fn.setdefault(fn, set()).add(start - 1)
            entry_cut_applied[fn] = True
        else:
            entry_cut_applied[fn] = False
        if not (_MU_UNLOCK_NODE_RE.search(last) or _SEM_POST_NODE_RE.search(last) or _CREATE_NODE_RE.search(last)):
            cut_after_by_fn.setdefault(fn, set()).add(end)
            exit_cut_applied[fn] = True
        else:
            exit_cut_applied[fn] = False

    # Remove cuts that would split inside mutex critical sections
    for fn, intervals in mutex_intervals_by_fn.items():
        if fn not in fn_range:
            continue
        cuts_set = cut_after_by_fn.get(fn)
        if not cuts_set:
            continue
        filtered = set(cuts_set)
        for lock_line, unlock_line in intervals:
            for k in list(filtered):
                if lock_line <= k < unlock_line:
                    filtered.discard(k)
        cut_after_by_fn[fn] = filtered

    # Merge rules: bidirectional absorption of sync primitives into
    # adjacent MU blocks.
    #
    # Forward chain (from each unlock): absorb post/create lines.
    # Backward chain (from each lock): absorb wait/join lines.
    #
    # Each direction only absorbs its own type and stops when it meets
    # the other type.  In the typical pattern:
    #   unlock → post → post → wait → wait → lock
    # forward absorbs the posts, backward absorbs the waits, they meet
    # in the middle and stop — post goes to the preceding MU, wait goes
    # to the following MU, no overlap.
    #
    # Cut-point deletion: "cut_after L" means a segment boundary exists
    # after line L.  To merge line X into the segment that ends at line
    # X-1, we remove cut_after(X-1).
    for fn in fn_range.keys():
        fn_cuts = cut_after_by_fn.setdefault(fn, set())
        prev_effective = prev_effective_by_fn.get(fn, {})
        next_effective = next_effective_by_fn.get(fn, {})
        fn_unlock_lines = unlock_lines_by_fn.get(fn, set())
        fn_lock_lines = lock_lines_by_fn.get(fn, set())

        # Build sets for O(1) lookup
        post_or_create = set(create_lines_by_fn.get(fn, []) + sem_post_lines_by_fn.get(fn, []))
        wait_or_join = set(join_lines_by_fn.get(fn, []) + sem_wait_lines_by_fn.get(fn, []))

        # Forward chain: unlock -> post/create -> post/create -> ...
        # Each absorbed line removes the cut *before* it (cur - 1).
        for unlock_line in fn_unlock_lines:
            cur = next_effective.get(unlock_line)
            while isinstance(cur, int) and cur in post_or_create:
                fn_cuts.discard(cur - 1)
                cur = next_effective.get(cur)

        # Backward chain: lock <- wait/join <- wait/join <- ...
        # Each absorbed line removes the cut *after* it (cur itself),
        # which is the boundary between this wait/join and the next line.
        for lock_line in fn_lock_lines:
            cur = prev_effective.get(lock_line)
            while isinstance(cur, int) and cur in wait_or_join:
                fn_cuts.discard(cur)
                cur = prev_effective.get(cur)

    # Build initial segments
    #
    # Important: Level2 segmentation is primarily intended for thread execution (main + thread entry functions)
    # and functions that actually have callsite nodes in internal_meta. Helper leaf functions (e.g. pure compute)
    # often have no internal_meta entries; emitting standalone segments for them makes the segment DAG look like
    # it contains many "isolated blocks" even though their runtime is already included in caller segments.
    segs_by_fn: Dict[str, List[Segment]] = {}
    for fn, (s, e) in fn_range.items():
        if fn not in thread_entry_fns and fn not in ordered_nodes_by_fn:
            continue
        segs_by_fn[fn] = _build_segments_for_function(
            fn,
            start_line=s,
            end_line=e,
            cut_after=cut_after_by_fn.get(fn, set()),
            mutex_intervals=mutex_intervals_by_fn.get(fn, []),
        )

    # Flatten segments (stable order)
    all_segments: List[Segment] = []
    for fn in sorted(segs_by_fn.keys()):
        all_segments.extend(segs_by_fn[fn])

    # Map node->segment
    node_to_seg: Dict[str, str] = {}
    for node, (owner_fn, line) in node_owner_line.items():
        segs = segs_by_fn.get(owner_fn)
        if not segs:
            continue
        idx = _find_segment_index_covering_line(segs, line)
        if idx is None:
            continue
        node_to_seg[node] = segs[idx].seg_id
    for fn, segs in segs_by_fn.items():
        if segs:
            node_to_seg.setdefault(fn, segs[0].seg_id)

    edges: List[Tuple[str, str, str]] = []

    # Intra-function sequential edges
    for fn, segs in segs_by_fn.items():
        for a, b in zip(segs, segs[1:]):
            edges.append((a.seg_id, b.seg_id, "intra"))

    def _find_seg_id(fn: str, line: int) -> Optional[str]:
        segs = segs_by_fn.get(fn)
        if not segs:
            return None
        idx = _find_segment_index_covering_line(segs, line)
        if idx is None:
            return None
        return segs[idx].seg_id

    # The generation metadata maps a handle to the base function name, while the
    # create cut already points at the actual @instanceN execution clone.  Pair
    # each join with the instance created by the same handle so the join edge
    # lands on that instance's executable tail instead of the first clone's.
    create_handle_to_instance: Dict[str, str] = {}
    for c in cuts:
        if c.kind != "create" or not c.entry_fn:
            continue
        if not (1 <= c.line <= len(source_lines)):
            continue
        m = re.search(r"\bpthread_create\s*\(\s*&\s*(\w+)", source_lines[c.line - 1])
        if m:
            create_handle_to_instance[m.group(1)] = c.entry_fn
    instance_groups: Dict[str, List[str]] = {}
    for fn in segs_by_fn:
        instance_groups.setdefault(normalize_func_name(fn), []).append(fn)
    multi_instance_bases = {
        base: sorted(fns) for base, fns in instance_groups.items() if len(fns) > 1
    }
    join_line_to_instance: Dict[int, str] = {}
    join_line_to_handle: Dict[int, str] = {}
    for c in cuts:
        if c.kind != "join":
            continue
        if not (1 <= c.line <= len(source_lines)):
            continue
        m = re.search(r"\bpthread_join\s*\(\s*(\w+)", source_lines[c.line - 1])
        if m:
            join_line_to_handle[c.line] = m.group(1)
            if m.group(1) in create_handle_to_instance:
                join_line_to_instance[c.line] = create_handle_to_instance[m.group(1)]

    # Create/join semantic edges (reuse legacy mapping logic from level1 cut points)
    for c in cuts:
        if c.kind == "create" and c.entry_fn:
            src = _find_seg_id(c.function, c.line)
            dst_list = segs_by_fn.get(c.entry_fn)
            if src and dst_list:
                edges.append((src, dst_list[0].seg_id, "create"))
        elif c.kind == "join":
            dst = _find_seg_id(c.function, c.line)
            entry_fn = join_line_to_instance.get(c.line)
            if entry_fn is None:
                handle = join_line_to_handle.get(c.line, "<unparsed>")
                if c.entry_fn is not None:
                    # A handle that cannot be paired to a create must not
                    # silently fall back to the base function when that source
                    # function has multiple @instanceN executions.
                    base = normalize_func_name(c.entry_fn)
                    candidates = sorted(fn for fn in segs_by_fn if normalize_func_name(fn) == base)
                    if len(candidates) > 1:
                        raise ValueError(
                            f"ambiguous join handle '{handle}' at line {c.line}: "
                            f"multiple execution instances {candidates}; "
                            f"cannot fall back to base function '{c.entry_fn}'")
                    entry_fn = candidates[0] if candidates else c.entry_fn
                elif multi_instance_bases:
                    raise ValueError(
                        f"cannot resolve join handle '{handle}' at line {c.line}: "
                        f"no base function mapping and multiple execution instances "
                        f"{multi_instance_bases}")
            src_list = segs_by_fn.get(entry_fn) if entry_fn is not None else None
            if dst and src_list:
                edges.append((src_list[-1].seg_id, dst, "join"))

    # Semaphore dep edges: strictly from circle.txt generated by collector.
    #
    # Important: prefer the canonical config location under intermediate_results/<base>/config_files/circle.txt.
    # The merge_post_wait directory may contain a stale copy from earlier runs (GUI used to skip export
    # when circle.txt already existed), which would silently drop sem_dep edges due to node-name mismatch.
    circle_path = base_dir / "intermediate_results" / base_name / "config_files" / "circle.txt"
    if not circle_path.exists():
        circle_path = level2_merge_root / "circle.txt"
    if not circle_path.exists():
        raise FileNotFoundError(f"missing circle.txt: {circle_path}")
    sem_calls = _parse_circle_sem_calls(circle_path)
    _validate_semaphore_determinacy(
        sem_calls=sem_calls,
        source_text="\n".join(source_lines),
        fn_range=fn_range,
        node_owner_line=node_owner_line,
    )
    _validate_semaphore_dynamic_instances(
        sem_calls=sem_calls,
        internal_meta=internal_meta,
        node_owner_line=node_owner_line,
    )
    sem_pairs: List[Tuple[str, str]] = _sem_pairs_from_calls(sem_calls)
    _validate_semaphore_initial_counts(
        sem_calls=sem_calls,
        source_text="\n".join(source_lines),
        node_owner_line=node_owner_line,
    )
    _validate_semaphore_branch_pairing(
        sem_calls=sem_calls,
        source_text="\n".join(source_lines),
        fn_range=fn_range,
        node_owner_line=node_owner_line,
    )
    internal_sem_witnesses: List[Dict[str, object]] = []

    for post_node, wait_node in sem_pairs:
        post = node_owner_line.get(post_node)
        wait = node_owner_line.get(wait_node)
        if not post or not wait:
            continue
        post_fn, post_line = post
        wait_fn, wait_line = wait
        sp = _find_seg_id(post_fn, post_line)
        sw = _find_seg_id(wait_fn, wait_line)
        if sp and sw:
            if sp == sw:
                # A determinate pair can fall inside one merged segment.  Keep
                # the witness for projection-aware rules, but only when the
                # source order is provably post-before-wait.  A wait before its
                # post would block in the source, and an unconfirmed same-line
                # order must not be silently absorbed as if it were ordered.
                if post_fn != wait_fn:
                    raise ValueError(
                        f"same-segment sem_dep pair {post_node} -> {wait_node} "
                        "belongs to different execution instances; refusing to "
                        "absorb an internal witness"
                    )
                if post_line < wait_line:
                    ordered = True
                elif post_line > wait_line:
                    ordered = False
                else:
                    post_order = _sem_call_source_offset(
                        node=post_node,
                        kind="post",
                        function=post_fn,
                        line_no=post_line,
                        source_text="\n".join(source_lines),
                        sem_calls=sem_calls,
                        node_owner_line=node_owner_line,
                    )
                    wait_order = _sem_call_source_offset(
                        node=wait_node,
                        kind="wait",
                        function=wait_fn,
                        line_no=wait_line,
                        source_text="\n".join(source_lines),
                        sem_calls=sem_calls,
                        node_owner_line=node_owner_line,
                    )
                    ordered = bool(
                        post_order is not None
                        and wait_order is not None
                        and post_order < wait_order
                    )
                if not ordered:
                    raise ValueError(
                        f"same-segment sem_dep pair {post_node} -> {wait_node} "
                        "is not provably post-before-wait; refusing to absorb "
                        "an internal witness"
                    )
                # A determinate pair can fall inside one merged segment.  Keep
                # the witness for projection-aware rules, but mark it internal
                # so final publication can absorb it instead of exposing a
                # scheduling self-loop.
                internal_sem_witnesses.append(
                    {"src": sp, "dst": sw, "kind": "sem_dep",
                     "post": post_node, "wait": wait_node}
                )
            edges.append((sp, sw, "sem_dep"))

    dag_edges = [{"src": s, "dst": d, "kind": k} for (s, d, k) in edges]

    # Fallback: if we failed to derive any dependency edges (e.g. internal_meta empty),
    # project function-level edges from dag.dot onto each function's first segment.
    if not dag_edges and segs_by_fn:
        fallback = _fallback_edges_from_dot(segs_by_fn=segs_by_fn, dot_path=merged_dot_path)
        if fallback:
            dag_edges = [{"src": s, "dst": d, "kind": k} for (s, d, k) in fallback]

    seg_json = {
        "base_name": base_name,
        "source_file": str(source_file),
        "inputs": {
            "functions_full": str(functions_full_path),
            "functions_ranges": str(ranges_path),
            "mycalls_meta_internal": str(internal_meta_path),
            "merged_dag_dot": str(merged_dot_path),
        },
        "thread_entry_functions": sorted(thread_entry_fns),
        "entry_cut_applied": entry_cut_applied,
        "exit_cut_applied": exit_cut_applied,
        "node_locations": {
            node: {"function": owner, "line": line}
            for node, (owner, line) in sorted(node_owner_line.items())
        },
        "node_to_segment": dict(sorted(node_to_seg.items())),
        "sem_dep_internal_witnesses": internal_sem_witnesses,
        "segments": [
            {
                "seg_id": s.seg_id,
                "function": s.function,
                "kind": s.kind,
                "start_line": s.start_line,
                "end_line": s.end_line,
            }
            for s in all_segments
        ],
    }
    dag_json = {
        "base_name": base_name,
        "nodes": [s.seg_id for s in all_segments],
        "edges": dag_edges,
    }
    return seg_json, dag_json


def _to_dot(seg_json: Dict, dag_json: Dict) -> str:
    segs = {s["seg_id"]: s for s in seg_json.get("segments", []) if isinstance(s, dict)}
    lines: List[str] = []
    lines.append("digraph Level2SegDAG {")
    lines.append("  rankdir=LR;")
    lines.append('  node [shape=box, fontname="Consolas", fontsize=10];')
    thread_palette = [
        "#E3F2FD",
        "#E8F5E9",
        "#FFF3E0",
        "#F3E5F5",
        "#E0F7FA",
        "#FCE4EC",
        "#F1F8E9",
        "#EDE7F6",
    ]
    fn_order = sorted({str(s.get("function", "")) for s in segs.values()})
    fn_color = {fn: thread_palette[i % len(thread_palette)] for i, fn in enumerate(fn_order)}
    for seg_id, s in sorted(segs.items()):
        fn = s.get("function", "")
        kind = s.get("kind", "")
        start = s.get("start_line", "")
        end = s.get("end_line", "")
        fill = fn_color.get(str(fn), "#F6F6F6")
        label = f"{seg_id}\\n{fn} {kind} @{start}-{end}"
        lines.append(f'  "{seg_id}" [style="rounded,filled", fillcolor="{fill}", label="{label}"];')
    edge_color = {
        "intra": "#666666",
        "create": "#1565C0",
        "join": "#2E7D32",
        "sem_dep": "#D32F2F",
    }
    for e in dag_json.get("edges", []):
        if not isinstance(e, dict):
            continue
        src = e.get("src")
        dst = e.get("dst")
        kind = e.get("kind", "intra")
        if src not in segs or dst not in segs:
            continue
        color = edge_color.get(kind, "#666666")
        style = "dashed" if kind == "sem_dep" else "solid"
        penwidth = "2.2" if kind in ("create", "join", "sem_dep") else "1.0"
        lines.append(f'  "{src}" -> "{dst}" [color="{color}", style="{style}", penwidth={penwidth}];')
    lines.append("}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Level-2 segment DAG (create/join/post/wait/mutex rules).")
    ap.add_argument("--base-dir", type=Path, default=Path(__file__).resolve().parents[1])
    ap.add_argument("--base-name", required=True)
    ap.add_argument("--source-file", type=Path, required=True)
    args = ap.parse_args()

    base_dir = args.base_dir.resolve()
    base_name = args.base_name
    source_file = args.source_file.resolve()

    seg_json, dag_json = build_level2_segments_and_dag(base_dir=base_dir, base_name=base_name, source_file=source_file)

    out_dir = base_dir / "intermediate_results" / base_name / "level2" / "stage2"
    out_dir.mkdir(parents=True, exist_ok=True)
    seg_path = out_dir / "segments_level2.json"
    dag_path = out_dir / "dag_level2_seg.json"
    dot_path = out_dir / "dag_level2_seg.dot"

    seg_path.write_text(json.dumps(seg_json, ensure_ascii=False, indent=2), encoding="utf-8")
    dag_path.write_text(json.dumps(dag_json, ensure_ascii=False, indent=2), encoding="utf-8")
    dot_path.write_text(_to_dot(seg_json, dag_json), encoding="utf-8")
    print(f"wrote {seg_path}")
    print(f"wrote {dag_path}")
    print(f"wrote {dot_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
