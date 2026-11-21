from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List


def _feature_insert_at(lines: List[str]) -> int:
    feature_def_re = re.compile(r"^\s*#\s*define\s+_(GNU|DEFAULT|POSIX|XOPEN|BSD|SVID)_SOURCE\b")
    insert_at = 0
    for i, ln in enumerate(lines):
        if feature_def_re.match(ln):
            insert_at = i + 1
            continue
        break
    return insert_at


def instrument_prio_all_segments_by_start_line(
    source_c: Path,
    *,
    segments_json: Path,
    priorities: Dict[str, int],
    out_c: Path,
    segment_trace: bool = False,
) -> List[str]:
    warnings: List[str] = []
    lines0 = source_c.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    insert_at = _feature_insert_at(lines0)
    header = "condition_runtime.h" if segment_trace else "prio_runtime.h"
    lines = lines0[:insert_at] + [f'#include "{header}"\n'] + lines0[insert_at:]

    seg_data = json.loads(segments_json.read_text(encoding="utf-8"))
    capabilities = seg_data.get("capabilities", {})
    if isinstance(capabilities, dict) and capabilities.get("instrumentation_compatible") is False:
        raise ValueError("analysis-only DAG: C instrumentation disabled")
    if isinstance(capabilities, dict) and capabilities.get("scheduler_compatible") is False:
        raise ValueError(f"condition model is not instrumentation-compatible: {capabilities.get('blocked_reasons', [])}")
    segments = seg_data.get("segments", [])

    include_shift = 1
    inserts_prio: Dict[int, List[int]] = {}
    inserts_begin = {}
    inserts_end = {}

    for seg in segments:
        if not isinstance(seg, dict):
            continue
        seg_id = seg.get("seg_id")
        start_line = seg.get("start_line")
        if not isinstance(seg_id, str) or not isinstance(start_line, int):
            continue
        prio = priorities.get(seg_id)
        if prio is None:
            continue
        ins_line = start_line + include_shift
        inserts_prio.setdefault(ins_line, []).append(int(prio))
        if segment_trace:
            end_line = seg.get("end_line")
            if not isinstance(end_line, int) or end_line < start_line:
                raise ValueError("invalid segment range")
            inserts_begin.setdefault(ins_line, []).append((seg_id, int(prio)))
            inserts_end.setdefault(end_line + include_shift, []).append(seg_id)

    out: List[str] = []
    for i in range(1, len(lines) + 1):
        prios = inserts_prio.get(i, [])
        if prios and not segment_trace:
            indent = re.match(r"[ \t]*", lines[i - 1]).group(0)  # type: ignore[union-attr]
            for prio in prios:
                out.append(f"{indent}l1_set_thread_prio_fifo({prio});\n")
        for sid, prio in inserts_begin.get(i, []):
            out.append(f"c2d_node_begin({json.dumps(sid)}, {prio});\n")
        out.append(lines[i - 1])
        for sid in inserts_end.get(i, []):
            out.append(f"c2d_node_end({json.dumps(sid)});\n")
    out_c.write_text("".join(out), encoding="utf-8")
    return warnings
