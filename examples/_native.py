"""Small native-generation helper shared by the public examples."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from Code2DAG.pipeline.runner import run_collector


def _gcc() -> str:
    return os.environ.get("CC") or ("gcc-13" if shutil.which("gcc-13") else "gcc")


REPO_ROOT = Path(__file__).resolve().parents[1]
PARENT = REPO_ROOT.parent


def _run(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PARENT))
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")


def build_native(source: Path, root: Path) -> tuple[Path, Path]:
    """Generate expand/threads/full artifacts for *source* under *root*.

    Returns (base_dir, pipeline_dir).  The source file is never modified.
    """
    source = source.resolve()
    root = root.resolve()
    base = root / "native"
    expand = root / "expand"
    expand.mkdir(parents=True, exist_ok=True)
    name = source.stem
    _run([_gcc(), "-std=c11", "-pthread", "-fsyntax-only", str(source)])
    _run([_gcc(), "-O0", "-g", "-fdump-rtl-expand", "-c", str(source), "-o", str(expand / f"{name}.o")])
    dumps = sorted(expand.glob("*.expand"))
    if not dumps:
        raise RuntimeError("GCC produced no .expand dump")
    dump = dumps[0]
    for mode in ("threads", "full"):
        cmd = [
            sys.executable,
            "-B",
            "-m",
            "Code2DAG.generation.legacy",
            str(dump),
            "--source-file",
            str(source),
            "--output-base",
            str(base),
            "--force",
        ]
        if mode == "threads":
            cmd.append("--threads-only")
        _run(cmd)
    result = base / "intermediate_results" / name
    shutil.copyfile(result / "config_files" / f"{name}.dot", result / "dag_generation" / "dag.dot")
    run_collector(base_dir=base, base_name=name, source_file=source)
    return base, result / "pipeline"
