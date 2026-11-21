"""Raw graph regression: full-mode create continuation must not self-loop."""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EDGE_RE = re.compile(r'^\s*"([^"]+)"\s*->\s*"([^"]+)"\s*;')
SOURCE_TEXT = """\
#include <pthread.h>
extern void burn(unsigned);
static void *worker(void *arg) { (void)arg; burn(10); return 0; }
int main(void) {
    pthread_t t;
    pthread_create(&t, 0, worker, 0);
    pthread_join(t, 0);
    return 0;
}
"""


def edges(path):
    return [EDGE_RE.match(line).groups() for line in path.read_text().splitlines()
            if EDGE_RE.match(line)]


class GenerationFullGraphTests(unittest.TestCase):
    def test_full_create_continuation_keeps_worker_join_without_self_edge(self):
        self.assertIsNotNone(shutil.which('gcc-13'), 'native GCC13 required; no skip')
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            source = temp / 'create_join.c'
            source.write_text(SOURCE_TEXT)
            expand = temp / 'expand'
            expand.mkdir()
            base = temp / 'base'
            self._run(['gcc-13', '-std=c11', '-pthread', '-fsyntax-only', source])
            self._run(['gcc-13', '-O0', '-g', '-fdump-rtl-expand', '-c', source,
                       '-o', expand / 'create_join.o'])
            dump = next(expand.glob('*.expand'))
            for mode in ('threads', 'full'):
                self._run([sys.executable, '-B', '-m', 'Code2DAG.generation.legacy',
                           dump, '--source-file', source, '--output-base', base,
                           '--force', *(['--threads-only'] if mode == 'threads' else [])])
            result = base / 'intermediate_results' / 'create_join' / 'config_files'
            full = edges(result / 'create_join.dot')
            threads = edges(result / 'create_join_threads.dot')
            for label, graph in (('full', full), ('threads', threads)):
                self.assertFalse([(a, b) for a, b in graph if a == b], label)
                self.assertIn(('main/pthread_create1', 'main/pthread_join2'), graph, label)
                self.assertIn(('worker/burn1', 'main/pthread_join2'), graph, label)
            self.assertEqual(
                2, len([(a, b) for a, b in full if b == 'main/pthread_join2']))
            self.assertEqual(
                2, len([(a, b) for a, b in threads if b == 'main/pthread_join2']))
            full_nodes = {a for a, _ in full} | {b for _, b in full}
            self.assertTrue({'main', 'main/pthread_create1', 'main/pthread_join2',
                             'worker', 'worker/burn1'} <= full_nodes)

    @staticmethod
    def _run(args):
        proc = subprocess.run([str(a) for a in args], cwd=ROOT,
                              capture_output=True, text=True, timeout=120)
        if proc.returncode:
            raise AssertionError((args, proc.stdout[-2000:], proc.stderr[-3000:]))


if __name__ == '__main__':
    unittest.main()
