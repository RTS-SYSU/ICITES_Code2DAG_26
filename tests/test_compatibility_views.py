"""Regression tests for the core compatibility views.

The tests intentionally start from the public source shapes and exercise the
normal ``run_composition_blocks`` entry point.  They assert that the final
graphs are closed and acyclic and that the compatibility rule did not replace
the Level-2 base graph with a separate source/AST topology.
"""
import json
import tempfile
import unittest
from pathlib import Path

from Code2DAG.pipeline.composition import run_composition_blocks, select_rule
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.tests import test_composition_c1 as c1


THREAD_ARRAY = """\
// SKIP PARAM: --set ana.activated[+] thread --set ana.activated[+] threadJoins
#include <pthread.h>
#include <stdio.h>

int myglobal;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void *t_fun(void *arg) {
  pthread_mutex_lock(&mutex);
  myglobal=myglobal+1; // NORACE
  pthread_mutex_unlock(&mutex);
  return NULL;
}

int main(void) {
  pthread_t id[10];
  int i;
  for (i=0; i<10; i++)
    pthread_create(&id[i], NULL, t_fun, NULL);
  pthread_mutex_lock(&mutex);
  myglobal=myglobal+1; // NORACE
  pthread_mutex_unlock(&mutex);
  for (i=0; i<10; i++)
    pthread_join(id[i], NULL);
  myglobal=myglobal+1; // NORACE
  return 0;
}
"""

HELPER_MUTEX = """\
#include <pthread.h>
#include <stdio.h>

int myglobal;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void munge(pthread_mutex_t *m) {
  pthread_mutex_lock(m);
  myglobal=myglobal+1; // NORACE
  pthread_mutex_unlock(m);
}

void *t_fun(void *arg) {
  munge(&mutex);
  return NULL;
}


int main(void) {
  pthread_t id;
  pthread_create(&id, NULL, t_fun, NULL);
  munge(&mutex);
  pthread_join (id, NULL);
  return 0;
}
"""

HELPER_MUTEX_TWO_LOCKS = """\
#include <pthread.h>
#include <stdio.h>

int myglobal;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;

void munge(pthread_mutex_t *m) {
  pthread_mutex_lock(m);
  myglobal=myglobal+1; // RACE!
  pthread_mutex_unlock(m);
}

void *t_fun(void *arg) {
  munge(&mutex2);
  return NULL;
}


int main(void) {
  pthread_t id;
  pthread_create(&id, NULL, t_fun, NULL);
  munge(&mutex1);
  pthread_join (id, NULL);
  return 0;
}
"""

ERROR_CHECKED_SYNC = """\
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

static int global_val = 0;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void *thread_func(void *arg) {
  int i;
  for (i = 0; i < 100; ++i) {
    if (pthread_mutex_lock(&mutex) != 0) {
      perror("pthread_mutex_lock");
      exit(EXIT_FAILURE);
    }
    ++global_val;
    if (pthread_mutex_unlock(&mutex) != 0) {
      perror("pthread_mutex_unlock");
      exit(EXIT_FAILURE);
    }
  }
  return NULL;
}

int main(void) {
  pthread_t t1, t2;
  if (pthread_create(&t1, NULL, thread_func, NULL) != 0) {
    perror("pthread_create");
    exit(EXIT_FAILURE);
  }
  if (pthread_create(&t2, NULL, thread_func, NULL) != 0) {
    perror("pthread_create");
    exit(EXIT_FAILURE);
  }
  if (pthread_join(t1, NULL) != 0) {
    perror("pthread_join");
    exit(EXIT_FAILURE);
  }
  if (pthread_join(t2, NULL) != 0) {
    perror("pthread_join");
    exit(EXIT_FAILURE);
  }
  printf("%d\\n", global_val);
  return 0;
}
"""

POINTER_ARRAY_MACRO = """\
#include <pthread.h>
#include <stdio.h>

#define N 3

static int shared_value = 0;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
  pthread_mutex_lock(&mutex);
  shared_value += 1;
  pthread_mutex_unlock(&mutex);
  return NULL;
}

int main(void) {
  pthread_t *ids[N];
  pthread_t storage[N];
  int i;
  for (i = 0; i < N; i++)
    ids[i] = &storage[i];
  for (i = 0; i < N; i++)
    pthread_create(ids[i], NULL, worker, NULL);
  for (i = 0; i < N; i++)
    pthread_join(*ids[i], NULL);
  return 0;
}
"""


class CompatibilityViewTests(unittest.TestCase):
    def setUp(self):
        self.helper = c1.CompositionC1Tests()
        self._temps = []

    def tearDown(self):
        for tmp in self._temps:
            tmp.cleanup()

    def _run(self, text, name):
        tmp = tempfile.TemporaryDirectory()
        self._temps.append(tmp)
        root = Path(tmp.name)
        source = root / (name + ".c")
        source.write_text(text)
        if select_rule(text) == "effective_line_merge":
            self.fail("compatibility source was not selected")
        base, pipeline = self.helper.native(root, source)
        result = run_composition_blocks(
            base_dir=base, base_name=name, source_file=source
        )
        block = pipeline / "blocks" / "level2" / result["rule_name"]
        struct_segments = json.loads((block / "segments_struct.json").read_text())
        struct_dag = json.loads((block / "dag_struct.json").read_text())
        sched_segments = json.loads((block / "segments_sched.json").read_text())
        sched_dag = json.loads((block / "dag_sched.json").read_text())
        return result, struct_segments, struct_dag, sched_segments, sched_dag

    def _assert_closed(self, segments, dag, view):
        ids = [row["seg_id"] for row in segments["segments"]]
        self.assertEqual(len(ids), len(set(ids)), f"duplicate {view} id")
        self.assertEqual(set(ids), set(dag["nodes"]), f"{view} index mismatch")
        for edge in dag["edges"]:
            self.assertIn(edge["src"], ids)
            self.assertIn(edge["dst"], ids)
            self.assertNotEqual(edge["src"], edge["dst"])
        topo_sort(ids, [(e["src"], e["dst"]) for e in dag["edges"]])

    def test_thread_array_expands_to_static_instances(self):
        result, ss, sd, es, ed = self._run(THREAD_ARRAY, "compat_array")
        self.assertEqual("compatibility_thread_array", result["rule_name"])
        self._assert_closed(ss, sd, "struct")
        self._assert_closed(es, ed, "sched")
        self.assertEqual(10, sum(e["kind"] == "create" for e in ed["edges"]))
        self.assertEqual(10, sum(e["kind"] == "join" for e in ed["edges"]))
        self.assertEqual(10, len(es["mappings"]["thread_instances"]))
        for instance in es["mappings"]["thread_instances"].values():
            self.assertEqual("SEG:main#001@16-19", instance["create_segment"])
            self.assertEqual("SEG:main#004@24-25", instance["join_segment"])

    def test_helper_mutex_injects_call_and_join_tail(self):
        result, ss, sd, es, ed = self._run(HELPER_MUTEX, "compat_helper")
        self.assertEqual("compatibility_helper_mutex", result["rule_name"])
        self._assert_closed(ss, sd, "struct")
        self._assert_closed(es, ed, "sched")
        self.assertEqual(2, len(es["mappings"]["helper_calls"]))
        self.assertEqual({"mutex"}, set(es["mappings"]["mutex_identity"]))
        self.assertTrue(any(e["kind"] == "call" for e in ed["edges"]))
        self.assertTrue(any(e["kind"] == "return" for e in ed["edges"]))
        self.assertEqual(1, sum(e["kind"] == "join" for e in ed["edges"]))

    def test_helper_mutex_preserves_two_mutex_identities(self):
        result, ss, sd, es, ed = self._run(HELPER_MUTEX_TWO_LOCKS, "compat_two")
        self.assertEqual("compatibility_helper_mutex", result["rule_name"])
        self._assert_closed(ss, sd, "struct")
        self._assert_closed(es, ed, "sched")
        self.assertEqual(
            {"mutex1", "mutex2"}, set(es["mappings"]["mutex_identity"])
        )
        mutexes = [e.get("mutex_id") for e in ed["edges"] if e["kind"] == "call"]
        self.assertEqual({"mutex1", "mutex2"}, set(mutexes))

    def test_error_checked_lock_and_unlock_reuse_base_graph(self):
        result, ss, sd, es, ed = self._run(
            ERROR_CHECKED_SYNC, "compat_error_checked"
        )
        self.assertEqual(
            "compatibility_error_checked_sync", result["rule_name"]
        )
        self._assert_closed(ss, sd, "struct")
        self._assert_closed(es, ed, "sched")
        self.assertGreaterEqual(
            sum(e["kind"] == "create" for e in ed["edges"]), 1
        )
        self.assertGreaterEqual(
            sum(e["kind"] == "join" for e in ed["edges"]), 1
        )
        self.assertEqual(
            "mutex", es["mappings"]["error_checked_sync"]["mutex"]
        )

    def test_pointer_thread_array_with_macro_count(self):
        result, ss, sd, es, ed = self._run(POINTER_ARRAY_MACRO, "compat_pointer_array")
        self.assertEqual("compatibility_thread_array", result["rule_name"])
        self._assert_closed(ss, sd, "struct")
        self._assert_closed(es, ed, "sched")
        self.assertEqual(3, sum(e["kind"] == "create" for e in ed["edges"]))
        self.assertEqual(3, sum(e["kind"] == "join" for e in ed["edges"]))
        self.assertEqual(3, len(es["mappings"]["thread_instances"]))


if __name__ == "__main__":
    unittest.main()
