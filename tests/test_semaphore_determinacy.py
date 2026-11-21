"""Semaphore determinacy regressions for the level-2 base graph."""
import tempfile
import unittest
from pathlib import Path

from Code2DAG.pipeline.composition import run_composition_blocks
from Code2DAG.pipeline.algo.common import topo_sort
from Code2DAG.level2.segment_dag_level2 import build_level2_segments_and_dag
from Code2DAG.tests import test_composition_c1 as c1


UNIQUE_PAIR = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

void *worker(void *arg) {
    sem_wait(&s);
    result += 1;
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    sem_post(&s);
    pthread_join(t, NULL);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

SAME_SEGMENT = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

void *worker(void *arg) {
    result += 1;
    sem_post(&s); sem_wait(&s);
    result += 2;
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

WAIT_BEFORE_POST_SAME_SEGMENT = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>

static sem_t s;

void *worker(void *arg) {
    sem_wait(&s);
    sem_post(&s);
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

MUTUALLY_EXCLUSIVE_BRANCHES = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    if (mode) {
        sem_post(&s);
    } else {
        sem_wait(&s);
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

POST_BRANCH_WAIT_AFTER = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    if (mode) {
        sem_post(&s);
    }
    sem_wait(&s);
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

SAME_BRANCH_POST_WAIT = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    if (mode) {
        sem_post(&s);
        sem_wait(&s);
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

UNCONDITIONAL_POST_CONDITIONAL_WAIT = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    sem_post(&s);
    if (mode) {
        sem_wait(&s);
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

CROSS_THREAD_UNCONDITIONAL_POST_CONDITIONAL_WAIT = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    if (mode) {
        sem_wait(&s);
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    sem_post(&s);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

SWITCH_EXCLUSIVE_CASES = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;

void *worker(void *arg) {
    switch (mode) {
    case 0:
        sem_post(&s);
        break;
    default:
        sem_wait(&s);
        break;
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

GOTO_SKIPS_POST = """\
#include <pthread.h>
#include <semaphore.h>

static sem_t s;
int mode;
int skip;

void *worker(void *arg) {
    if (mode) {
        if (skip) {
            goto wait_label;
        }
        sem_post(&s);
wait_label:
        sem_wait(&s);
    }
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    return 0;
}
"""

AMBIGUOUS = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

void *worker(void *arg) {
    sem_wait(&s);
    sem_post(&s);
    sem_wait(&s);
    sem_post(&s);
    result += 1;
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 1);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

ONLY_POST = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

int main(void) {
    sem_init(&s, 0, 1);
    sem_post(&s);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

ONLY_WAIT = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

int main(void) {
    sem_init(&s, 0, 1);
    sem_wait(&s);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

REVERSE_SAME_THREAD = """\
#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>

int result;
static sem_t s;

void *worker(void *arg) {
    result += 1;
    return NULL;
}

int main(void) {
    pthread_t t;
    sem_init(&s, 0, 1);
    sem_wait(&s);
    pthread_create(&t, NULL, worker, NULL);
    sem_post(&s);
    pthread_join(t, NULL);
    sem_destroy(&s);
    printf("RESULT:%d\\n", result);
    return 0;
}
"""

DYNAMIC_POST_C2A = """\
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>

static int shared = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static sem_t done;

void *worker(void *arg) {
    int mode = *(int *)arg;
    if (mode) {
        pthread_mutex_lock(&lock);
        shared += 1;
        pthread_mutex_unlock(&lock);
    } else {
        shared += 2;
    }
    sem_post(&done);
    return NULL;
}

int main(void) {
    pthread_t t1, t2;
    int one = 1;
    sem_init(&done, 0, 0);
    pthread_create(&t1, NULL, worker, &one);
    pthread_create(&t2, NULL, worker, &one);
    sem_wait(&done);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    sem_destroy(&done);
    printf("shared=%d\\n", shared);
    return 0;
}
"""

DYNAMIC_WAIT_C2A = """\
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>

static int shared = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static sem_t go;

void *consumer(void *arg) {
    int mode = *(int *)arg;
    if (mode) {
        pthread_mutex_lock(&lock);
        shared += 1;
        pthread_mutex_unlock(&lock);
    } else {
        shared += 2;
    }
    sem_wait(&go);
    return NULL;
}

int main(void) {
    pthread_t c1, c2;
    int one = 1;
    sem_init(&go, 0, 0);
    pthread_create(&c1, NULL, consumer, &one);
    pthread_create(&c2, NULL, consumer, &one);
    for (int i = 0; i < 2; i++) {
        sem_post(&go);
    }
    pthread_join(c1, NULL);
    pthread_join(c2, NULL);
    sem_destroy(&go);
    printf("shared=%d\\n", shared);
    return 0;
}
"""


class SemaphoreDeterminacyTests(unittest.TestCase):
    def setUp(self):
        self.helper = c1.CompositionC1Tests()
        self._temps = []

    def tearDown(self):
        for tmp in self._temps:
            tmp.cleanup()

    def _native(self, text, name):
        tmp = tempfile.TemporaryDirectory()
        self._temps.append(tmp)
        root = Path(tmp.name)
        source = root / (name + ".c")
        source.write_text(text)
        base, pipeline = self.helper.native(root, source)
        return source, base, pipeline

    @staticmethod
    def _sem_edges(dag):
        return [edge for edge in dag["edges"] if edge.get("kind") == "sem_dep"]

    def test_unique_pair_is_published(self):
        source, base, _ = self._native(UNIQUE_PAIR, "unique_pair")
        _, dag = build_level2_segments_and_dag(
            base_dir=base, base_name="unique_pair", source_file=source
        )
        sem = self._sem_edges(dag)
        self.assertEqual(1, len(sem))
        self.assertNotEqual(sem[0]["src"], sem[0]["dst"])
        topo_sort(dag["nodes"], [(edge["src"], edge["dst"]) for edge in dag["edges"]])

    def test_same_path_multiple_post_wait_is_rejected(self):
        source, base, pipeline = self._native(AMBIGUOUS, "ambiguous")
        with self.assertRaisesRegex(ValueError, r"ambiguous semaphore .*same path"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="ambiguous", source_file=source
            )
        with self.assertRaisesRegex(ValueError, r"ambiguous semaphore .*same path"):
            run_composition_blocks(
                base_dir=base, base_name="ambiguous", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/effective_line_merge/segments_sched.json").exists()
        )

    def test_same_segment_pair_is_recorded_not_published_as_self_loop(self):
        source, base, pipeline = self._native(SAME_SEGMENT, "same_seg")
        raw, dag = build_level2_segments_and_dag(
            base_dir=base, base_name="same_seg", source_file=source
        )
        base_sem = self._sem_edges(dag)
        self.assertEqual(1, len(base_sem))
        self.assertEqual(base_sem[0]["src"], base_sem[0]["dst"])
        self.assertEqual(1, len(raw.get("sem_dep_internal_witnesses", [])))

        run_composition_blocks(base_dir=base, base_name="same_seg", source_file=source)
        final = pipeline / "blocks/level2/effective_line_merge"
        final_dag = __import__("json").loads((final / "dag_sched.json").read_text())
        final_segments = __import__("json").loads((final / "segments_sched.json").read_text())
        self.assertFalse(
            any(
                edge.get("kind") == "sem_dep" and edge.get("src") == edge.get("dst")
                for edge in final_dag["edges"]
            )
        )
        absorbed = final_segments["mappings"]["absorbed_internal_sem_dependencies"]
        self.assertEqual(1, len(absorbed))
        self.assertEqual("sem_dep", absorbed[0]["edge"]["kind"])
        topo_sort(
            final_dag["nodes"],
            [(edge["src"], edge["dst"]) for edge in final_dag["edges"]],
        )

    def test_wait_before_post_in_same_segment_is_rejected(self):
        source, base, pipeline = self._native(
            WAIT_BEFORE_POST_SAME_SEGMENT, "wait_before_post_same_seg"
        )
        with self.assertRaisesRegex(ValueError, r"same-segment|post.*wait|order"):
            build_level2_segments_and_dag(
                base_dir=base,
                base_name="wait_before_post_same_seg",
                source_file=source,
            )
        with self.assertRaisesRegex(ValueError, r"same-segment|post.*wait|order"):
            run_composition_blocks(
                base_dir=base,
                base_name="wait_before_post_same_seg",
                source_file=source,
            )
        self.assertFalse(
            (
                pipeline
                / "blocks/level2/effective_line_merge/segments_sched.json"
            ).exists()
        )

    def _assert_rejected(self, text, name, pattern=r"semaphore|branch|post.*wait|path"):
        source, base, pipeline = self._native(text, name)
        with self.assertRaisesRegex(ValueError, pattern):
            build_level2_segments_and_dag(
                base_dir=base, base_name=name, source_file=source
            )
        with self.assertRaisesRegex(ValueError, pattern):
            run_composition_blocks(
                base_dir=base, base_name=name, source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/conditional_branches/segments_sched.json").exists()
        )

    def test_post_and_wait_in_mutually_exclusive_branches_are_rejected(self):
        self._assert_rejected(
            MUTUALLY_EXCLUSIVE_BRANCHES, "mutually_exclusive_sem"
        )

    def test_post_branch_wait_after_branch_is_rejected(self):
        self._assert_rejected(POST_BRANCH_WAIT_AFTER, "post_branch_wait_after")

    def test_switch_exclusive_cases_are_rejected(self):
        self._assert_rejected(SWITCH_EXCLUSIVE_CASES, "switch_exclusive_sem")

    def test_goto_skipping_post_is_rejected(self):
        self._assert_rejected(GOTO_SKIPS_POST, "goto_skips_post")

    def test_same_branch_post_before_wait_is_accepted(self):
        source, base, pipeline = self._native(
            SAME_BRANCH_POST_WAIT, "same_branch_post_wait"
        )
        result = run_composition_blocks(
            base_dir=base, base_name="same_branch_post_wait", source_file=source
        )
        self.assertEqual("success", result.get("status"))
        block = pipeline / "blocks/level2/conditional_branches"
        dag = __import__("json").loads((block / "dag_sched.json").read_text())
        segments = __import__("json").loads((block / "segments_sched.json").read_text())
        sem = [edge for edge in dag["edges"] if edge.get("kind") == "sem_dep"]
        self.assertEqual(1, len(sem))
        rows = {row["seg_id"]: row for row in segments["segments"]}
        self.assertEqual(
            ["then"],
            [m["branch_id"] for m in rows[sem[0]["src"]].get("condition_memberships", [])],
        )
        self.assertEqual(
            ["then"],
            [m["branch_id"] for m in rows[sem[0]["dst"]].get("condition_memberships", [])],
        )

    def test_unconditional_post_conditional_wait_is_accepted(self):
        source, base, pipeline = self._native(
            UNCONDITIONAL_POST_CONDITIONAL_WAIT,
            "unconditional_post_conditional_wait",
        )
        result = run_composition_blocks(
            base_dir=base,
            base_name="unconditional_post_conditional_wait",
            source_file=source,
        )
        self.assertEqual("success", result.get("status"))
        dag = __import__("json").loads(
            (
                pipeline
                / "blocks/level2/conditional_branches/dag_sched.json"
            ).read_text()
        )
        self.assertEqual(
            1, sum(edge.get("kind") == "sem_dep" for edge in dag["edges"])
        )

    def test_cross_thread_unconditional_post_conditional_wait_is_accepted(self):
        source, base, pipeline = self._native(
            CROSS_THREAD_UNCONDITIONAL_POST_CONDITIONAL_WAIT,
            "cross_thread_unconditional_post_conditional_wait",
        )
        result = run_composition_blocks(
            base_dir=base,
            base_name="cross_thread_unconditional_post_conditional_wait",
            source_file=source,
        )
        self.assertEqual("success", result.get("status"))

    def test_nonzero_initial_count_wait_before_post_is_rejected(self):
        source, base, pipeline = self._native(REVERSE_SAME_THREAD, "reverse_nonzero")
        with self.assertRaisesRegex(ValueError, r"initial count|sem_dep relation"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="reverse_nonzero", source_file=source
            )
        with self.assertRaisesRegex(ValueError, r"initial count|sem_dep relation"):
            run_composition_blocks(
                base_dir=base, base_name="reverse_nonzero", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/effective_line_merge/segments_sched.json").exists()
        )

    def test_nonzero_initial_count_same_segment_is_rejected(self):
        text = SAME_SEGMENT.replace("sem_init(&s, 0, 0)", "sem_init(&s, 0, 1)")
        source, base, pipeline = self._native(text, "same_seg_nonzero")
        with self.assertRaisesRegex(ValueError, r"initial count|sem_dep relation"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="same_seg_nonzero", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/effective_line_merge/segments_sched.json").exists()
        )

    def test_nonliteral_initial_count_is_rejected(self):
        text = REVERSE_SAME_THREAD.replace("sem_init(&s, 0, 1)", "sem_init(&s, 0, 1 + 0)")
        source, base, pipeline = self._native(text, "reverse_nonliteral")
        with self.assertRaisesRegex(ValueError, r"initial count|sem_dep relation"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="reverse_nonliteral", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/effective_line_merge/segments_sched.json").exists()
        )

    def test_zero_initial_count_same_thread_reverse_is_rejected_by_final_cycle(self):
        text = REVERSE_SAME_THREAD.replace("sem_init(&s, 0, 1)", "sem_init(&s, 0, 0)")
        source, base, pipeline = self._native(text, "reverse_zero")
        _, dag = build_level2_segments_and_dag(
            base_dir=base, base_name="reverse_zero", source_file=source
        )
        sem = self._sem_edges(dag)
        self.assertEqual(1, len(sem))
        self.assertNotEqual(sem[0]["src"], sem[0]["dst"])
        with self.assertRaisesRegex(Exception, r"acyclic|cycle"):
            run_composition_blocks(
                base_dir=base, base_name="reverse_zero", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/effective_line_merge/segments_sched.json").exists()
        )

    def test_multi_instance_post_pair_is_rejected(self):
        source, base, pipeline = self._native(DYNAMIC_POST_C2A, "dynamic_post")
        with self.assertRaisesRegex(ValueError, r"dynamic thread instance|unique dynamic pairing"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="dynamic_post", source_file=source
            )
        with self.assertRaisesRegex(ValueError, r"dynamic thread instance|unique dynamic pairing"):
            run_composition_blocks(
                base_dir=base, base_name="dynamic_post", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/composition_c2a/segments_sched.json").exists()
        )

    def test_multi_instance_wait_pair_is_rejected(self):
        source, base, pipeline = self._native(DYNAMIC_WAIT_C2A, "dynamic_wait")
        with self.assertRaisesRegex(ValueError, r"dynamic thread instance|unique dynamic pairing"):
            build_level2_segments_and_dag(
                base_dir=base, base_name="dynamic_wait", source_file=source
            )
        with self.assertRaisesRegex(ValueError, r"dynamic thread instance|unique dynamic pairing"):
            run_composition_blocks(
                base_dir=base, base_name="dynamic_wait", source_file=source
            )
        self.assertFalse(
            (pipeline / "blocks/level2/composition_c2a/segments_sched.json").exists()
        )

    def test_only_post_or_only_wait_has_no_sem_dependency(self):
        for text, name in ((ONLY_POST, "only_post"), (ONLY_WAIT, "only_wait")):
            with self.subTest(name=name):
                source, base, pipeline = self._native(text, name)
                _, dag = build_level2_segments_and_dag(
                    base_dir=base, base_name=name, source_file=source
                )
                self.assertEqual([], self._sem_edges(dag))
                run_composition_blocks(base_dir=base, base_name=name, source_file=source)
                final = pipeline / "blocks/level2/effective_line_merge"
                self.assertTrue((final / "segments_sched.json").exists())
                final_dag = __import__("json").loads((final / "dag_sched.json").read_text())
                self.assertFalse(any(edge.get("kind") == "sem_dep" for edge in final_dag["edges"]))


if __name__ == "__main__":
    unittest.main()
