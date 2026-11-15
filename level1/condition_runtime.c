#define _GNU_SOURCE
#include "condition_runtime.h"
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define C2D_THREADS 64
#define C2D_EVENTS 128
struct event {
    const char *node;
    long tid;
    int requested, rc, query, policy, actual, before_policy, before_priority;
    uint64_t begin, end, set_wall, set_cpu, probe_cpu;
};
static struct event events[C2D_THREADS][C2D_EVENTS];
static int counts[C2D_THREADS];
static atomic_int threads, errors;
static _Thread_local int slot = -1;
static _Thread_local struct event *current;
static int audit, set_priority = 1;
static uint64_t stamp(clockid_t kind) {
    struct timespec t;
    clock_gettime(kind, &t);
    return (uint64_t)t.tv_sec * 1000000000ULL + (uint64_t)t.tv_nsec;
}
void c2d_node_begin(const char *node, int priority) {
    uint64_t cpu = stamp(CLOCK_THREAD_CPUTIME_ID);
    if (current) { atomic_fetch_add(&errors, 1); return; }
    if (slot < 0) slot = atomic_fetch_add(&threads, 1);
    if (slot >= C2D_THREADS || counts[slot] >= C2D_EVENTS) { atomic_fetch_add(&errors, 1); return; }
    struct event *e = &events[slot][counts[slot]++];
    current = e;
    e->node = node; e->tid = (long)syscall(SYS_gettid); e->requested = priority;
    e->begin = stamp(CLOCK_MONOTONIC);
    e->policy = e->actual = e->before_policy = e->before_priority = -1;
    if (audit) {
        struct sched_param old;
        int policy;
        if (!pthread_getschedparam(pthread_self(), &policy, &old)) {
            e->before_policy = policy; e->before_priority = old.sched_priority;
        }
    }
    if (set_priority) {
        struct sched_param sp = {.sched_priority = priority};
        uint64_t c = stamp(CLOCK_THREAD_CPUTIME_ID), w = stamp(CLOCK_MONOTONIC);
        e->rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);
        e->set_wall = stamp(CLOCK_MONOTONIC) - w;
        e->set_cpu = stamp(CLOCK_THREAD_CPUTIME_ID) - c;
    }
    if (audit) {
        struct sched_param actual;
        e->query = pthread_getschedparam(pthread_self(), &e->policy, &actual);
        if (!e->query) e->actual = actual.sched_priority;
    }
    e->probe_cpu = stamp(CLOCK_THREAD_CPUTIME_ID) - cpu;
}
void c2d_node_end(const char *node) {
    uint64_t cpu = stamp(CLOCK_THREAD_CPUTIME_ID);
    if (!current || strcmp(current->node, node)) { atomic_fetch_add(&errors, 1); return; }
    current->end = stamp(CLOCK_MONOTONIC);
    current->probe_cpu += stamp(CLOCK_THREAD_CPUTIME_ID) - cpu;
    current = NULL;
}
int __real_main(int, char **, char **);
int __wrap_main(int argc, char **argv, char **envp) {
    const char *a = getenv("CODE2DAG_AUDIT"), *p = getenv("CODE2DAG_PRIORITY_MODE");
    audit = a && !strcmp(a, "1");
    set_priority = !(p && !strcmp(p, "none"));
    uint64_t start = stamp(CLOCK_MONOTONIC);
    int rc = __real_main(argc, argv, envp);
    uint64_t finish = stamp(CLOCK_MONOTONIC);
    uint64_t first = UINT64_MAX, last = 0, wall = 0, cpu = 0, probe = 0;
    int total = 0, failures = 0, verification_failures = 0;
    int nthreads = atomic_load(&threads);
    for (int t = 0; t < nthreads && t < C2D_THREADS; t++) {
        for (int i = 0; i < counts[t]; i++) {
            struct event *e = &events[t][i];
            if (!e->end) atomic_fetch_add(&errors, 1);
            if (e->begin < first) first = e->begin;
            if (e->end > last) last = e->end;
            wall += e->set_wall; cpu += e->set_cpu; probe += e->probe_cpu; total++;
            if (e->rc) failures++;
            if (audit && set_priority && (e->query || e->policy != SCHED_FIFO || e->actual != e->requested)) verification_failures++;
        }
    }
    int error_count = atomic_load(&errors);
    // Buffered events are emitted only after the measured application interval.
    fprintf(stderr, "C2D_METRICS {\"response_ns\":%llu,\"node_makespan_ns\":%llu,\"set_wall_ns\":%llu,\"set_cpu_ns\":%llu,\"probe_cpu_ns\":%llu,\"events\":%d,\"threads\":%d,\"set_failures\":%d,\"verification_failures\":%d,\"probe_errors\":%d,\"audit\":%d,\"priority_enabled\":%d,\"euid\":%d}\n",
        (unsigned long long)(finish-start), (unsigned long long)(total && last>=first ? last-first : 0),
        (unsigned long long)wall, (unsigned long long)cpu, (unsigned long long)probe,
        total, nthreads, failures, verification_failures, error_count, audit, set_priority, (int)geteuid());
    for (int t = 0; t < nthreads && t < C2D_THREADS; t++) for (int i = 0; i < counts[t]; i++) {
        struct event *e = &events[t][i];
        fprintf(stderr,"C2D_EVENT {\"node\":\"%s\",\"tid\":%ld,\"requested\":%d,\"rc\":%d,\"query\":%d,\"policy\":%d,\"actual\":%d,\"before_policy\":%d,\"before_priority\":%d,\"begin_ns\":%llu,\"end_ns\":%llu}\n",
            e->node, e->tid, e->requested, e->rc, e->query, e->policy, e->actual, e->before_policy, e->before_priority,
            (unsigned long long)e->begin, (unsigned long long)e->end);
    }
    if (!rc && (error_count || (set_priority && (failures || verification_failures)))) return 86;
    return rc;
}
