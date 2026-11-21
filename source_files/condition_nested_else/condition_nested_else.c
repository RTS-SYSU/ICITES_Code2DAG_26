#define _GNU_SOURCE
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>

static sem_t sem_start, sem_done;
static pthread_t thread_worker;
static int value, outer_evaluations, inner_evaluations;

static void *worker(void *unused) {
    (void)unused;
    sem_wait(&sem_start);
    if ((outer_evaluations++, getenv("C2D_OUTER") != NULL)) {
        puts("outer_then");
        value += 1000;
    } else {
        puts("outer_else_before");
        value += 1;
        if ((inner_evaluations++, getenv("C2D_INNER") != NULL)) {
            puts("inner_then");
            value += 10;
        } else {
            puts("inner_else");
            value += 20;
        }
        puts("outer_else_after");
        value += 100;
    }
    sem_post(&sem_done);
    return NULL;
}

int main(void) {
    puts("main:start");
    sem_init(&sem_start, 0, 0);
    sem_init(&sem_done, 0, 0);
    pthread_create(&thread_worker, NULL, worker, NULL);
    sem_post(&sem_start);
    sem_wait(&sem_done);
    pthread_join(thread_worker, NULL);
    printf("value=%d outer_evaluations=%d inner_evaluations=%d\n", value, outer_evaluations, inner_evaluations);
    puts("main:end");
    return 0;
}
