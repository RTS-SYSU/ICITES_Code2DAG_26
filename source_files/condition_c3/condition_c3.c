#define _GNU_SOURCE
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>

static sem_t sem_start, sem_done;
static pthread_t thread_worker;
static int mode, value, outer_evaluations, inner_evaluations;

static void *worker(void *unused) {
    (void)unused;
    sem_wait(&sem_start);
    if ((outer_evaluations++, mode == 0)) {
        puts("branch_zero");
        value = 10;
    } else if ((inner_evaluations++, mode == 1)) {
        puts("branch_one");
        value = 20;
    } else {
        puts("branch_other");
        value = 30;
    }
    puts("after_condition");
    value += 100;
    sem_post(&sem_done);
    return NULL;
}

int main(void) {
    const char *arg = getenv("C2D_MODE");
    mode = arg ? atoi(arg) : 7;
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
