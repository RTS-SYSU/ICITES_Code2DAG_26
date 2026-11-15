#define _GNU_SOURCE
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>

static sem_t sem_start, sem_done;
static pthread_t thread_worker;
static int value, evaluations;

static void *worker(void *unused) {
    (void)unused;
    sem_wait(&sem_start);
    if ((evaluations++, getenv("C2D_FLAG") != NULL)) {
        puts("optional_work");
        value += 10;
    }
    puts("after_condition");
    value += 100;
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
    printf("value=%d evaluations=%d\n", value, evaluations);
    puts("main:end");
    return 0;
}
