#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
static sem_t sx;
static sem_t sy;
void *worker(void *arg) {
    result += 100;
    pthread_mutex_lock(&m1);
    result += 2;
    for (int i = 0; i < 2; i++) {
        if (((i + mode) % 2) == 0) {
            result += 1;
        } else {
            result += 2;
        }
        result += 3;
    }
    sem_post(&sx);
    result += 4;
    pthread_mutex_unlock(&m1);
    pthread_mutex_lock(&m2);
    result += 5;
    sem_wait(&sx);
    for (int j = 0; j < 2; j++) {
        if (((j + mode) % 2) == 0) {
            result += 6;
        } else {
            result += 7;
        }
        result += 8;
    }
    result += 9;
    pthread_mutex_unlock(&m2);
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t; mode = argc - 1;
    sem_init(&sx, 0, 1);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&sx);
    printf("RESULT:%d\n", result);
    return 0;
}
