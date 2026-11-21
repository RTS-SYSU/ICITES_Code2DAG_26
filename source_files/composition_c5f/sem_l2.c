#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static sem_t sx;
void *worker(void *arg) {
    result += 100;
    pthread_mutex_lock(&m1);
    result += 2;
    sem_post(&sx);
    sem_post(&sx);
    sem_post(&sx);
    sem_post(&sx);
    sem_post(&sx);
    for (int i = 0; i < 3; i++) {
        if (((i + mode) % 2) == 0) {
            sem_wait(&sx);
            result += 1;
        } else {
            sem_wait(&sx);
            result += 2;
        }
        result += 3;
    }
    result += 4;
    for (int j = 0; j < 2; j++) {
        if (((j + mode) % 2) == 0) {
            sem_wait(&sx);
            result += 5;
        } else {
            sem_wait(&sx);
            result += 6;
        }
        result += 7;
    }
    result += 8;
    pthread_mutex_unlock(&m1);
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t; mode = argc - 1;
    sem_init(&sx, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&sx);
    printf("RESULT:%d\n", result);
    return 0;
}
