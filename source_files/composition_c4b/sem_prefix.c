#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
sem_t go;
int result;
void *task(void *arg) {
    result += 100;
    sem_wait(&go);
    pthread_mutex_lock(&m);
    result += 1;
    for (int i = 0; i < 3; i++) {
        result += 2;
    }
    result += 3;
    pthread_mutex_unlock(&m);
    result += 1000;
    return NULL;
}
int main(void) {
    pthread_t t;
    sem_init(&go, 0, 0);
    pthread_create(&t, NULL, task, NULL);
    sem_post(&go);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
