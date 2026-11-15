#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <semaphore.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static sem_t sx;
void *worker(void *arg) {
    result += 100; /* PREFIX */
    pthread_mutex_lock(&m1);
    result += 2;
    sem_post(&sx);
    if (mode == 0) {
        sem_wait(&sx);
        for (int i = 0; i < 3; i++) {
            result += 1; /* BODY_i */
        }
    } else {
        sem_wait(&sx);
        result += 20;
    }
    result += 6;
    pthread_mutex_unlock(&m1);
    result += 1000; /* SUFFIX */
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    sem_init(&sx, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&sx);
    printf("RESULT:%d\n", result);
    return 0;
}
