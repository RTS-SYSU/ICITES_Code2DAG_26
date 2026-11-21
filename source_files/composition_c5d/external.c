#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
static sem_t sem_x;
void *worker(void *arg) {
    result += 100;
    for (int i = 0; i < 3; i++) {
        result += 1;
        pthread_mutex_lock(&m1);
        result += 2;
        if (((i + mode) % 2) == 0) {
            result += 3;
        } else {
            result += 4;
        }
        result += 5;
        pthread_mutex_unlock(&m1);
        result += 6;
    }
    sem_post(&sem_x);
    sem_wait(&sem_x);
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    sem_init(&sem_x, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    sem_destroy(&sem_x);
    printf("RESULT:%d\n", result);
    return 0;
}
