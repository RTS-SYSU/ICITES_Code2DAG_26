#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static sem_t sem_start, sem_done;
int mode;
int result;
void *worker(void *arg) {
    sem_wait(&sem_start);
    result += 100;
    if (mode == 0) {
        result += 2;
        pthread_mutex_lock(&m1);
        result += 4;
        for (int i = 0; i < 3; i++) {
            result += 3;
        }
        result += 6;
        pthread_mutex_unlock(&m1);
        result += 8;
    } else {
        result += 7;
    }
    result += 1000;
    sem_post(&sem_done);
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    sem_init(&sem_start, 0, 0);
    sem_init(&sem_done, 0, 0);
    pthread_create(&t, NULL, worker, NULL);
    sem_post(&sem_start);
    sem_wait(&sem_done);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
