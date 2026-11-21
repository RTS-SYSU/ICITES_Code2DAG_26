#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t q = PTHREAD_MUTEX_INITIALIZER;
int result;
void *task(void *arg) {
    result += 100;
    pthread_mutex_lock(&m);
    for (int i = 0; i < 2; i++) {
        result += 2;
    }
    pthread_mutex_unlock(&m);
    result += 7;
    pthread_mutex_lock(&q);
    for (int j = 0; j < 3; j++) {
        result += 4;
    }
    pthread_mutex_unlock(&q);
    result += 1000;
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
