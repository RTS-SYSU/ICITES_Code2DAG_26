#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t q = PTHREAD_MUTEX_INITIALIZER;
int result;
void *task(void *unused) {
    result += 100;
    for (int i = 0; i < 3; i++) {
        result += 2;
        pthread_mutex_lock(&m);
        result += 1;
        pthread_mutex_unlock(&m);
        result += 5;
        pthread_mutex_lock(&q);
        result += 100;
        pthread_mutex_unlock(&q);
        result += 3;
    }
    result += 7;
    for (int j = 0; j < 2; j++) {
        result += 2;
        pthread_mutex_lock(&m);
        result += 1;
        pthread_mutex_unlock(&m);
        result += 5;
        pthread_mutex_lock(&q);
        result += 100;
        pthread_mutex_unlock(&q);
        result += 3;
    }
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
