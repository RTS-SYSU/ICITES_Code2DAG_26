#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int result;
void *observer(void *arg) {
    if (result % 2) {
        result += 11;
    } else {
        result += 17;
    }
    return NULL;
}
void *worker(void *arg) {
    result += 100;
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
void *task(void *arg) {
    result += 5;
    return NULL;
}
int main(void) {
    pthread_t t;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    worker(NULL);
    observer(NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
