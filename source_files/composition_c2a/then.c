#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int mode;
int result;
void *worker(void *arg) {
    result += 100;
    if (mode == 0) {
        pthread_mutex_lock(&m);
        result += 1;
        printf("CS:T1\n");
        pthread_mutex_unlock(&m);
        pthread_mutex_lock(&m);
        result += 1;
        printf("CS:T2\n");
        pthread_mutex_unlock(&m);
    } else {
        result += 20;
    }
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
