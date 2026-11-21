#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *arg) {
    result += 100;
    for (int i = 0; i < 3; i++) {
        result += 1;
        pthread_mutex_lock(&m1);
        result += 2;
        if (((i + mode) % 2) == 0) {
            result += 3;
        }
        result += 5;
        pthread_mutex_unlock(&m1);
        result += 6;
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
