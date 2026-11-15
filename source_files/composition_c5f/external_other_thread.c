#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *observer(void *arg) {
    if (mode == 0) {
        result += 21;
    } else {
        result += 23;
    }
    return NULL;
}
void *worker(void *arg) {
    result += 100;
    pthread_mutex_lock(&m1);
    result += 2;
    for (int i = 0; i < 3; i++) {
        if (((i + mode) % 2) == 0) {
            result += 1;
        } else {
            result += 2;
        }
        result += 3;
    }
    result += 6;
    pthread_mutex_unlock(&m1);
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t, o; mode = argc - 1;
    pthread_create(&t, NULL, worker, NULL);
    pthread_create(&o, NULL, observer, NULL);
    pthread_join(o, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
