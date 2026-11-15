#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
int mode;
int result;
void *worker(void *arg) {
    result += 100; /* BEFORE */
    pthread_mutex_lock(&m);
    result += 2; /* COMMON_PREFIX */
    if (mode == 0) {
        result += 10; /* THEN */
    } else {
        result += 20; /* ELSE */
    }
    result += 3; /* COMMON_SUFFIX */
    pthread_mutex_unlock(&m);
    for (int i = 0; i < 3; i++) {
        result += i; /* OTHER_LOOP */
    }
    result += 1000; /* AFTER */
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
