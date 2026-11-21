#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *unused) {
    result += 100; /* PREFIX */
    pthread_mutex_lock(&m1);
    result += 2;
    for (int i = 0; i < 1; i++) {
        if ((i + mode) % 2 == 0) {
            result += 1; /* THEN */
        } else {
            result += 10; /* ELSE */
        }
    }
    result += 6;
    pthread_mutex_unlock(&m1);
    result += 1000; /* SUFFIX */
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
