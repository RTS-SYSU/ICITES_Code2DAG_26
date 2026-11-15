#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *worker(void *arg) {
    result += 100; /* PREFIX */
    pthread_mutex_lock(&m1);
    result += 2;
    if (mode == 0) {
        for (int i = 0; i < 0; i++) {
            for (int j = 0; j < 3; j++) {
                result += 1; /* BODY_j */
            }
        }
    } else {
        result += 20;
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
