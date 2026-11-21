#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int mode,result;
void *worker(void *arg) { result += 10; return NULL; }
int main(int argc, char **argv) {
    pthread_t t; mode = argc - 1;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    result += 100;
    for (int i = 0; i < 3; i++) {
        if (mode == 0) {
            pthread_mutex_lock(&m1);
            result += 2;
            pthread_mutex_unlock(&m1);
        } else {
            result += 7;
        }
    }
    result += 1000;
    printf("RESULT:%d\n", result);
    return 0;
}
