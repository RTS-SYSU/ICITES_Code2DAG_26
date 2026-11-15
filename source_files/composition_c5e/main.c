#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
void *task(void *arg) { result += 7; return NULL; }
int main(int argc, char **argv) {
    pthread_t t; mode = argc - 1;
    pthread_create(&t, NULL, task, NULL);
    pthread_join(t, NULL);
    result += 100;
    pthread_mutex_lock(&m1);
    result += 2;
    if (mode == 0) {
        for (int i = 0; i < 3; i++) {
            result += 1;
        }
    } else {
        result += 20;
    }
    result += 6;
    pthread_mutex_unlock(&m1);
    result += 1000;
    printf("RESULT:%d\n", result);
    return 0;
}
