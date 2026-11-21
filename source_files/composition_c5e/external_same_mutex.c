#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
void *peer(void *arg) {
    pthread_mutex_lock(&m1);
    result += 7;
    pthread_mutex_unlock(&m1);
    return NULL;
}
void *worker(void *arg) {
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
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t, p;
    mode = argc - 1;
    pthread_create(&p, NULL, peer, NULL);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    pthread_join(p, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
