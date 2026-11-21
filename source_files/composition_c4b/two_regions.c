#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t shared_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t ta, tb;
int a, b;
void *thread_a(void *arg) {
    a = 1;
    pthread_mutex_lock(&shared_mutex);
    a += 10;
    for (int i = 0; i < 2; i++) {
        a += 20;
    }
    pthread_mutex_unlock(&shared_mutex);
    a += 100;
    return NULL;
}
void *thread_b(void *arg) {
    b = 2;
    pthread_mutex_lock(&shared_mutex);
    b += 30;
    for (int j = 0; j < 3; j++) {
        b += 40;
    }
    pthread_mutex_unlock(&shared_mutex);
    b += 200;
    return NULL;
}
int main(void) {
    pthread_create(&ta, NULL, thread_a, NULL);
    pthread_create(&tb, NULL, thread_b, NULL);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    printf("RESULT:%d\n", a+b);
    return 0;
}
