#include <pthread.h>
static pthread_mutex_t mutex_a = PTHREAD_MUTEX_INITIALIZER;
static pthread_t t0, t1, t2;
static int v0, v1, v2;
static void *worker_0(void *arg) {
    (void)arg;
    v0 = 1;
    pthread_mutex_lock(&mutex_a);
    v0 += 10;
    v0 += 20;
    pthread_mutex_unlock(&mutex_a);
    v0 += 100;
    return NULL;
}
static void *worker_1(void *arg) {
    (void)arg;
    v1 = 1;
    pthread_mutex_lock(&mutex_a);
    v1 += 10;
    v1 += 20;
    pthread_mutex_unlock(&mutex_a);
    v1 += 100;
    return NULL;
}
static void *worker_2(void *arg) {
    (void)arg;
    v2 = 1;
    pthread_mutex_lock(&mutex_a);
    v2 += 10;
    v2 += 20;
    pthread_mutex_unlock(&mutex_a);
    v2 += 100;
    return NULL;
}
int main(void) {
    pthread_create(&t0, NULL, worker_0, NULL);
    pthread_create(&t1, NULL, worker_1, NULL);
    pthread_create(&t2, NULL, worker_2, NULL);
    pthread_join(t0, NULL);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    return 0;
}
