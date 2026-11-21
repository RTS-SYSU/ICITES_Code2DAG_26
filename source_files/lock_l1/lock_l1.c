#include <pthread.h>
static pthread_mutex_t shared_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t ta, tb;
static int a, b;
static void *thread_a(void *arg) {
    (void)arg;
    a = 1;
    pthread_mutex_lock(&shared_mutex);
    a += 10;
    a += 20;
    pthread_mutex_unlock(&shared_mutex);
    a += 100;
    return NULL;
}
static void *thread_b(void *arg) {
    (void)arg;
    b = 2;
    pthread_mutex_lock(&shared_mutex);
    b += 30;
    b += 40;
    pthread_mutex_unlock(&shared_mutex);
    b += 200;
    return NULL;
}
int main(void) {
    pthread_create(&ta, NULL, thread_a, NULL);
    pthread_create(&tb, NULL, thread_b, NULL);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    return 0;
}
