#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int result;
static pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
static pthread_t t0, t1;
void *worker_0(void *arg) {
    puts("EV:T:w0:start");
    puts("EV:L2a:before:m1");
    pthread_mutex_lock(&m1);
    puts("EV:L2a:critical:m1");
    result += 1;
    puts("EV:L2a:critical_end:m1");
    pthread_mutex_unlock(&m1);
    puts("EV:L2a:after:m1");
    puts("EV:L2b:before:m2");
    pthread_mutex_lock(&m2);
    puts("EV:L2b:critical:m2");
    result += 2;
    puts("EV:L2b:critical_end:m2");
    pthread_mutex_unlock(&m2);
    puts("EV:L2b:after:m2");
    puts("EV:T:w0:end");
    return NULL;
}
void *worker_1(void *arg) {
    puts("EV:T:w1:start");
    puts("EV:L2c:before:m1");
    pthread_mutex_lock(&m1);
    puts("EV:L2c:critical:m1");
    result += 3;
    puts("EV:L2c:critical_end:m1");
    pthread_mutex_unlock(&m1);
    puts("EV:L2c:after:m1");
    puts("EV:L2d:before:m2");
    pthread_mutex_lock(&m2);
    puts("EV:L2d:critical:m2");
    result += 4;
    puts("EV:L2d:critical_end:m2");
    pthread_mutex_unlock(&m2);
    puts("EV:L2d:after:m2");
    puts("EV:T:w1:end");
    return NULL;
}
int main(void) {
    pthread_create(&t0, NULL, worker_0, NULL);
    pthread_create(&t1, NULL, worker_1, NULL);
    pthread_join(t0, NULL);
    pthread_join(t1, NULL);
    return 0;
}
