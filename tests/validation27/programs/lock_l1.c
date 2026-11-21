#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;

void *worker_a(void *arg) {
    puts("EV:T:a:start");
    puts("EV:L1:a:before");
    pthread_mutex_lock(&m1);
    puts("EV:L1:a:critical");
    result += 1;
    puts("EV:L1:a:critical_end");
    pthread_mutex_unlock(&m1);
    puts("EV:L1:a:after");
    puts("EV:T:a:end");
    return NULL;
}
void *worker_b(void *arg) {
    puts("EV:T:b:start");
    puts("EV:L1:b:before");
    pthread_mutex_lock(&m1);
    puts("EV:L1:b:critical");
    result += 2;
    puts("EV:L1:b:critical_end");
    pthread_mutex_unlock(&m1);
    puts("EV:L1:b:after");
    puts("EV:T:b:end");
    return NULL;
}
int main(void) {
static pthread_t a,b;
    pthread_create(&a,NULL,worker_a,NULL);
    pthread_create(&b,NULL,worker_b,NULL);
    pthread_join(a,NULL);
    pthread_join(b,NULL);
    return 0;
}
