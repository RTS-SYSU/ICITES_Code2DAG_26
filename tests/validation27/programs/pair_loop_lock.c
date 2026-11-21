#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
static pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
    ev("T:worker:start");

    for (int i = 0; i < 3; i++) {
        evi("P4a:i",i);
        ev("LK:pair_loop_lock:before:m1");
        pthread_mutex_lock(&m1);
        ev("P4a:critical");
        result += 1;
        ev("LK:pair_loop_lock:critical_end:m1");
        pthread_mutex_unlock(&m1);
        ev("LK:pair_loop_lock:after:m1");
    }

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_loop_lock:main:start");
    ev("P:pair_loop_lock:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_loop_lock:join:worker");

    ev("P:pair_loop_lock:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
