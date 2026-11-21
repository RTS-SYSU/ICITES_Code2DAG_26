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

    ev("LK:pair_lock_loop:before:m1");
    pthread_mutex_lock(&m1);
    ev("P4b:critical");
        for (int i = 0; i < 3; i++) {
         evi("P4b:i",i);
         result += 1;
    }
    ev("LK:pair_lock_loop:critical_end:m1");
    pthread_mutex_unlock(&m1);
    ev("LK:pair_lock_loop:after:m1");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_lock_loop:main:start");
    ev("P:pair_lock_loop:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_lock_loop:join:worker");

    ev("P:pair_lock_loop:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
