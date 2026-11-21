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

    ev("LK:pair_lock_cond:before:m1");
    pthread_mutex_lock(&m1);
    ev("P2b:critical");
    ev("P2b:entry");
        if ((mode % 2) == 0) {
         ev("P2b:then");
         result += 1;
    } else {
         ev("P2b:else");
         result += 2;
    }
    ev("P2b:exit");
    ev("LK:pair_lock_cond:critical_end:m1");
    pthread_mutex_unlock(&m1);
    ev("LK:pair_lock_cond:after:m1");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_lock_cond:main:start");
    ev("P:pair_lock_cond:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_lock_cond:join:worker");

    ev("P:pair_lock_cond:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
