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

    ev("P2a:entry");
    if ((mode % 2) == 0) {
        ev("P2a:then");
        ev("LK:pair_cond_lock:before:m1");
        pthread_mutex_lock(&m1);
        ev("P2a:critical");
        result += 1;
        ev("LK:pair_cond_lock:critical_end:m1");
        pthread_mutex_unlock(&m1);
        ev("LK:pair_cond_lock:after:m1");
        } else {
        
        ev("P2a:else");
        result += 2;
        ev("LK:pair_cond_lock:before:m1");
        pthread_mutex_lock(&m1);
        ev("P2a:critical2");
        result += 3;
        ev("LK:pair_cond_lock:critical_end:m1");
        pthread_mutex_unlock(&m1);
        ev("LK:pair_cond_lock:after:m1");
    }
    ev("P2a:exit");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_cond_lock:main:start");
    ev("P:pair_cond_lock:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_cond_lock:join:worker");

    ev("P:pair_cond_lock:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
