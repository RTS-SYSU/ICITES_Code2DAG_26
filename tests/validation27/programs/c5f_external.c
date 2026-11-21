#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
static pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
    ev("T:worker:start");

    ev("LK:c5f_external:before:m1");
    pthread_mutex_lock(&m1);
    ev("EXT:target:critical");
    for (int i = 0; i < 3; i++) {
        evi("EXT:target:i",i);
        ev("EXT:target:entry");
                if ((i + mode) % 2 == 0) {
             ev("EXT:target:then");
             result += 1;
        } else {
             ev("EXT:target:else");
             result += 2;
        }
        ev("EXT:target:exit");
    }
    ev("LK:c5f_external:critical_end:m1");
    pthread_mutex_unlock(&m1);
    ev("LK:c5f_external:after:m1");
    ev("LK:c5f_external:before:m2");
    pthread_mutex_lock(&m2);
    ev("EXT:lock:critical");
    result += 4;
    ev("LK:c5f_external:critical_end:m2");
    pthread_mutex_unlock(&m2);
    ev("LK:c5f_external:after:m2");
        for (int j = 0; j < 2; j++) {
         evi("EXT:loop:j",j);
         result += 5;
    }
        ev("EXT:cond:entry");
    if (mode % 2 == 0) {
         ev("EXT:cond:then");
         result += 6;
    } else {
         ev("EXT:cond:else");
         result += 7;
    }
    ev("EXT:cond:exit");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:c5f_external:main:start");
    ev("P:c5f_external:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:c5f_external:join:worker");

    ev("P:c5f_external:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
