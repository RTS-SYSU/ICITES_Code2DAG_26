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

    ev("C5B:entry");
    if ((mode % 2) == 0) {
        ev("C5B:then");
        ev("LK:c5b:before:m1");
        pthread_mutex_lock(&m1);
        ev("C5B:critical");
                for (int i = 0; i < 3; i++) {
             evi("C5B:i",i);
             result += 1;
        }
        ev("LK:c5b:critical_end:m1");
        pthread_mutex_unlock(&m1);
        ev("LK:c5b:after:m1");
        } else {
         ev("C5B:else");
         result += 2;
    }
    ev("C5B:exit");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:c5b:main:start");
    ev("P:c5b:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:c5b:join:worker");

    ev("P:c5b:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
