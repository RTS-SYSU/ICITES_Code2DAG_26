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

    for (int i = 0; i < 1; i++) {
        evi("C5D:i",i);
        ev("LK:c5d:before:m1");
        pthread_mutex_lock(&m1);
        ev("C5D:critical");
        ev("C5D:entry");
                if ((i + mode) % 2 == 0) {
             ev("C5D:then");
             result += 1;
        } else {
             ev("C5D:else");
             result += 2;
        }
        ev("C5D:exit");
        ev("LK:c5d:critical_end:m1");
        pthread_mutex_unlock(&m1);
        ev("LK:c5d:after:m1");
    }

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:c5d:main:start");
    ev("P:c5d:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:c5d:join:worker");

    ev("P:c5d:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
