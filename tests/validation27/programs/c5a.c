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

    ev("C5A:entry");
    if ((mode % 2) == 0) {
        ev("C5A:then");
                for (int i = 0; i < 0; i++) {
             evi("C5A:i",i);
             ev("LK:c5a:before:m1");
             pthread_mutex_lock(&m1);
             ev("C5A:critical");
             result += 1;
             ev("LK:c5a:critical_end:m1");
             pthread_mutex_unlock(&m1);
             ev("LK:c5a:after:m1");
        }
        } else {
         ev("C5A:else");
         result += 2;
    }
    ev("C5A:exit");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:c5a:main:start");
    ev("P:c5a:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:c5a:join:worker");

    ev("P:c5a:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
