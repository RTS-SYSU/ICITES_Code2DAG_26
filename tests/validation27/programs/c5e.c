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

    ev("LK:c5e:before:m1");
    pthread_mutex_lock(&m1);
    ev("C5E:critical");
    ev("C5E:entry");
    if ((mode % 2) == 0) {
        ev("C5E:then");
                for (int i = 0; i < 3; i++) {
             evi("C5E:i",i);
             result += 1;
        }
        } else {
         ev("C5E:else");
         result += 2;
    }
    ev("C5E:exit");
    ev("LK:c5e:critical_end:m1");
    pthread_mutex_unlock(&m1);
    ev("LK:c5e:after:m1");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:c5e:main:start");
    ev("P:c5e:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:c5e:join:worker");

    ev("P:c5e:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
