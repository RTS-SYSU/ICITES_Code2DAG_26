#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
void *worker(void *arg) {
    ev("T:worker:start");

    for (int i = 0; i < 3; i++) {
        evi("P3b:i",i);
        ev("P3b:entry");
                if ((i + mode) % 2 == 0) {
             ev("P3b:then");
             result += 1;
        } else {
             ev("P3b:else");
             result += 2;
        }
        ev("P3b:exit");
    }

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_loop_cond:main:start");
    ev("P:pair_loop_cond:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_loop_cond:join:worker");

    ev("P:pair_loop_cond:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
