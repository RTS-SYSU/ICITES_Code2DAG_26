#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
void *worker(void *arg) {
    ev("T:worker:start");

    ev("P3a:entry");
    if ((mode % 2) == 0) {
        ev("P3a:then");
                for (int i = 0; i < 3; i++) {
             evi("P3a:i",i);
             result += 1;
        }
        } else {
         ev("P3a:else");
         result += 2;
    }
    ev("P3a:exit");

    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:pair_cond_loop:main:start");
    ev("P:pair_cond_loop:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:pair_cond_loop:join:worker");

    ev("P:pair_cond_loop:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
