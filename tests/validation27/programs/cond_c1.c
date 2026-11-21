#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
void *worker(void *arg) {
    ev("T:worker:start");
    ev("C1:entry");
        if ((mode % 2) == 0) {
         ev("C1:then");
         result += 2;
    }
        else {
         ev("C1:else");
         result += 3;
    }
    ev("C1:exit");
    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:cond_c1:main:start");
    ev("P:cond_c1:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:cond_c1:join:worker");

    ev("P:cond_c1:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
