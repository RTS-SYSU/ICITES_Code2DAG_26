#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
void *worker(void *arg) {
    ev("T:worker:start");
    ev("C2:outer:entry");
    if (mode < 2) {
        ev("C2:outer:then");
        ev("C2:inner:entry");
                if (mode == 0) {
             ev("C2:inner:then");
             result += 2;
        }
                else {
             ev("C2:inner:else");
             result += 3;
        }
        ev("C2:inner:exit");
        } else {
         ev("C2:outer:else");
         result += 4;
    }
    ev("C2:outer:exit");
    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:cond_c2:main:start");
    ev("P:cond_c2:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:cond_c2:join:worker");

    ev("P:cond_c2:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
