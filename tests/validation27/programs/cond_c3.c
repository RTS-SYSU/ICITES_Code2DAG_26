#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
void *worker(void *arg) {
    ev("T:worker:start");
    ev("C3:entry");
    if (mode == 0) {
        puts("EV:C3:a");
        result += 1;
        } else if (mode == 1) {
        
        puts("EV:C3:b");
        result += 2;
        } else {
        
        puts("EV:C3:c");
        result += 3;
    }
    ev("C3:exit");
    ev("T:worker:end");
    return NULL;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    ev("P:cond_c3:main:start");
    ev("P:cond_c3:create:worker");
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    ev("P:cond_c3:join:worker");

    ev("P:cond_c3:main:end");
    printf("RESULT:%d\n", result);
    return 0;
}
