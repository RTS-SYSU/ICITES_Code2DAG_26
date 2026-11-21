#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <semaphore.h>
static int mode;
static int result;
static void ev(const char *what) { printf("EV:%s\n",what); }
static void evi(const char *what,int n) { printf("EV:%s:%d\n",what,n); }
static sem_t gate;
int helper_observe(int x);
static pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;

void *worker(void *arg) {
    ev("T:worker:start");
    ev("LK:coexist:before:m1");
    pthread_mutex_lock(&m1);
    ev("CO:target:critical");
    for (int i = 0; i < 3; i++) {
        evi("CO:target:i",i);
        ev("CO:target:entry");
                if ((i + mode) % 2 == 0) {
             ev("CO:target:then");
             result += helper_observe(i);
        } else {
             ev("CO:target:else");
             result += helper_observe(i);
        }
        ev("CO:target:exit");
    }
    ev("LK:coexist:critical_end:m1");
    pthread_mutex_unlock(&m1);
    ev("LK:coexist:after:m1");
    ev("LK:coexist:before:m2");
    pthread_mutex_lock(&m2);
    ev("CO:lock:critical");
    result += 4;
    ev("LK:coexist:critical_end:m2");
    pthread_mutex_unlock(&m2);
    ev("LK:coexist:after:m2");
        for (int j = 0; j < 2; j++) {
         evi("CO:loop:j",j);
         result += 5;
    }
        ev("CO:cond:entry");
        if (mode % 2 == 0) {
         ev("CO:cond:then");
         result += 6;
    } else {
         ev("CO:cond:else");
         result += 7;
    }
    ev("CO:cond:exit");
    ev("T:worker:end");
    ev("P:coexist:sem:post");
    sem_post(&gate);
    return NULL;
}
int helper_observe(int x) {
    int value;
    ev("CO:helper:entry");
    if (x % 2 == 0) {
        ev("CO:helper:then");
        value = 3;
        } else {
        
        ev("CO:helper:else");
        value = 4;
    }
    ev("CO:helper:exit");
    return value;
}
int main(int argc, char **argv) {
    mode = argc - 1;
    pthread_t t;
    sem_init(&gate,0,0);
    ev("P:coexist:main:start");
    ev("P:coexist:create:worker");
    pthread_create(&t,NULL,worker,NULL);
    sem_wait(&gate);
    ev("P:coexist:sem:wait");
    ev("P:coexist:sem:done");
    pthread_join(t,NULL);
    ev("P:coexist:join:worker");
    ev("P:coexist:main:end");
    sem_destroy(&gate);
    printf("RESULT:%d\n",result);
    return 0;
}
