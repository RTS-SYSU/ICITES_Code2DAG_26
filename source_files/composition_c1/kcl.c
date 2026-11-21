#include <pthread.h>
#include <stddef.h>
pthread_mutex_t shared_mutex = PTHREAD_MUTEX_INITIALIZER;
int flag;
volatile int value_a;
volatile int value_b;
void *worker_a(void *arg) {
    value_a += 1; /* PREFIX */
    pthread_mutex_lock(&shared_mutex);
    value_a += 17; /* CS_A */
    pthread_mutex_unlock(&shared_mutex);
    if (flag) {
        value_a += 11; /* THEN */
    } else {
        value_a += 13; /* ELSE */
    }
    for (int i = 0; i < 3; i++) {
        value_a += i; /* BODY */
    }
    value_a += 23; /* SUFFIX */
    return NULL;
}
void *worker_b(void *arg) {
    pthread_mutex_lock(&shared_mutex);
    value_b += 19; /* CS_B */
    pthread_mutex_unlock(&shared_mutex);
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t a, b;
    flag = argc - 1;
    pthread_create(&a, NULL, worker_a, NULL);
    pthread_create(&b, NULL, worker_b, NULL);
    pthread_join(a, NULL);
    pthread_join(b, NULL);
    return 0;
}
