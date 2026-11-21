#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
void *worker(void *arg) {
    result += 100; /* PREFIX */
    if (mode == 0) {
        result += 10;
    } else {
        if (mode == 1) {
        for (int i = 0; i < 3; i++) {
            result += 1; /* BODY_i */
        }
        } else {
            result += 7;
        }
    }
    result += 1000; /* SUFFIX */
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t;
    mode = argc - 1;
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
