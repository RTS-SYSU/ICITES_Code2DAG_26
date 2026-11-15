#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
void *worker(void *unused) {
    result += 100; /* PREFIX */
    for (int i = 0; i < 3; i++) {
        if ((i + mode) % 3 == 0) {
            result += 1;
        } else if ((i + mode) % 3 == 1) {
            result += 10;
        } else {
            result += 100;
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
