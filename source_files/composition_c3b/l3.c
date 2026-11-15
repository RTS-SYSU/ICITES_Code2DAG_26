#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
int result;
void *worker(void *unused) {
    result += 100; /* PREFIX */
    for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 3; j++) {
        if ((i + j + mode) % 2 == 0) {
            result += 1; /* THEN */
        } else {
            result += 10; /* ELSE */
        }
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
