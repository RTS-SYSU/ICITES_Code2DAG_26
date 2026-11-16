#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode,result;
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
extern void burn(unsigned);
void *peer_a(void *arg) {
    burn(6000000);
    return NULL;
}
void *peer_b(void *arg) {
    burn(6000000);
    return NULL;
}
void *peer_c(void *arg) {
    burn(6000000);
    return NULL;
}
void *peer_d(void *arg) {
    burn(6000000);
    return NULL;
}
void *lock_peer_a(void *arg) {
    pthread_mutex_lock(&m1);
    burn(8000000);
    pthread_mutex_unlock(&m1);
    return NULL;
}
void *worker(void *arg) {
    burn(500000);
    result += 100;
    for (int i = 0; i < 3; i++) {
        burn(500000);
        result += 1;
        pthread_mutex_lock(&m1);
        burn(500000);
        result += 2;
        if (((i + mode) % 2) == 0) {
            burn(1600000);
            result += 3;
        } else {
            burn(3000000);
            result += 4;
        }
        burn(500000);
        result += 5;
        pthread_mutex_unlock(&m1);
        burn(500000);
        result += 6;
    }
    burn(1800000);
    result += 1000;
    return NULL;
}
int main(int argc, char **argv) {
    pthread_t t, p1, p2, p3, p4, l1;
    mode = argc - 1;
    pthread_create(&p1, NULL, peer_a, NULL);
    pthread_create(&p2, NULL, peer_b, NULL);
    pthread_create(&p3, NULL, peer_c, NULL);
    pthread_create(&p4, NULL, peer_d, NULL);
    pthread_create(&l1, NULL, lock_peer_a, NULL);
    pthread_create(&t, NULL, worker, NULL);
    pthread_join(t, NULL);
    pthread_join(l1, NULL);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(p3, NULL);
    pthread_join(p4, NULL);
    printf("RESULT:%d\n", result);
    return 0;
}
