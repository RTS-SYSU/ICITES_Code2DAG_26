#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;
int choose_outer, choose_inner, choose_left, choose_right;
long result_0;
long result_1;
long result_2;
long result_3;
long result_4;
long result_5;
long result_6;
long result_7;
long result_8;
long result_9;
long result_10;
long result_11;
long result_12;
long result_13;
long result_14;
long result_15;
long result_16;
long result_17;
long result_18;
extern void burn(unsigned);
pthread_t h3, h4;
pthread_t h5, h6, h7;

void *task_3(void *arg) {
    burn(845584);
    result_3 += 4;
    if (choose_left == 0) {
        burn(1294772);
        result_7 += 8;
    } else {
        burn(1062398);
        result_8 += 9;
    }
    return NULL;
}

void *task_4(void *arg) {
    burn(205810);
    result_4 += 5;
    if (choose_right == 0) {
        burn(745346);
        result_9 += 10;
    } else {
        burn(1348536);
        result_10 += 11;
    }
    return NULL;
}

void *task_5(void *arg) {
    pthread_mutex_lock(&m1);
    burn(205810);
    pthread_mutex_unlock(&m1);
    burn(1965096);
    result_16 += 17;
    return NULL;
}

void *task_7(void *arg) {
    pthread_mutex_lock(&m1);
    burn(205810);
    pthread_mutex_unlock(&m1);
    result_18 += 19;
    return NULL;
}

void *task_6(void *arg) {
    burn(1965096);
    result_17 += 18;
    return NULL;
}

void *task_1(void *arg) {
    burn(1258024);
    result_1 += 2;
    pthread_create(&h3, NULL, task_3, NULL);
    pthread_create(&h4, NULL, task_4, NULL);
    pthread_create(&h5, NULL, task_5, NULL);
    pthread_create(&h6, NULL, task_6, NULL);
    pthread_create(&h7, NULL, task_7, NULL);
    return NULL;
}

void *task_2(void *arg) {
    burn(611904);
    result_2 += 3;
    if (choose_outer == 0) {
        burn(1396218);
        result_5 += 6;
        if (choose_inner == 0) {
            burn(972146);
            result_11 += 12;
        } else {
            burn(406392);
            result_12 += 13;
        }
    } else {
        burn(510462);
        result_6 += 7;
        if (choose_inner == 0) {
            burn(1240676);
            result_13 += 14;
        } else {
            burn(1088406);
            result_14 += 15;
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    pthread_t hx, hp;
    choose_outer = (argc - 1) & 1;
    choose_inner = ((argc - 1) >> 1) & 1;
    choose_left = ((argc - 1) >> 2) & 1;
    choose_right = ((argc - 1) >> 3) & 1;
    burn(1393508);
    result_0 += 1;
    pthread_create(&hp, NULL, task_1, NULL);
    pthread_join(hp, NULL);
    pthread_create(&hx, NULL, task_2, NULL);
    pthread_join(hx, NULL);
    pthread_join(h3, NULL);
    pthread_join(h4, NULL);
    pthread_join(h5, NULL);
    pthread_join(h6, NULL);
    pthread_join(h7, NULL);
    burn(1965096);
    result_15 += 16;
    printf("RESULT:%ld\n", result_0 + result_1 + result_2 + result_3 + result_4 + result_5 + result_6 + result_7 + result_8 + result_9 + result_10 + result_11 + result_12 + result_13 + result_14 + result_15 + result_16 + result_17 + result_18);
    return 0;
}
