#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
int mode;
long result_main;
long result_task1;
long result_task2;
long result_then;
long result_else;
long result_task3;
long result_task4;
long result_task5;
long result_task6;
long result_task7;
extern void burn(unsigned);

void *task1(void *arg) {
    burn(800000);
    result_task1 += 1;
    return NULL;
}
void *task2_branch(void *arg) {
    burn(1200000);
    result_task2 += 2;
    if (mode == 0) {
        burn(1600000);
        result_then += 5;
    } else {
        burn(800000);
        result_else += 6;
    }
    return NULL;
}
void *task3(void *arg) {
    burn(1600000);
    result_task3 += 3;
    return NULL;
}
void *task4(void *arg) {
    burn(1600000);
    result_task4 += 4;
    return NULL;
}

void *task5(void *arg) {
    burn(800000);
    result_task5 += 7;
    return NULL;
}

void *task6(void *arg) {
    burn(1600000);
    result_task6 += 8;
    return NULL;
}
void *task7(void *arg) {
    burn(1600000);
    result_task7 += 9;
    return NULL;
}

int main(int argc, char **argv) {
    pthread_t t1, t2, t3, t4, t5;
    pthread_t t6, t7;
    mode = argc - 1;
    burn(800000);
    result_main += 100;
    pthread_create(&t1, NULL, task1, NULL);
    pthread_create(&t2, NULL, task2_branch, NULL);
    pthread_create(&t5, NULL, task5, NULL);
    pthread_join(t1, NULL);
    pthread_create(&t3, NULL, task3, NULL);
    pthread_create(&t4, NULL, task4, NULL);
    pthread_create(&t6, NULL, task6, NULL);
    pthread_create(&t7, NULL, task7, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);
    pthread_join(t5, NULL);
    pthread_join(t6, NULL);
    pthread_join(t7, NULL);
    burn(200000);
    result_main += 1000;
    printf("RESULT:%ld\n", result_main + result_task1 + result_task2 + result_then + result_else + result_task3 + result_task4 + result_task5 + result_task6 + result_task7);
    return 0;
}
