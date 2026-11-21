#include <stdio.h>
static int result;
int main(void) {
    for (int i = 0; i < 0; i++) {
        for (int j = 0; j < 3; j++) {
            printf("EV:loop_l3_zero:pair:%d:%d\n", i, j);
            result += 1;
        }
    }
    printf("RESULT:%d\n", result);
    return 0;
}
