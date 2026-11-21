#include <stdio.h>
static int result;
int main(void) {
    for (int i = 0; i < 2; i++) {
        printf("EV:L2a:i:%d\n", i);
        result += 1;
    }
    for (int j = 0; j < 2; j++) {
        printf("EV:L2b:j:%d\n", j);
        result += 2;
    }
    printf("RESULT:%d\n", result);
    return 0;
}
