#include <stdio.h>
static int result;
int main(void) {
    for (int i = 0; i < 0; i++) {
        printf("EV:L1:i:%d\n", i);
        result += 1;
    }
    printf("RESULT:%d\n", result);
    return 0;
}
