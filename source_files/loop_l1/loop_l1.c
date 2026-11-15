#include <stdio.h>
volatile int result;
int main(void) {
    result = 1;
    for (int i = 0; i < 3; i++) {
        result += i + 1;
        result += 2;
    }
    result += 10;
    printf("result=%d\n", result);
    return 0;
}
