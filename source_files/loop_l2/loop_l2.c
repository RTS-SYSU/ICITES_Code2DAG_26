#include <stdio.h>
volatile int result;
int main(void) {
    result = 1;
    for (int i = 0; i < 2; i++) {
        result += i + 1;
    }
    result += 10;
    for (int j = 0;
         j < 3;
         j++)
    {
        result += j + 2;
    }
    result += 100;
    printf("result=%d\n", result);
    return 0;
}
