int main(void) {
    int result = 1;
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < 3; j++) {
            result += i + j;
        }
    }
    result += 10;
    return 0;
}
