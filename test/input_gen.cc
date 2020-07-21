/*
    Generate key value pairs to test.in.
    Used to test key value store.
    keys are 64-bit number converted to string
    values are concatenation of 64-bit number in string. can be bigger than 4096 bytes. less than ~20 * 236 bytes
*/

#include <cstdio>
#include <cstdint>
// #include <string>
// #include <iostream>
// #include <typeinfo>
#include <random>

using namespace std;

int main(int argc, char **argv){
    if (argc != 2){
        printf("Usage: please only specify where to write test input data.");
        exit(0);
    }
    mt19937_64 ran;
    FILE * fd = fopen(argv[1], "w");
    uint64_t c;
    for (int i=0; i < 100; i++){
        c = ran();
        fprintf(fd, "%lu ", c); 
        int value_len = c & (1<<8)-1;
        for (int i = 0; i < value_len; i++){
            c = ran();
            fprintf(fd, "%lu", c);
        }
        fprintf(fd, "\n");
    } 
    fclose(fd);
    return 0;
}