#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>
#include <getopt.h>
#include <string.h>
#define CLFLUSH(addr)    asm volatile ("clflush (%0)\n" :: "r"(addr));
#define SFENSE    asm volatile ("sfence\n" : : );

#define OPTSTRING "i:o:s:h"
#define OPTNUM 2
#define MAX_PATH_LEN 256
#define DAX_PREFIX "/dev/dax"
void print_help(char **argv) {
    printf("usage: %s [-s insize] -i path1 -o path2\n", argv[0]);
}
uint32_t get_unit(char c) {
    switch (c) {
        case 'k':
        case 'K':
            return 1024;
        case 'm':
        case 'M':
            return 1024 * 1024;
        case 'g':
        case 'G':
            return 1024 * 1024 * 1024;
        default:
            return 1;
    }
}
int main(int argc, char **argv) {
    int c;
    srand(time(NULL));
    char inpath[MAX_PATH_LEN];
    char outpath[MAX_PATH_LEN];
    int optnum = 0;
    size_t insize = 0;
    while ((c = getopt(argc, argv, OPTSTRING)) != -1) {
        switch (c) {
            case 'i': // device path
                strncpy(inpath, optarg, MAX_PATH_LEN);
                optnum++;
                break;
            case 'o': // io_size
                strncpy(outpath, optarg, MAX_PATH_LEN);
                optnum++;
                break;
            case 's':
                insize = atol(optarg);
                break;
        }
    }
    if (optnum < OPTNUM) {
        print_help(argv);
        exit(-1);
    }
    int infd, outfd;
    infd = open(inpath, O_RDWR);
    if (infd == -1) {
        perror("infile open failed");
        exit(-1);
    }
    outfd = open(outpath, O_RDWR | O_CREAT, 0644);
    if (outfd == -1) {
        perror("outfile open failed");
        exit(-1);
    }
    if (insize == 0) { // insize isn't initialized through cmd arg
        if (memcmp(inpath, DAX_PREFIX, strlen(DAX_PREFIX)) == 0) { // prefix with /dev/daxX.X
            int region, namespace;
            sscanf(inpath, "/dev/dax%d.%d", &region, &namespace);
            char sysfs_path[MAX_PATH_LEN];
            snprintf(sysfs_path, MAX_PATH_LEN, "/sys/class/dax/dax%1d.%1d/size", region, namespace);
            FILE *sys_size_file = fopen(sysfs_path, "r");
            if (sys_size_file == NULL) {
                perror("Open sys size file failed");
                exit(-1);
            }
            fscanf(sys_size_file, "%lu", &insize);
            fclose(sys_size_file);
        }
        else { // a normal file, use fstat
            struct stat fst;
            if (fstat(infd, &fst) != 0) {
                perror("fstat failed");
                exit(-1);
            }
            insize = fst.st_size;
        }
    }
    if (memcmp(outpath, DAX_PREFIX, strlen(DAX_PREFIX)) == 0) { // outfile is a dax device
        ;
    }
    else { // normal file
        if (ftruncate(outfd, insize) != 0) {
            perror("ftruncate outfile failed");
            exit(-1);
        }
    }
    uint8_t *inbase, *outbase;
    printf("copy %lu bytes from %s to %s\n", insize, inpath, outpath);
    inbase = (uint8_t*)mmap(NULL, insize, PROT_READ | PROT_WRITE, MAP_SHARED, infd, 0);
    if (inbase == MAP_FAILED) {
        perror("infile mmap failed");
        exit(-1);
    }
    outbase = (uint8_t*)mmap(NULL, insize, PROT_READ | PROT_WRITE, MAP_SHARED, outfd, 0);
    if (outbase == MAP_FAILED) {
        perror("outfile mmap failed");
        exit(-1);
    }
    memcpy(outbase, inbase, insize);
    msync(outbase, insize, MS_SYNC);
    munmap(inbase, insize);
    munmap(outbase, insize);
    close(infd);
    close(outfd);
    return 0;
}
