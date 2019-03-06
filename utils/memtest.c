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
#define CLFLUSH(addr)    asm volatile ("clflush (%0)\n" :: "r"(addr));
#define SFENSE    asm volatile ("sfence\n" : : );

#define OPTSTRING "d:s:b:h"
#define OPTNUM 3
#define MAX_PATH_LEN 256
void print_help(char **argv) {
    printf("usage: %s -d /dev/daxX.X -s size -b io_size\n", argv[0]);
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
static inline unsigned long long asm_rdtscp(void)
{
	unsigned hi, lo;
	__asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"rcx");
	return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
void mem_test(uint8_t *base, size_t length, size_t io_size) {
    uint8_t *buf = (uint8_t*)malloc(io_size);
    for (size_t rep = 0; rep < 2; rep++) {
        uint64_t tsc = 0;
        size_t op = 0;
        for (uint8_t *addr = base; addr < base + length; addr += io_size) {
            uint64_t tsc_begin = asm_rdtscp();
            memmove(buf, addr, io_size);
            tsc += asm_rdtscp() - tsc_begin;
            op++;
        }
        printf("[%ld]read seq io_size %lu, bytes/tsc %lu / %lu (%f) tsc/op %f\n", rep, io_size, length, tsc, (double)length/tsc, (double)tsc/op);
    }
    for (size_t rep = 0; rep < 2; rep++) {
        uint64_t tsc = 0;
        size_t cnt = 0;
        size_t op = 0;
        for (uint8_t *addr_r = base + length - io_size, *addr_l = base; addr_l < addr_r; addr_r -= io_size, addr_l += io_size) {
            uint64_t tsc_begin = asm_rdtscp();
            memmove(buf, addr_l, io_size);
            memmove(buf, addr_r, io_size);
            tsc += asm_rdtscp() - tsc_begin;
            cnt += io_size;
            cnt += io_size;
            op += 2;
        }
        printf("[%ld]read reverse io_size %lu, bytes/tsc %lu / %lu (%f) tsc/op %f\n", rep, io_size, cnt, tsc, (double)cnt/tsc, (double)tsc/op);
    }
    {
        uint64_t tsc = 0;
        size_t op = 0;
        for (size_t i=0; i < length; i += io_size) {
            int r = rand() % (length/io_size);
            uint8_t *addr = base + r * io_size;
            uint64_t tsc_begin = asm_rdtscp();
            memmove(buf, addr, io_size);
            tsc += asm_rdtscp() - tsc_begin;
            op++;
        }
        printf("read random io_size %lu, bytes/tsc %lu / %lu (%f) tsc/op %f\n", io_size, length, tsc, (double)length/tsc, (double)tsc/op);
    }
    free(buf);
}
int main(int argc, char **argv) {
    int c;
    srand(time(NULL));
    char devpath[MAX_PATH_LEN];
    int optnum = 0;
    size_t length = 0;
    size_t io_size = 0;
    while ((c = getopt(argc, argv, OPTSTRING)) != -1) {
        switch (c) {
            case 'd': // device path
                strncpy(devpath, optarg, MAX_PATH_LEN);
                optnum++;
                break;
            case 's': // length
                length = atol(optarg);
                printf("length=%lu optarg %s\n", length, optarg);
                length *= get_unit(optarg[strlen(optarg)-1]);
                optnum++;
                break;
            case 'b': // io_size
                io_size = atol(optarg);
                io_size *= get_unit(optarg[strlen(optarg)-1]);
                optnum++;
                break;
        }
    }
    if (optnum < OPTNUM) {
        print_help(argv);
        exit(-1);
    }
    uint8_t *base;
    if (strcmp(devpath, "mem") == 0) {
        base = (uint8_t*)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        printf("use anonymous mmap\n");
    }
    else{
        int daxfd = open(devpath, O_RDWR);
        if (daxfd < 0) {
            perror("open dax failed");
            exit(-1);
        }
        printf("open %s, length %lu\n", devpath, length);
        base = (uint8_t*)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, daxfd, 0);
        if (base == MAP_FAILED) {
            perror("mmap dax failed");
            exit(-1);
        }
    }
    mem_test(base, length, io_size);
    return 0;
}
