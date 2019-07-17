#include <stdio.h>
#include <pthread.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#define MAX_FILE_NAME_LEN 1024
#define MAX_THREADS 1024
static size_t block_size;
static uint32_t n_threads;
static size_t max_file_size;
static size_t write_unit;
static int opt_num;
static int fd_v;
static char filename_v[MAX_FILE_NAME_LEN];
static pthread_t threads[MAX_THREADS];
typedef struct {
    size_t total_seq_read;
    size_t total_rand_read;
    size_t total_write;
} worker_result_t;
static worker_result_t worker_results[MAX_THREADS];

static inline int panic(char *str) {
    fprintf(stderr, str);
    exit(-1);
}
static uint64_t *write_buf;
static void *worker_thread(void *);

#ifndef PREFIX
#define PREFIX "/mlfs"
#endif
#define OPTSTRING "b:j:M:w:h"
#define OPTNUM 4
void print_help(char **argv) {
    printf("usage: %s -b block_size -j n_threads -M max_file_size -w write_unit_size\n", argv[0]);
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
    getchar();
    while ((c = getopt(argc, argv, OPTSTRING)) != -1) {
        switch (c) {
            case 'b': // block_size
                block_size = atoi(optarg);
                block_size *= get_unit(optarg[strlen(optarg)-1]);
                if (block_size % sizeof(uint64_t)) {
                    panic("block_size should be dividable by 64");
                }
                opt_num++;
                break;
            case 'j': // number of threads
                n_threads = atoi(optarg);
                assert(n_threads <= MAX_THREADS && "too many threads");
                opt_num++;
                break;
            case 'M': // max file size
                max_file_size = atoi(optarg);
                max_file_size *= get_unit(optarg[strlen(optarg)-1]);
                assert((max_file_size % block_size == 0) && "max_file_size should be dividable by block_size");
                opt_num++;
                break;
            case 'w': // write_unit size
                write_unit = atoi(optarg);
                write_unit *= get_unit(optarg[strlen(optarg)-1]);
                assert((write_unit % block_size == 0) && "write unit should be dividable by block_size");
                opt_num++;
                break;
            case 'h':
                print_help(argv);
                exit(0);
                break;
            case '?':
            default:
                print_help(argv);
                panic("wrong args\n");
        }
    }
    if (opt_num < OPTNUM) {
        print_help(argv);
        panic("insufficient args\n");
    }
    write_buf = (uint64_t*)malloc(write_unit);
    for (int i=0; i < write_unit/sizeof(uint64_t); ++i) {
        write_buf[i] = i;
    }
    snprintf(filename_v, MAX_FILE_NAME_LEN, PREFIX "/MTCC-0");
    fd_v = open(filename_v, O_RDWR | O_CREAT, 0666);
    if (fd_v == -1) {
            perror("open failed");
            exit(-1);
    }

    for (int i=0; i < n_threads; ++i) {
        assert(pthread_create(&threads[i], NULL, worker_thread, (void*)(&worker_results[i])) == 0);
    }
    for (int i=0; i < n_threads; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
    }
    struct stat file_stat;
    fstat(fd_v, &file_stat);
    printf("%s size is %lu\n", filename_v, file_stat.st_size);
    close(fd_v);
    
    for (int i=0; i < n_threads; ++i) {
        printf("thread %d : read %lu (seq %lu rand %lu), write %lu\n", i,
                worker_results[i].total_seq_read + worker_results[i].total_rand_read,
                worker_results[i].total_seq_read, worker_results[i].total_rand_read,
                worker_results[i].total_write);
    }
    return 0;
}

static void *worker_thread(void *arg) {
    worker_result_t *r = (worker_result_t *)(arg);
    struct stat file_stat;
    void *read_buf = malloc(block_size);
    for(size_t i = 0; i < max_file_size; i += write_unit) {
        assert(fwrite(fd_v, write_buf, write_unit, i) != -1);
        r->total_write += write_unit;
    }
    return NULL;
}
