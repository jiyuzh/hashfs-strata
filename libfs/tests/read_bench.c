#include <stdio.h>
#include <pthread.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#define MAX_FILE_NUM 1024
#define MAX_FILE_NAME_LEN 1024
#define MAX_THREADS 1024
static size_t block_size;
static uint32_t n_threads;
static int seq;
static size_t max_file_size;
static size_t read_unit;
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
static void *worker_thread(void *);

#ifndef PREFIX
#define PREFIX "/mlfs"
#endif
#define OPTSTRING "b:j:s:M:h"
#define OPTNUM 4
void print_help(char **argv) {
    printf("usage: %s -b block_size -j n_threads -s seq -M max_file_size\n", argv[0]);
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
            case 's': // how many portion should do sequential read (0.0~1.0)
                seq = atoi(optarg);
                opt_num++;
                break;
            case 'M': // max file size
                max_file_size = atoi(optarg);
                max_file_size *= get_unit(optarg[strlen(optarg)-1]);
                assert((max_file_size % block_size == 0) && "max_file_size should be dividable by block_size");
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
    
    snprintf(filename_v, MAX_FILE_NAME_LEN, PREFIX "/MTCC-0");
    fd_v = open(filename_v, O_RDONLY);
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
    struct timeval pre_time, post_time;
    gettimeofday(&pre_time, NULL);
    
    worker_result_t *r = (worker_result_t *)(arg);
    struct stat file_stat;
    void *read_buf = malloc(block_size);

    int fd = fd_v;
    assert(fstat(fd, &file_stat) == 0 && "fstat failed");
    size_t size = file_stat.st_size;
    size_t block_num = size/block_size;
    if (seq) { // sequential read
        for (int i=0; i < max_file_size; i += block_size) {
            ssize_t rs = pread(fd, read_buf, block_size, i);
            assert(rs != -1 && "sequential read failed");
            r->total_seq_read += rs;
        }
    }
    else { // random read
        for (int i=0; i < max_file_size; i += block_size) {
            int rand_offset = (rand()%(block_num)) * block_size;
            ssize_t rs = pread(fd, read_buf, block_size, rand_offset);
            assert(rs != -1 && "random read failed");
            r->total_rand_read += rs;
        }
    }

    gettimeofday(&post_time, NULL);
    time_t dif_sec = post_time.tv_sec - pre_time.tv_sec;
    suseconds_t dif_micro = post_time.tv_usec - pre_time.tv_usec;
    printf("Time elapsed: %lu seconds, %lu microseconds", dif_sec, dif_micro);
    
    return NULL;
}
