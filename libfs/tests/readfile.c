#include <stdio.h>
#include <pthread.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "mlfs/mlfs_interface.h"
#include "global/util.h"
#define MAX_FILE_NUM 1024
#define MAX_FILE_NAME_LEN 1024
#define MAX_THREADS 1024
static size_t block_size;
static uint32_t n_threads;
static double seq_ratio;
static size_t read_total;
static int opt_num;
static int fd;
static char filename[MAX_FILE_NAME_LEN];
static pthread_t threads[MAX_THREADS];
typedef struct {
    size_t total_seq_read;
    size_t total_rand_read;
} worker_result_t;
static worker_result_t worker_results[MAX_THREADS];

static void *worker_thread(void *);

#ifndef PREFIX
#define PREFIX "/mlfs"
#endif
#define OPTSTRING "b:j:s:r:f:h"
#define OPTNUM 5
void print_help(char **argv) {
    printf("usage: %s -b block_size -j n_threads -s seq_ratio -f file_path -r read_total\n", argv[0]);
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
                seq_ratio = atof(optarg);
                assert(seq_ratio >= 0 && seq_ratio <= 1 && "seq ratio should in 0.0~1.0");
                opt_num++;
                break;
            case 'r': // read_total size
                read_total = atoi(optarg);
                read_total *= get_unit(optarg[strlen(optarg)-1]);
                assert((read_total % block_size == 0) && "read_total should be dividable by block_size");
                opt_num++;
                break;
            case 'f': // file_path
                strcpy(filename, optarg);
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
    fd = open(filename, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "can't find %s try to create it\n", filename);
        fd = open(filename, O_CREAT | O_RDWR, 0644);
        if (fd == -1) {
            perror("cannot create file");
            exit(-1);
        }
        void *read_buf = malloc(block_size);
        for (size_t i=0; i < read_total; i += block_size) {
            write(fd, read_buf, block_size);
        }
        free(read_buf);
        lseek(fd, 0, SEEK_SET);
    }
    show_libfs_stats("init done");
    reset_libfs_stats();
    uint64_t tsc_begin = asm_rdtscp();
    for (int i=0; i < n_threads; ++i) {
        assert(pthread_create(&threads[i], NULL, worker_thread, (void*)(&worker_results[i])) == 0);
    }
    for (int i=0; i < n_threads; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
    }
    uint64_t readtime = asm_rdtscp() - tsc_begin;
    printf("elapsed time %lu\n", readtime);
    close(fd);
    for (int i=0; i < n_threads; ++i) {
        printf("thread %d : read %lu (seq %lu rand %lu)\n", i,
                worker_results[i].total_seq_read + worker_results[i].total_rand_read,
                worker_results[i].total_seq_read, worker_results[i].total_rand_read);
    }
    return 0;
}

static void *worker_thread(void *arg) {
    worker_result_t *r = (worker_result_t *)(arg);
    struct stat file_stat;
    void *read_buf = malloc(block_size);
    while (1) {
        assert(fstat(fd, &file_stat) == 0 && "fstat failed");
        size_t size = file_stat.st_size;
        size_t block_num = size/block_size;
        if (r->total_seq_read + r->total_rand_read > read_total) // exceeds max file size
            break;
        if ((double)(rand())/RAND_MAX < seq_ratio) { // sequential read
            for (int i=0; i < size; i += block_size) {
                ssize_t rs = pread(fd, read_buf, block_size, i);
                assert(rs != -1 && "sequential read failed");
                r->total_seq_read += rs;
            }
        }
        else { // random read
            for (int i=0; i < size; i += block_size) {
                int rand_offset = (rand()%(block_num)) * block_size;
                ssize_t rs = pread(fd, read_buf, block_size, rand_offset);
                assert(rs != -1 && "random read failed");
                r->total_rand_read += rs;
            }
        }
    }
    return NULL;
}
