#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "mlfs/mlfs_interface.h"
#include "global/util.h"
#include "nested_dir.h"
#define MAX_FILE_NUM 16384
#define MAX_FILE_NAME_LEN 1024
#define MAX_THREADS 1024
static size_t block_size;
static uint32_t n_threads;
static double seq_ratio;
static size_t read_total;
static int opt_num;
static uint32_t file_num;
static int fd_v[MAX_FILE_NUM];
static char filename_v[MAX_FILE_NUM][MAX_FILE_NAME_LEN];
static pthread_t threads[MAX_THREADS];
static bool full_random = false;
typedef struct {
    int tid;
    size_t total_seq_read;
    size_t total_rand_read;
} worker_result_t;
static worker_result_t worker_results[MAX_THREADS];

static void *worker_thread(void *);
static void *worker_thread_rand(void *);

static bool flush_cache = false;
static bool pre_read = false;
char prebuf[4096];
void *cbuf = NULL;

void flush_llc(void) {
    size_t repeats = 1;
    size_t mult = 1;
    size_t allocation_size = mult * 32 * 1024 * 1024;

    if (!cbuf) {
        cbuf = malloc(allocation_size);
        if (!cbuf) panic("OOM!");
    }

    const size_t cache_line = 64;
    register char *cp = (char *)cbuf;
    register size_t i = 0;

    //memset(buf, 42, allocation_size);

    //asm volatile("sfence\n\t"
    //             :
    //             :
    //             : "memory"); 

    for (i = 0; i < allocation_size; i += cache_line) {
        asm volatile("clflush (%0)\n\t"
                     : 
                     : "r"(&cp[i])
                     : "memory");
    }

    asm volatile("sfence\n\t"
                 :
                 :
                 : "memory"); 
}

#ifndef PREFIX
#define PREFIX "/mlfs"
#endif
#define OPTSTRING "b:j:s:r:n:h:xfp"
#define OPTNUM 5
void print_help(char **argv) {
    printf("usage: %s -b block_size -j n_threads -s seq_ratio -n num_files -r read_total -x (means full random)\n", argv[0]);
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
    unsigned t = time(NULL);
    //unsigned t = 1568842819;
    printf("Random seed is %u\n", t);
    srand(t);
    while ((c = getopt(argc, argv, OPTSTRING)) != -1) {
        switch (c) {
            case 'b': // block_size
                //block_size = atoi(optarg);
                sscanf(optarg, "%zu", &block_size);
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
                //read_total = atoi(optarg);
                sscanf(optarg, "%zu", &read_total);
                read_total *= get_unit(optarg[strlen(optarg)-1]);
                assert((read_total % block_size == 0) && "read_total should be dividable by block_size");
                opt_num++;
                break;
            case 'n': // how many files are operated concurrently
                file_num = atoi(optarg);
                if (file_num > MAX_FILE_NUM) {
                    panic("too many files");
                }
                opt_num++;
                break;
            case 'h':
                print_help(argv);
                exit(0);
                break;
            case 'x':
                full_random = true;
                opt_num++;
                break;
            case 'f':
                flush_cache = true;
                opt_num ++;
                break;
            case 'p':
                pre_read = true;
                opt_num ++;
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
    show_libfs_stats("init done");
    reset_libfs_stats();
    //uint64_t tsc_begin = asm_rdtscp();

    open_many_files(fd_v, filename_v, file_num, PREFIX, O_RDWR);
    if (pre_read) {
        for (int f = 0; f < file_num; ++f) {
            struct stat s;
            int serr = fstat(fd_v[f], &s);
            if (serr) {
                perror("stat failed");
                exit(-1);
            }

            (void)lseek(fd_v[f], 0, SEEK_SET);
            for (size_t sz = 0; sz < s.st_size; sz += UINT64_C(4096)) {
                (void)read(fd_v[f], prebuf, 
                        4096LLU < s.st_size - sz ? 4096 : s.st_size - sz);
            }
            (void)lseek(fd_v[f], 0, SEEK_SET);
        }
        show_libfs_stats("preread done");
        reset_libfs_stats();
    }


    struct timeval start, end;
    int terr = gettimeofday(&start, NULL);
    if (terr) panic("GETTIMEOFDAY\n");

    for (int i=0; i < n_threads; ++i) {
        worker_results[i].tid = i;
        assert(pthread_create(&threads[i], NULL, 
                    full_random ? worker_thread_rand : worker_thread, 
                    (void*)(&worker_results[i])) == 0);
    }
    for (int i=0; i < n_threads; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
    }
    //uint64_t readtime = asm_rdtscp() - tsc_begin;
    //printf("elapsed time %lu\n", readtime);
    terr = gettimeofday(&end, NULL);
    if (terr) panic("GETTIMEOFDAY\n");
    double secs = ((double)(end.tv_sec - start.tv_sec)) + 
                   ((double)(end.tv_usec - start.tv_usec) * 1e-6);
    printf("elapsed time: %f\n", secs);

    for (int i=0; i < file_num; ++i) {
        close(fd_v[i]);
    }

    for (int i=0; i < n_threads; ++i) {
        printf("thread %d : read %lu (seq %lu rand %lu)\n", i,
                worker_results[i].total_seq_read + worker_results[i].total_rand_read,
                worker_results[i].total_seq_read, worker_results[i].total_rand_read);
        printf("thread %d : throughput = %.1f MB/s\n",
                ((double)(worker_results[i].total_seq_read + worker_results[i].total_rand_read) / secs)
                / (1024.0 * 1024.0));
    }
    return 0;
}

static void *worker_thread(void *arg) {
    worker_result_t *r = (worker_result_t *)(arg);
    struct stat file_stat;
    void *read_buf = malloc(block_size);
    int curr_file = 0;
    while (1) {
        int fnum = curr_file++ % file_num;
        int fd = fd_v[fnum];
        assert(fstat(fd, &file_stat) == 0 && "fstat failed");
        size_t size = file_stat.st_size;
        if (size <= 0) {
            fprintf(stderr, "file %s (fd %d) has size %lu!\n", 
                    filename_v[fnum], fd, size);
        }
        assert(size > 0 && "can't read from a file of size 0!!");
        size_t block_num = size/block_size;
        if (r->total_seq_read + r->total_rand_read >= read_total) {
            // exceeds max file size
            break;
        }

        if ((double)(rand())/RAND_MAX < seq_ratio) { // sequential read
            off_t offset = lseek(fd, 0, SEEK_CUR);
            assert(offset != -1 && "lseek failed");
            off_t next_offset = block_size > 4096 ? offset + block_size : offset + 4096;
            offset = next_offset < size ? offset : 0;

            ssize_t rs = pread(fd, read_buf, block_size, offset);
            assert(rs != -1 && "sequential read failed");
            assert(rs > 0 && "sequential read was 0!");
            r->total_seq_read += rs;

            offset = lseek(fd, next_offset, SEEK_SET);
            assert(offset != -1 && "post-seq read lseek failed");
        } else { // random read
            int rand_offset = (rand()%(block_num)) * block_size;
            ssize_t rs = pread(fd, read_buf, block_size, rand_offset);
            assert(rs != -1 && "random read failed");
            r->total_rand_read += rs;

            off_t offset = lseek(fd, block_size, SEEK_CUR);
            assert(offset != -1 && "lseek failed");
        }

        if (flush_cache) flush_llc();
    }
    return NULL;
}

static void *worker_thread_rand(void *arg) {
    worker_result_t *r = (worker_result_t *)(arg);
    struct stat file_stat;
    void *read_buf = malloc(block_size);
    int curr_file = 0;
    while (1) {
        int fd = fd_v[curr_file++ % file_num];
        assert(fstat(fd, &file_stat) == 0 && "fstat failed");
        size_t size = file_stat.st_size;
        size_t block_num = size/block_size;
        if (r->total_seq_read + r->total_rand_read >= read_total) // exceeds max file size
            break;

        int rand_offset = (rand()%(block_num)) * block_size;
        ssize_t rs = pread(fd, read_buf, block_size, rand_offset);
        assert(rs != -1 && "random read failed");
        r->total_rand_read += rs;

        if (flush_cache) flush_llc();
    }
    return NULL;
}
