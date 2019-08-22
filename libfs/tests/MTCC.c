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
#define MAX_FILE_NUM 1024
#define MAX_FILE_NAME_LEN 1024
#define MAX_THREADS 1024
static size_t block_size;
static uint32_t n_threads;
static double seq_ratio;
static uint32_t file_num;
static size_t max_file_size;
static size_t start_file_size;
static size_t read_unit;
static size_t write_unit;
static int opt_num;
static int fd_v[MAX_FILE_NUM];
static char filename_v[MAX_FILE_NUM][MAX_FILE_NAME_LEN];
static pthread_t threads[MAX_THREADS];
typedef struct {
    size_t total_seq_read;
    size_t total_rand_read;
    size_t total_write;
} worker_result_t;
static worker_result_t worker_results[MAX_THREADS];

static inline int panic(char *str) {
    fprintf(stderr, "%s", str);
    exit(-1);
}
static uint64_t *write_buf;
static void *worker_thread(void *);

#ifndef PREFIX
#define PREFIX "/mlfs"
#endif
#define OPTSTRING "b:j:n:s:M:r:w:h:S"
#define OPTNUM 7
void print_help(char **argv) {
    printf("usage: %s -b block_size -j n_threads -s seq_ratio -n num_file -M max_file_size -S start_file_size -w write_unit_size -r read_unit_size\n", argv[0]);
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
    //getchar();
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
            case 'n': // how many files are operated concurrently
                file_num = atoi(optarg);
                if (file_num > MAX_FILE_NUM) {
                    panic("too many files");
                }
                opt_num++;
                break;
            case 'M': // max file size
                max_file_size = atoi(optarg);
                max_file_size *= get_unit(optarg[strlen(optarg)-1]);
                assert((max_file_size % block_size == 0) && "max_file_size should be dividable by block_size");
                opt_num++;
                break;
            case 'S': // start file size
                max_file_size = atoi(optarg);
                max_file_size *= get_unit(optarg[strlen(optarg)-1]);
                assert((max_file_size % block_size == 0) && "max_file_size should be dividable by block_size");
                opt_num++;
                break;
            case 'r': // read_unit size
                read_unit = atoi(optarg);
                read_unit *= get_unit(optarg[strlen(optarg)-1]);
                assert((read_unit % block_size == 0) && "read_unit should be dividable by block_size");
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
    for (int i=0; i < file_num; ++i) {
        snprintf(filename_v[i], MAX_FILE_NAME_LEN, PREFIX "/MTCC-%d", i);
        fd_v[i] = open(filename_v[i], O_RDWR | O_CREAT, 0666);
        if (fd_v[i] == -1) {
            perror("open failed");
            exit(-1);
        }
        // init each file with read_unit data or start size, whichever is larger
        size_t sz = read_unit > start_file_size ? read_unit : start_file_size;
        for (size_t s = 0; s < sz; s += write_unit) {
            assert(write(fd_v[i], write_buf, write_unit) != -1);
        }
    }

    for (int i=0; i < n_threads; ++i) {
        assert(pthread_create(&threads[i], NULL, worker_thread, (void*)(&worker_results[i])) == 0);
    }

    for (int i=0; i < n_threads; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
    }

    for (int i=0; i < file_num; ++i) {
        struct stat file_stat;
        fstat(fd_v[i], &file_stat);
        printf("%s size is %lu\n", filename_v[i], file_stat.st_size);
        close(fd_v[i]);
    }
    
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
    while (1) {
        int fd = fd_v[rand()%file_num];
        assert(fstat(fd, &file_stat) == 0 && "fstat failed");
        size_t size = file_stat.st_size;
        size_t block_num = size/block_size;
        if (size > max_file_size) // exceeds max file size
            break;
        if ((double)(rand())/RAND_MAX < seq_ratio) { // sequential read
            int start_offset = (rand()%(block_num - read_unit/block_size + 1)) * block_size;
            for (int i=start_offset; i < start_offset + read_unit; i += block_size) {
                ssize_t rs = pread(fd, read_buf, block_size, i);
                assert(rs != -1 && "sequential read failed");
                r->total_seq_read += rs;
            }
        }
        else { // random read
            for (int i=0; i < read_unit; i += block_size) {
                int rand_offset = (rand()%(block_num)) * block_size;
                ssize_t rs = pread(fd, read_buf, block_size, rand_offset);
                assert(rs != -1 && "random read failed");
                r->total_rand_read += rs;
            }
        }
        lseek(fd, 0, SEEK_END);
        assert(write(fd, write_buf, write_unit) != -1);
        r->total_write += write_unit;
    }
    return NULL;
}
