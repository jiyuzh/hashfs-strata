#define _GNU_SOURCE
#include <sys/stat.h>
#include <sys/syscall.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <mlfs/mlfs_interface.h>
#define BLOCK_SIZE 81920
#define THREADS 5
int *replay_log = NULL;
int replay_size = 0;
int total_size = 0;
void *worker_thread(void*);
int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage %s file_path r/w/rw\n", argv[0]);
        exit(0);
    }
    printf("waiting\n");
    fflush(stdout);
    sleep(3);

    scanf("%d", &replay_size);
    replay_log = malloc(sizeof(int)*replay_size);
    int size;
    int ri=0;
    while (scanf("%d", &size) != EOF) {
        replay_log[ri++] = size;
        if (size > BLOCK_SIZE) {
            printf("size %d too large\n", size);
            exit(-1);
        }
        total_size += size;
    }
    pthread_t threads[THREADS];
    for (int i=0; i < THREADS; ++i) {
        assert(pthread_create(&threads[i], NULL, worker_thread, (void*)argv) == 0);
    }
    for (int i=0; i < THREADS; ++i) {
        assert(pthread_join(threads[i], NULL) == 0);
    }
    return 0;
}
void *worker_thread(void *arg) {
    int fd;
    char **argv = (char**)arg;
    char fn[1024];
    sleep(rand()%10);
    sprintf(fn, "%s.%u", argv[1], pthread_self());
    if ((strcmp(argv[2], "w") == 0) || (strcmp(argv[2], "rw") == 0)) {
        fd = open(fn, O_CREAT | O_TRUNC | O_RDWR, 0666);
        if (fd == -1) {
            perror("open failed");
            exit(-1);
        }
        unsigned char *write_buf = malloc(BLOCK_SIZE);
        unsigned int offset = 0;
        printf("running\n");
        for (int ri=0; ri < replay_size; ++ri) {
            unsigned int size = replay_log[ri];
            for (unsigned int i=0; i < size; ++i) {
                write_buf[i] = (offset + i)%(256U);
            }
            write(fd, write_buf, size);
            offset += size;
        }
        assert(offset == total_size);
        struct stat file_stat;
        fstat(fd, &file_stat);
        close(fd);
        fsync(fd);
        free(write_buf);
        printf("write done, to fd %d(%s), total %d, st_size %d\n", fd, fn, total_size, file_stat.st_size);
        fflush(stdout);
    }
    sleep(2);
    unsigned char *read_buf = malloc(BLOCK_SIZE);
    if ((strcmp(argv[2], "r") == 0) || (strcmp(argv[2], "rw") == 0)) {
        fd = open(fn, O_RDONLY, 0666);
        struct stat file_stat;
        fstat(fd, &file_stat);
        printf("open st_size %d\n", file_stat.st_size);
        if (fd == -1) {
            perror("open failed\n");
            exit(-1);
        }
        int offset = 0;
        for (unsigned int i=0; i < replay_size; ++i) {
            pread(fd, read_buf, replay_log[i], offset);
            for (unsigned int j=0; j < replay_log[i]; ++j) {
                if (read_buf[j] != (offset + j)%(256U)) {
                    printf("at offset %lu, should be %d, is %d\n", offset + j, (offset+j)%256, read_buf[j]);
                    exit(-1);
                }
            }
            offset += replay_log[i];
            printf("pass size +%d (%d/%d)\n", replay_log[i], offset, total_size);
        }
        close(fd);
    }
    unlink(fn);
    return NULL;
}
