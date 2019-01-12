#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <mlfs/mlfs_interface.h>
#define BLOCK_SIZE 81920
#define MAX_REPLAY 8192000
int fd;
int replay_log[MAX_REPLAY];
int replay_size = 0;
int total_size;
int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage %s file_path r/w/rw\n", argv[0]);
        exit(0);
    }
    printf("waiting\n");
    fflush(stdout);
    sleep(3);
    if ((strcmp(argv[2], "w") == 0) || (strcmp(argv[2], "rw") == 0)) {
        fd = open(argv[1], O_CREAT | O_TRUNC | O_RDWR, 0666);
        if (fd == -1) {
            perror("open failed");
            exit(-1);
        }
        unsigned char *write_buf = malloc(BLOCK_SIZE);
        unsigned int size;
        unsigned int offset = 0;
        printf("running\n");
        while (scanf("%d", &size) != EOF) {
            replay_log[replay_size++] = size;
            if (size > BLOCK_SIZE) {
                printf("size %d too large\n", size);
                exit(-1);
            }
            for (unsigned int i=0; i < size; ++i) {
                write_buf[i] = (offset + i)%(256U);
            }
            write(fd, write_buf, size);
            offset += size;
        }
        struct stat file_stat;
        fstat(fd, &file_stat);
        close(fd);
        fsync(fd);
        total_size = offset;
        printf("write done, to fd %d, total %d, st_size %d\n", fd, total_size, file_stat.st_size);
        fflush(stdout);
    }
    sleep(4);
    unsigned char *read_buf = malloc(BLOCK_SIZE);
    if ((strcmp(argv[2], "r") == 0) || (strcmp(argv[2], "rw") == 0)) {
        if (strcmp(argv[2], "r") == 0) {
            int size;
            replay_size = 0;
            while(scanf("%d", &size) != EOF) {
                replay_log[replay_size++] = size;
                if (size > BLOCK_SIZE) {
                    printf("size %d too large\n", size);
                    exit(-1);
                }
                total_size += size;
            }
        }
        fd = open(argv[1], O_RDONLY, 0666);
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
    return 0;
}
