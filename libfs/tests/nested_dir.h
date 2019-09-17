#ifndef NESTED_DIR_H
#define NESTED_DIR_H
#define BRANCH_FACTOR 30
#ifndef MAX_FILE_NAME_LEN
#define MAX_FILE_NAME_LEN 1024
#endif
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
/// prefix should not have a taling '/'
/// filename_v could be NULL
/// fd_v could be NULL, just for robust, do not use like that for now.
void open_many_files(int *fd_v, char (*filename_v)[MAX_FILE_NAME_LEN], int file_num, const char *prefix, int oflag) {
    if (file_num <= BRANCH_FACTOR) {
        char buf[MAX_FILE_NAME_LEN];
        for (int i=0; i < BRANCH_FACTOR; ++i) {
            snprintf(buf, MAX_FILE_NAME_LEN, "%s/MTCC-leaf-%03d", prefix, i);
            if (fd_v) {
                fd_v[i] = open(buf, oflag);
                if (fd_v[i] == -1) {
                    fprintf(stderr, "failed to open %s: %s\n", buf, strerror(errno));
                    exit(-1);
                }
            }
            if (filename_v) {
                strncpy(filename_v[i], buf, MAX_FILE_NAME_LEN);
            }
        }
    }
    else { //recursive
        // split number of files for each subdir
        int base_split = file_num / BRANCH_FACTOR;
        int remain_split = file_num % BRANCH_FACTOR;
        int chunk_size;
        char buf[MAX_FILE_NAME_LEN];

        // (chunk_size = base_split + 1) x remain_split
        // then
        // (chunk_size = base_split) x (BRANCH_FACTOR - remain_split)
        for (int i=0; i < BRANCH_FACTOR; ++i) {
            if (i < remain_split) {
                chunk_size = base_split + 1;
            }
            else {
                chunk_size = base_split;
            }
            snprintf(buf, MAX_FILE_NAME_LEN, "%s/MTCC-node-%03d", prefix, i);
            // make next level dir
            int ret = mkdir(buf, 0755);
            if (ret != 0 && errno != EEXIST) {
                perror("can not mkdir, not EEXIST");
                exit(-1);
            }
            open_many_files(fd_v, filename_v, chunk_size, buf, oflag);
            if (fd_v) {
                fd_v += chunk_size;
            }
            if (filename_v) {
                filename_v += chunk_size;
            }
        }

    }
}
#endif
