#define _GNU_SOURCE
#include <dirent.h>     /* Defines DT_* constants */
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <assert.h>
#include <string.h>

#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

struct linux_dirent {
    long           d_ino;
    off_t          d_off;
    unsigned short d_reclen;
    char           d_name[];
};
#define MAX_PATH_LEN 1024
#define BLOCK_SIZE 16384
void recursive_readdir(const char *path) {
    int fd;
    fd = open(path, O_RDONLY);
    if (fd == -1) {
        handle_error("open dir");
    }
    struct stat file_stat;
    assert(fstat(fd, &file_stat) == 0);
    if (S_ISDIR(file_stat.st_mode)) {
        int nread, n_file = 0, bpos;
        //char d_type;
        struct linux_dirent *d;
        char buf[BLOCK_SIZE];
        printf("Checking dir %s, inum %lu\n", path, file_stat.st_ino);
        for ( ; ; ) {
            nread = syscall(SYS_getdents, fd, buf, BLOCK_SIZE);
            if (nread == -1)
                handle_error("getdents");

            if (nread == 0)
                break;
            for (bpos = 0; bpos < nread;) {
                n_file++;
                d = (struct linux_dirent *) (buf + bpos);
                //d_type = *(buf + bpos + d->d_reclen - 1);
                if (strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
                    char entry_path[MAX_PATH_LEN];
                    snprintf(entry_path, MAX_PATH_LEN, "%s/%s", path, d->d_name);
                    recursive_readdir(entry_path);
                }
                bpos += d->d_reclen;
            }
        }
        printf("%d files in %s\n", n_file, path);
    }
    else {
        int read_cnt;
        void *buf = malloc(BLOCK_SIZE);
        printf("Checking file %s, size %lu, inum %lu\n", path, file_stat.st_size, file_stat.st_ino);
        while ((read_cnt = read(fd, buf, BLOCK_SIZE)) > 0 );
        free(buf);
    }
    close(fd);
}
int main(int argc, char *argv[]) {
    recursive_readdir("/mlfs");
    exit(EXIT_SUCCESS);
}


