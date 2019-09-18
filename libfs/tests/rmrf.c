#define _GNU_SOURCE
#include <dirent.h>     /* Defines DT_* constants */
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>

#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

struct linux_dirent {
    long           d_ino;
    off_t          d_off;
    unsigned short d_reclen;
    char           d_name[];
};

#define BUF_SIZE 1024
void rmrf(const char *dirpath) {
    //int n_file = 0;
    int fd, nread;
    struct linux_dirent *d;
    char buf[BUF_SIZE];
    char path_buf[BUF_SIZE];
    //char d_type;
    int bpos;
    fd = open(dirpath, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        fprintf(stderr, "open, file: %s, error: %s\n", dirpath, strerror(errno));
    }

    for ( ; ; ) {
        nread = syscall(SYS_getdents, fd, buf, BUF_SIZE);
        if (nread > 0) {
            //n_file++;
            for (bpos = 0; bpos < nread;) {
                d = (struct linux_dirent *) (buf + bpos);
                //d_type = *(buf + bpos + d->d_reclen - 1);
                if (strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
                    snprintf(path_buf, BUF_SIZE, "%s/%s", dirpath, d->d_name);
                    // d_type is unreliable
                    rmrf(path_buf);
                }
                bpos += d->d_reclen;
            }
        }
        else if (nread == 0)
            break;
        else if (nread == -1 && errno == ENOTDIR) {
            int ret = unlink(dirpath);
            if (ret != 0) {
                fprintf(stderr, "unlink, file: %s, error: %s\n", dirpath, strerror(errno));
            }
        }
        else {
            fprintf(stderr, "unhandled ret: %d, errno %d, %s, path: %s\n", nread, errno, strerror(errno), dirpath);
            abort();
        }

    }
}

int main(int argc, char *argv[]) {
    const char *root_dir_path = argc > 1 ? argv[1] : ".";
    rmrf(root_dir_path);

    exit(EXIT_SUCCESS);
}


