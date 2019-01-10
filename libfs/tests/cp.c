#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#define BLOCK_SIZE 4096
int main(int argc, char **argv) {
    if (argc < 3) {
        printf("%s src dst\n", argv[0]);
    }
    int src = open(argv[1], O_RDONLY);
    if (src == -1) {
        perror("open src failed");
        exit(-1);
    }
    int dst = open(argv[2], O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (dst == -1) {
        perror("open dst failed");
        exit(-1);
    }
    struct stat src_stat;
    assert(fstat(src, &src_stat) == 0);
    void *buf = malloc(BLOCK_SIZE);
    int read_cnt = 0;
    while ((read_cnt = read(src, buf, BLOCK_SIZE)) > 0) {
        write(dst, buf, read_cnt);
    }
    close(src);
    close(dst);
    return 0;
}
