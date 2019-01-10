#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <ctype.h>
#include <signal.h>
#include <time.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <mlfs/mlfs_interface.h>

#define KB 1024
#define BLOCKSIZE 4*KB

char buf[BLOCKSIZE];

int randomappend(const char *fname, size_t nblocks) {
  int fd = open(fname, O_WRONLY | O_CREAT, 0666);
  if (fd < 0) {
    perror("randomappend");
    return -1;
  }

  for (size_t i = 0; i < nblocks; ++i) {
    int ret = write(fd, buf, BLOCKSIZE);
    if (ret != BLOCKSIZE) {
      perror("append");
      goto err;
    }
  }

  fsync(fd);
  close(fd);
  return 0;

err:
  close(fd);
  return -1;
}

int randomtruncate(const char *fname, size_t nblocks) {
  int fd = open(fname, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if (fd < 0) {
    perror("randomtruncate");
    return -1;
  }

  int offset = lseek(fd, 0, SEEK_END);
  if (offset < 0) {
    perror("lseek");
    goto err;
  }

  if (!offset) {
    close(fd);
    return 0;
  }

  int new_size = offset - (int)(nblocks * BLOCKSIZE);
  new_size = (int)fmax(offset, 0);

  int ret = ftruncate(fd, new_size);
  if (ret < 0) {
    perror("ftruncate");
    goto err;
  }

  fsync(fd);
  close(fd);
  return 0;

err:
  close(fd);
  return -1;
}

int main(int argc, const char **argv) {

  struct sigaction sa;
  srand(time(NULL));
  memset(buf, 42, BLOCKSIZE);

  if (argc < 3) {
    fprintf(stderr, "Usage: %s <nfiles> <nblocks> <skip> <mode> <s>\n", argv[0]);
    return -1;
  }

  int nfiles  = atoi(argv[1]);
  int nblocks = atoi(argv[2]);
  int skip    = atoi(argv[3]);
  int mode    = atoi(argv[4]);
  int start   = atoi(argv[5]);

	init_fs();

  printf("Press enter to continue\n");
  (void)getchar();

  for (int f = 0; f < 100 && mode == 0; ++f) {
    char dname[BUFSIZ];
    snprintf(dname, BUFSIZ, "/mlfs/dir-%d/", f);

    int ret = mkdir(dname, 0777);
    if (ret != 0 && errno != EEXIST) {
      fprintf(stderr, "errno = %d\n", errno);
      perror("mkdir");
      return ret;
    }
  }

  for (int f = 0; f < nfiles && mode == 0; ++f) {
    char fname[BUFSIZ];
    snprintf(fname, BUFSIZ, "/mlfs/dir-%d/testfile-%d", f % 100, f);

    int ret = randomappend(fname, nblocks);
    if (ret) return ret;
    printf("\rCreate: %d / %d", f + 1, nfiles);
    fflush(stdout);
  }

  for (int f = start; f < nfiles && mode == 1; f += skip) {
    char fname[BUFSIZ];
    snprintf(fname, BUFSIZ, "/mlfs/dir-%d/testfile-%d", f % 100, f);
    int ret = unlink(fname);
    if (ret) return ret;
    printf("\rTruncate: %d / %d", (f / skip) + 1, nfiles / skip);
    fflush(stdout);
  }

  make_digest_request_sync(100);

  printf("\n");

  return 0;
}

