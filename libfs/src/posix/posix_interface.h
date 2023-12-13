#ifndef _POSIX_INTERFACE_H_
#define _POSIX_INTERFACE_H_

#include <sys/stat.h>
#include "global/global.h"

#ifdef __cplusplus
extern "C" {
#endif

int mlfs_posix_chdir(const char *pathname);
int mlfs_posix_open(const char *path, int flags, unsigned short mode);
int mlfs_posix_access(const char *pathname, int mode);
int mlfs_posix_creat(char *path, uint16_t mode);
ssize_t mlfs_posix_read(int fd, void *buf, size_t count);
ssize_t mlfs_posix_pread64(int fd, void *buf, size_t count, loff_t off);
ssize_t mlfs_posix_write(int fd, const void *buf, size_t count);
ssize_t mlfs_posix_pwrite64(int fd, const void *buf, size_t count, loff_t off);
off_t mlfs_posix_lseek(int fd, int64_t offset, int origin);
int mlfs_posix_mkdir(char *path, unsigned int mode);
int mlfs_posix_rmdir(char *path);
int mlfs_posix_close(int fd);
int mlfs_posix_stat(const char *filename, struct stat *stat_buf);
int mlfs_posix_fstat(int fd, struct stat *stat_buf);
int mlfs_posix_fallocate(int fd, offset_t offset, offset_t len);
int mlfs_posix_unlink(const char *filename);
int mlfs_posix_truncate(const char *filename, off_t length);
int mlfs_posix_ftruncate(int fd, off_t length);
int mlfs_posix_rename(char *oldname, char *newname);
int mlfs_posix_getdents(int fd, struct linux_dirent *buf, size_t count, offset_t off);
int mlfs_posix_fcntl(int fd, int cmd, void *arg);

#ifdef __cplusplus
}
#endif

#endif
