#include <asm/unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <assert.h>

#include <mlfs/mlfs_interface.h>
#include <posix/posix_interface.h>
#include <global/types.h>

#include "interfaces.h"
#include "shim_types.h"
#include "shim_syscall_macro.h"
#include "shim_sys_fs.h"


/* System call ABIs
 * syscall number : %eax
 * parameter sequence: %rdi, %rsi, %rdx, %rcx, %r8, %r9
 * Kernel destroys %rcx, %r11
 * return : %rax
 */

#define PATH_BUF_SIZE 1024
#define MLFS_PREFIX (char *)"/mlfs"

#ifdef __cplusplus
extern "C" {
#endif
static char shim_pwd[PATH_BUF_SIZE];

// _output can be uninitialized
// return value: output length?
static int collapse_name(const char *input, char *_output)
{
	char *output = _output;

	while(1) {
		/* Detect a . or .. component */
		if (input[0] == '.') {
			if (input[1] == '.' && input[2] == '/') {
				/* A .. component */
				if (output == _output)
					return -1;
				input += 2;
				while (*(++input) == '/');
				while(--output != _output && *(output - 1) != '/');
				continue;
			} else if (input[1] == '/' || input[1] == 0) {
				/* A . component */
				input += 1;
				while (*(++input) == '/');
				continue;
			}
		}

		/* Copy from here up until the first char of the next component */
		while(1) {
			if (*input == 0) {
				*output = 0;
				return output - _output;
			}
			*output++ = *input++;
			if (*input == '/') {
				*output++ = '/';
				/* Consume any extraneous separators */
				while (*(++input) == '/');
				break;
			}
		}
	}
}

// path is a potentially relative path
// path_buf and dest_path are char buffer, whose size is buf_size
// return value: a pointer to absolute path
//     it will be path_buf if already absolute
//     or dest_path, which is shim_pwd + path
static char * get_absolute_path(const char *path, char *path_buf, char *dest_path, size_t buf_size) {
    if (path == NULL || path_buf == NULL || dest_path == NULL) {
        return NULL;
    }
    else {
        collapse_name(path, path_buf);
        if (*path_buf == '/') {
            return path_buf;
        }
        else {
            strncpy(dest_path, shim_pwd, buf_size);
            strncat(dest_path, path_buf, buf_size - strlen(dest_path));
            return dest_path;
        }
    }
}

#ifdef MIRROR_SYSCALL
#define MLFS_FD_MAP_SIZE 1024
static int fd_map[MLFS_FD_MAP_SIZE];
static char fn_map[MLFS_FD_MAP_SIZE][PATH_BUF_SIZE];
#define MLFS_RET mlfs_ret
#define MLFS_RET_DEF int mlfs_ret
#define MLFS_FD fd_map[fd]
#define MLFS_FNAME fn_map[fd]
#define MLFS_BUF mlfs_buf
#define MLFS_BUF_DEF(type, count) type mlfs_buf = malloc(count)
#define CMP_SUBFIELD(stat1, stat2, field_fmt, field, cmp) do {\
		if (!(cmp(stat1->field, stat2->field))) {\
            syscall_warn(#field " ("#stat1")" field_fmt " != " "("#stat2")" field_fmt "\n", stat1->field, stat2->field);\
		}} while(0)
#define NUM_CMP(a,b) (a == b)
#define STRING_CMP(a,b) (!strcmp(a,b))
#define ST_MODE_CMP(a,b) ((a&S_IFMT) == (b&S_IFMT))
#define CMP_STAT(stat1, stat2)\
		CMP_SUBFIELD(stat1, stat2, "%u", st_mode, ST_MODE_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%ld", st_size, NUM_CMP)
/*		CMP_SUBFIELD(stat1, stat2, "%lu", st_dev, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%lu", st_ino, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%lu", st_nlink, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%u", st_uid, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%u", st_gid, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%lu", st_rdev, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%lu", st_blksize, NUM_CMP);\
		CMP_SUBFIELD(stat1, stat2, "%lu", st_blocks, NUM_CMP)*/
#else
#define MLFS_RET ret
#define MLFS_RET_DEF
#define MLFS_FD fd
#define MLFS_BUF buf
#define MLFS_BUF_DEF(type, count)
#endif
int shim_do_chdir(const char *pathname)
{
    int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(pathname, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(__NR_chdir)
		:"rax", "rdi"
	   );
	}

    if (in_mlfs) {
		MLFS_RET_DEF;
        MLFS_RET = mlfs_posix_chdir(fullpath);
        syscall_dump("%d", MLFS_RET, "%s", fullpath);
#ifdef MIRROR_SYSCALL
		if (ret != MLFS_RET) {
            syscall_abort(" path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
    }

    if (ret == 0) {
        strncpy(shim_pwd, fullpath, PATH_BUF_SIZE);
    }
    return ret;
}

int shim_do_open(const char *filename, int flags, mode_t mode)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%esi;"
		"mov %3, %%edx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fullpath), "r"(flags), "r"(mode), "r"(__NR_open)
		:"rax", "rdi", "rsi", "rdx"
		);
	}

	if (in_mlfs) {	
		MLFS_RET_DEF;
        MLFS_RET = mlfs_posix_open(fullpath, flags, mode);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%#X", flags, "%#X", mode);

		if (MLFS_RET >= 0 && !check_mlfs_fd(MLFS_RET)) {
            syscall_warn("incorrect fd %d: path %s\n", MLFS_RET, fullpath);
		}
#ifdef MIRROR_SYSCALL
        if ((ret < 0 && MLFS_RET > 0) || (ret > 0 && MLFS_RET < 0)) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
        }
        if (ret >= 0) {
            fd_map[ret] = MLFS_RET;
            strncpy(fn_map[ret], fullpath, PATH_BUF_SIZE);
        }
#endif
	}

    return ret;
}

int shim_do_openat(int dfd, const char *filename, int flags, mode_t mode)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%edx;"
		"mov %4, %%r10d;"
		"mov %5, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(dfd),"r"(fullpath), "r"(flags), "r"(mode), "r"(__NR_openat)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

    if (in_mlfs) {
		MLFS_RET_DEF;
		if (dfd != AT_FDCWD) {
            syscall_abort("%s\n", "Only support AT_FDCWD");
		}
        MLFS_RET = mlfs_posix_open(fullpath, flags, mode);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%d", dfd, "%#X", flags, "%#X", mode);

		if (MLFS_RET >= 0 && !check_mlfs_fd(MLFS_RET)) {
            syscall_warn("incorrect fd %d: path %s\n", MLFS_RET, fullpath);
		}
#ifdef MIRROR_SYSCALL
        if ((ret < 0 && MLFS_RET > 0) || (ret > 0 && MLFS_RET < 0)) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
        }
        if (ret >= 0) {
            fd_map[ret] = MLFS_RET;
            strncpy(fn_map[ret], fullpath, PATH_BUF_SIZE);
        }
#endif
    }

	return ret;
}

int shim_do_creat(char *filename, mode_t mode)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%esi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(mode), "r"(__NR_creat)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_creat(fullpath, mode);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%#X", mode);

		if (MLFS_RET >= 0 && !check_mlfs_fd(MLFS_RET)) {
            syscall_warn("incorrect fd %d: path %s\n", MLFS_RET, fullpath);
		}
#ifdef MIRROR_SYSCALL
        if ((ret < 0 && MLFS_RET > 0) || (ret > 0 && MLFS_RET < 0)) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
        }
        if (ret >= 0) {
            fd_map[ret] = MLFS_RET;
            strncpy(fn_map[ret], fullpath, PATH_BUF_SIZE);
        }
#endif
	}

	return ret;
}

size_t shim_do_read(int fd, void *buf, size_t count)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);

#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
		asm("mov %1, %%edi;"
			"mov %2, %%rsi;"
			"mov %3, %%rdx;"
			"mov %4, %%eax;"
			"syscall;\n\t"
			"mov %%eax, %0;\n\t"
			:"=r"(ret)
			:"r"(fd), "m"(buf), "r"(count), "r"(__NR_read)
			:"rax", "rdi", "rsi", "rdx"
			);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_BUF_DEF(void*, count);
		MLFS_RET = mlfs_posix_read(get_mlfs_fd(MLFS_FD), MLFS_BUF, count);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", count);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
		for (size_t i=0; i < ret; ++i) {
			if (((char*)buf)[i] != ((char*)mlfs_buf)[i]) {
				syscall_abort("inconsistent at fd %d(%s), ret %d(%d),\
						count %lu, @%lu buf %#1x, mlfs_buf %#1x\n",
						fd, MLFS_FNAME, ret, MLFS_RET, count, 
						i, ((char*)buf)[i], ((char*)mlfs_buf)[i]);
			}
		}
		free(MLFS_BUF);
#endif
	}

	return ret;
}

size_t shim_do_pread64(int fd, void *buf, size_t count, loff_t off)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%rdx;"
		"mov %4, %%r10;"
		"mov %5, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(count), "r"(off), "r"(__NR_pread64)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_BUF_DEF(void*, count);
		MLFS_RET = mlfs_posix_pread64(get_mlfs_fd(MLFS_FD), MLFS_BUF, count, off);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", count, "%lu", off);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
		for (size_t i=0; i < ret; ++i) {
			if (((char*)buf)[i] != ((char*)mlfs_buf)[i]) {
				syscall_abort("inconsistent at fd %d(%s), ret %d(%d),\
						count %lu, @%lu buf %#1x, mlfs_buf %#1x\n",
						fd, MLFS_FNAME, ret, MLFS_RET, count, 
						i, ((char*)buf)[i], ((char*)mlfs_buf)[i]);
			}
		}
		free(MLFS_BUF);
#endif
	}

	return ret;
}

size_t shim_do_write(int fd, void *buf, size_t count)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%rdx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(count), "r"(__NR_write)
		:"rax", "rdi", "rsi", "rdx"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_write(get_mlfs_fd(MLFS_FD), buf, count);
        syscall_dump("%d", MLFS_RET, "%d", fd, "%lu", count);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

size_t shim_do_pwrite64(int fd, void *buf, size_t count, loff_t off)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%rdx;"
		"mov %4, %%r10;"
		"mov %5, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(count),"r"(off), "r"(__NR_pwrite64)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_pwrite64(get_mlfs_fd(MLFS_FD), buf, count, off);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", count, "%lu", off);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_close(int fd)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(__NR_close)
		:"rax", "rdi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_close(get_mlfs_fd(MLFS_FD));
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
		fd_map[fd] = 0;
		MLFS_FNAME[0] = 0;
#endif
	}

	return ret;
}

int shim_do_lseek(int fd, off_t offset, int origin)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%edx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(offset), "r"(origin), "r"(__NR_lseek)
		:"rax", "rdi", "rsi", "rdx"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_lseek(get_mlfs_fd(MLFS_FD), offset, origin);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", offset, "%d", origin);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_mkdir(void *path, mode_t mode)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(path, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%esi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(mode), "r"(__NR_mkdir)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_mkdir(fullpath, mode);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%#X", mode);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_rmdir(const char *path)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(path, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(__NR_rmdir)
		:"rax", "rdi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_rmdir(fullpath);
        syscall_dump("%d", MLFS_RET, "%s", fullpath);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_rename(char *oldname, char *newname)
{
	int ret;
    char old_path_buf[PATH_BUF_SIZE], old_dest_path[PATH_BUF_SIZE], *old_fullpath;
    char new_path_buf[PATH_BUF_SIZE], new_dest_path[PATH_BUF_SIZE], *new_fullpath;
    old_fullpath = get_absolute_path(oldname, old_path_buf, old_dest_path, PATH_BUF_SIZE);
    new_fullpath = get_absolute_path(newname, new_path_buf, new_dest_path, PATH_BUF_SIZE);
	uint8_t in_mlfs = (strncmp(old_fullpath, MLFS_PREFIX, 5) == 0) && (strncmp(new_fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(old_fullpath), "m"(new_fullpath), "r"(__NR_rename)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_rename(old_fullpath, new_fullpath);
        syscall_dump("%d", MLFS_RET, "%s", old_fullpath, "%s", new_fullpath);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("oldpath %s, newpath %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    old_fullpath, new_fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_fallocate(int fd, int mode, off_t offset, off_t len)
{
	int ret;
    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%esi;"
		"mov %3, %%rdx;"
		"mov %4, %%r10;"
		"mov %5, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(mode), "r"(offset), "r"(len), "r"(__NR_fallocate)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_fallocate(get_mlfs_fd(MLFS_FD), offset, len);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%#X", mode, "%lu", offset, "%lu", len);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_stat(const char *filename, struct stat *buf)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "m"(buf), "r"(__NR_stat)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_BUF_DEF(struct stat*, sizeof(struct stat));
		MLFS_RET = mlfs_posix_stat(fullpath, MLFS_BUF);
        syscall_dump("%d", MLFS_RET, "%s", fullpath);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
		if (ret == 0) {
			CMP_STAT(buf, MLFS_BUF);
		}
		free(MLFS_BUF);
#endif
	}

	return ret;
}

int shim_do_lstat(const char *filename, struct stat *buf)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "m"(buf), "r"(__NR_lstat)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		// Symlink does not implemented yet
		// so stat and lstat is identical now.
		MLFS_RET_DEF;
		MLFS_BUF_DEF(struct stat*, sizeof(struct stat));
		MLFS_RET = mlfs_posix_stat(fullpath, MLFS_BUF);
        syscall_dump("%d", MLFS_RET, "%s", fullpath);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
		if (ret == 0) {
			CMP_STAT(buf, MLFS_BUF);
		}
		free(MLFS_BUF);
#endif
	}

	return ret;
}

int shim_do_fstat(int fd, struct stat *buf)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(__NR_fstat)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_BUF_DEF(struct stat*, sizeof(struct stat));
		MLFS_RET = mlfs_posix_fstat(get_mlfs_fd(MLFS_FD), MLFS_BUF);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
		if (ret == 0) {
			CMP_STAT(buf, MLFS_BUF);
		}
		free(MLFS_BUF);
#endif
	}

	return ret;
}

int shim_do_truncate(const char *filename, off_t length)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(filename, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(length), "r"(__NR_truncate)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_truncate(fullpath, length);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%lu", length);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_ftruncate(int fd, off_t length)
{
	int ret;
    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(length), "r"(__NR_ftruncate)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_ftruncate(get_mlfs_fd(MLFS_FD), length);
		syscall_trace(__func__, ret, 2, MLFS_FD, length);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", length);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_unlink(const char *path)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(path, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(__NR_unlink)
		:"rax", "rdi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_unlink(fullpath);
        syscall_dump("%d", MLFS_RET, "%s", fullpath);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_symlink(const char *target, const char *linkpath)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(target, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(target), "m"(linkpath), "r"(__NR_symlink)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		printf("%s\n", target);
		printf("symlink: do not support yet\n");
		exit(-1);
	}

	return ret;
}

int shim_do_access(const char *pathname, int mode)
{
	int ret;
    char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
    fullpath = get_absolute_path(pathname, path_buf, dest_path, PATH_BUF_SIZE);
    uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%esi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(mode), "r"(__NR_access)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_access(fullpath, mode);
        syscall_dump("%d", MLFS_RET, "%s", fullpath, "%#X", mode);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fullpath, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_fsync(int fd)
{
	int ret;
    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(__NR_fsync)
		:"rax", "rdi"
		);
	}

	if (in_mlfs) {
		// libfs has quick persistency guarantee.
		// fsync is nop.
		MLFS_RET_DEF;
		MLFS_RET = 0;
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}


	return ret;
}

int shim_do_fdatasync(int fd)
{
	int ret;
    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(__NR_fdatasync)
		:"rax", "rdi"
		);
	}

	if (in_mlfs) {
		// fdatasync is nop.
		MLFS_RET_DEF;
		MLFS_RET = 0;
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_abort("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

int shim_do_sync(void)
{
	int ret;

	printf("sync: do not support yet\n");
	exit(-1);

	asm("mov %1, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(__NR_sync)
		:"rax"
		);

	return ret;
}

int shim_do_fcntl(int fd, int cmd, void *arg)
{
	int ret;

    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%esi;"
		"mov %3, %%rdx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "r"(cmd), "m"(arg), "r"(__NR_fcntl)
		:"rax", "rdi", "rsi", "rdx"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_RET = mlfs_posix_fcntl(get_mlfs_fd(MLFS_FD), cmd, arg);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%#X", cmd);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
            syscall_warn("fd %d path %s, inconsistent return value: ret %d, mlfs_ret %d\n",
                    fd, MLFS_FNAME, ret, MLFS_RET);
		}
#endif
	}

	return ret;
}

void* shim_do_mmap(void *addr, size_t length, int prot,
		int flags, int fd, off_t offset)
{
	void* ret;

	if (check_mlfs_fd(MLFS_FD)) {
		printf("mmap: not implemented\n");
		exit(-1);
	}

	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%edx;"
		"mov %4, %%r10d;"
		"mov %5, %%r8d;"
		"mov %6, %%r9;"
		"mov %7, %%eax;"
		"syscall;\n\t"
		"mov %%rax, %0;\n\t"
		:"=r"(ret)
		:"m"(addr), "r"(length), "r"(prot), "r"(flags), "r"(fd), "r"(offset), "r"(__NR_mmap)
		:"rax", "rdi", "rsi", "rdx", "r10", "r8", "r9"
		);

	return ret;
}

int shim_do_munmap(void *addr, size_t length)
{
	int ret;

	asm("mov %1, %%rdi;"
		"mov %2, %%rsi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(addr), "r"(length), "r"(__NR_munmap)
		:"rax", "rdi", "rsi"
		);

	return ret;
}

size_t shim_do_getdents(int fd, struct linux_dirent *buf, size_t count)
{
	int ret;
    uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%rdx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(count), "r"(__NR_getdents)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		MLFS_BUF_DEF(struct linux_dirent*, count);
		MLFS_RET = mlfs_posix_getdents(get_mlfs_fd(MLFS_FD), MLFS_BUF, count);
        syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%lu", count);
#ifdef MIRROR_SYSCALL
        // shouldn't compare return value, since it represents the size read to the buffer
        // should be fs dependent
		size_t n_entry = 0, mlfs_n_entry = 0;
		for (struct linux_dirent *dir = buf;
			(uint8_t*)dir < (uint8_t*)buf + ret;
			dir = (struct linux_dirent*)((uint8_t*)dir + dir->d_reclen)) {
			++n_entry;
		}
		for (struct linux_dirent *mlfs_dir = MLFS_BUF;
			(uint8_t*)mlfs_dir < (uint8_t*)MLFS_BUF + MLFS_RET;
			mlfs_dir = (struct linux_dirent*)((uint8_t*)mlfs_dir + mlfs_dir->d_reclen)) {
			++mlfs_n_entry;
		}
		if (n_entry != mlfs_n_entry) {
            syscall_abort("fd %d path %s, inconsistent n_entry %lu, mlfs %lu\n",
                    fd, MLFS_FNAME, n_entry, mlfs_n_entry);
		}
        memcpy(buf, MLFS_BUF, count);
        ret = MLFS_RET;
		free(MLFS_BUF);
#endif
	}

	return ret;
}

size_t shim_do_getdents64(int fd, struct linux_dirent64 *buf, size_t count)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%rsi;"
		"mov %3, %%rdx;"
		"mov %4, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"r"(fd), "m"(buf), "r"(count), "r"(__NR_getdents)
		:"rax", "rdi", "rsi", "rdx", "r10"
		);
	}

	if (in_mlfs) {
		syscall_abort("%s\n", "getdent64 is not supported\n");
	}

	return ret;
}

int shim_do_chmod(const char *pathname, mode_t mode)
{
	int ret;
	char path_buf[PATH_BUF_SIZE], dest_path[PATH_BUF_SIZE], *fullpath;
	fullpath = get_absolute_path(pathname, path_buf, dest_path, PATH_BUF_SIZE);
	uint8_t in_mlfs = (strncmp(fullpath, MLFS_PREFIX, 5) == 0);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%rdi;"
		"mov %2, %%esi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fullpath), "r"(mode), "r"(__NR_chmod)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		// strata doesn't implement permission support, always return 0 (success)
		MLFS_RET = 0;
		syscall_dump("%d", MLFS_RET, "%s", fullpath, "%#X", mode);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
			// doesn't check return value
		}
#endif
	}
	return ret;
}

int shim_do_fchmod(int fd, mode_t mode)
{
	int ret;
	uint8_t in_mlfs = check_mlfs_fd(MLFS_FD);
#ifdef MIRROR_SYSCALL
	if (1) {
#else
	if (!in_mlfs) {
#endif
	asm("mov %1, %%edi;"
		"mov %2, %%esi;"
		"mov %3, %%eax;"
		"syscall;\n\t"
		"mov %%eax, %0;\n\t"
		:"=r"(ret)
		:"m"(fd), "r"(mode), "r"(__NR_fchmod)
		:"rax", "rdi", "rsi"
		);
	}

	if (in_mlfs) {
		MLFS_RET_DEF;
		// strata doesn't implement permission support, always return 0 (success)
		MLFS_RET = 0;
		syscall_dump("%d", MLFS_RET, "%d", MLFS_FD, "%#X", mode);
#ifdef MIRROR_SYSCALL
		if (MLFS_RET != ret) {
			// doesn't check return value
		}
#endif
	}
	return ret;
}

char *shim_do_getcwd(char *buf, size_t size)
{
	char *ret;
	if (shim_pwd[0] == 0) { // haven't called chdir
		asm("mov %1, %%rdi;"
			"mov %2, %%rsi;"
			"mov %3, %%eax;"
			"syscall;\n\t"
			"mov %%rax, %0;\n\t"
			:"=r"(ret)
			:"m"(buf), "r"(size), "r"(__NR_getcwd)
			:"rax", "rdi", "rsi"
	   );
	}
	else { // have called chdir, return shim_pwd
		strncpy(buf, shim_pwd, size);
		ret = buf;
	}
	return ret;
}
#ifdef __cplusplus
}
#endif
