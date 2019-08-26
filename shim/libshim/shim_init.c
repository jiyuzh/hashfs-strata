#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <dlfcn.h>
#include <mlfs/mlfs_interface.h>

void shim_init() __attribute((constructor));
void shim_fini() __attribute((destructor));

int shim_initialized = 0;

//int (* my_printf)(const char *format, ...) = NULL;

void shim_init(void)
{
	if (!shim_initialized) {
		//asm("int $3");
		init_fs();
		shim_initialized = 1;
	}
    printf("libshim init: "
#ifdef MIRROR_SYSCALL
            "MIRROR_SYSCALL "
#endif
#ifdef SYS_TRACE
            "SYS_TRACE "
#endif
            "\n");
}

void shim_fini(void)
{
	shim_initialized = 0;
	shutdown_fs();
}
