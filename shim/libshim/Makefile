CC = gcc5
#CC = clang-3.8
# The compiling order matters
OBJS = syscalldb.o shim_init.o \
	   shim_sys_fs.o \
	   shim_syscalls.o \
	   syscall_trace.o \
	   shim_table.o

HEADERS = $(wildcard *.h)

CUR_DIR = $(shell pwd)
LIBFS_SRC_DIR = $(abspath $(CUR_DIR)/../../libfs/src)
LIBFS_BUILD_DIR = $(abspath $(CUR_DIR)/../../libfs/build)
LIBSPDK_DIR = $(abspath $(CUR_DIR)/../../libfs/src/storage/spdk)
LIBFS = $(abspath $(CUR_DIR)/../../libfs/build/libmlfs.a)
NVML_DIR= $(abspath $(CUR_DIR)/../../libfs/lib/nvml/src/nondebug)

CFLAGS = -fno-strict-aliasing -Wall -fno-omit-frame-pointer -I$(LIBFS_SRC_DIR) #-Wno-initializer-overrides
#ASFLAGS = -Wa,--noexecstack -x assembler-with-cpp
LDFLAGS =  -Wl,-rpath=$(abspath $(LIBFS_BUILD_DIR)) \
		   -Wl,-rpath=$(abspath $(LIBSPDK_DIR)) \
		   -Wl,-rpath=$(abspath $(NVML_DIR)) \
		   -lm

# could be -DMIRROR_SYSCALL -DSYS_TRACE
DEBUG_ARGS ?=

libshim.so: $(OBJS)
	$(CC) -shared $(DEBUG) -o $@ $^ -ldl -L$(LIBFS_BUILD_DIR) -lmlfs -L$(LIBSPDK_DIR) -lspdk $(LDFLAGS) $(DEBUG_ARGS)

shim_table.o: shim_table.c
	gcc -g -c -Wall -Werror -fPIC $(CFLAGS) $< -o $@ -I$(LIBFS_SRC_DIR) $(DEBUG_ARGS)

%.o: %.c $(HEADERS)
	@#$(CC) -E -Werror -fPIC $(CFLAGS) $<  -o $(patsubst %.o, %.i, $@)
	$(CC) -g -c -Wall -Werror -fPIC $(CFLAGS) $< -o $@ -I$(LIBFS_SRC_DIR) $(DEBUG_ARGS)

%.o: %.S $(HEADERS)
	$(AS) $(ASFLAGS) -g -c $< -o $@

clean:
	rm -rf libshim.so $(OBJS) *.i
