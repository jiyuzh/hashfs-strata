########
#
#   Simplified Makefile, for each source we call a function (make-goal) to
#	compile, this is done in the last line of this file.
#
########

#CC=clang-3.8
#CC=gcc
CXX=g++ --std=c++11
CC=gcc5 --std=gnu99

########
#  List of all modules' directories
########
MODULES   := io concurrency config filesystem global ds log mkfs \
	mlfs posix storage

########
#  List of all modules' directories
########

########
#  All modules' directories in src and build
########
PREFIX ?= ebuild
SRC_DIR   := $(addprefix src/,$(MODULES))
BUILD_DIR := $(addprefix $(PREFIX)/,$(MODULES))

########
#  Source and Object files in their  module directories
########
SRC       := $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.c))
OBJ       := $(patsubst src/%.c,$(PREFIX)/%.o,$(SRC))

########
#  Lib directories
########
NVML_DIR := $(abspath $(CURDIR)/lib/nvml/src)
SPDK_DIR := $(abspath $(CURDIR)/lib/spdk)
LIBSPDK_DIR := $(abspath $(CURDIR)/src/storage/spdk/)
JEMALLOC_DIR := $(abspath $(CURDIR)/lib/jemalloc-4.5.0)
C_LIBCUCKOO_DIR := $(abspath $(CURDIR)/lib/libcuckoo-c/src)
API_DIR := $(abspath ../../nvm-file-indexing/api)

INCLUDES := $(addprefix -I,src/ $(LIBSPDK_DIR) $(NVML_DIR)/include $(C_LIBCUCKOO_DIR) $(API_DIR))

LD_FLAGS = -lpthread -lm -L$(NVML_DIR)/nondebug/ -lpmem -lrt \
	-Wl,-rpath=$(abspath $(API_DIR)/build) -L$(abspath $(API_DIR)/build) -lNVMIndexingAPI

#-Wl,-fuse-ld=gold

#MLFS_FLAGS = -DUSE_SLAB -DLIBFS -DMLFS_INFO
#MLFS_FLAGS = -DLIBFS
MLFS_FLAGS = -DLIBFS -DMLFS_INFO -DSTORAGE_PERF
#MLFS_FLAGS += -DCONCURRENT
#MLFS_FLAGS += -DINVALIDATION
MLFS_FLAGS += -DKLIB_HASH
#MLFS_FLAGS += -DUSE_SSD
#MLFS_FLAGS += -DUSE_HDD
#MLFS_FLAGS += -DMLFS_LOG
ifeq ($(PREFIX), hbuild)
    MLFS_FLAGS += -DHASHTABLE -DINDEX_NAME=\"hash\"
else
ifneq ($(PREFIX), ebuild)
    $(error "prefix must be ebuild or hbuild")
endif
	MLFS_FLAGS += -DINDEX_NAME=\"ext\ \"
endif

OPT_ARGS ?= -g -O3
#######
# Optimization definitions
#######
MLFS_FLAGS += -DREUSE_PREVIOUS_PATH  # optimization for extent trees

########
# Testing definitions
########
MLFS_FLAGS += -DSIMULATE_FRAGMENTATION

########
#  vpath and compile function for each file
########
vpath %.c $(SRC_DIR)

define make-goal
$1/%.o: %.c
	$(CC) $(INCLUDES) $(MLFS_FLAGS) $(OPT_ARGS) -fPIC -c $$< -o $$@ -DLIBFS
endef

########
#  Phony targets
########
.PHONY: all checkdirs clean mkfs spdk

all: checkdirs spdk libmlfs mkfs
checkdirs: $(BUILD_DIR)
	@mkdir -p bin

clean:
	rm -rf $(PREFIX)
	make -C src/storage/spdk/ clean PREFIX="$(PREFIX)"

########
#  Create dirs recursively
########
$(BUILD_DIR):
	@mkdir -p $@

########
#  Targets
########
#build/libmlfs.a: $(OBJ)
libmlfs: $(OBJ) spdk
	ar cr $(PREFIX)/libmlfs.a $(OBJ)
	$(CC) -shared $(OPT_ARGS) -o $(PREFIX)/libmlfs.so $(OBJ) $(LD_FLAGS) -DLIBFS $(MLFS_FLAGS) -L$(LIBSPDK_DIR) -lspdk -L$(JEMALLOC_DIR)/lib -Wl,-rpath=$(abspath $(JEMALLOC_DIR)/lib) -ljemalloc -ljson-c

spdk: $(OBJ)
	make -C src/storage/spdk/ MLFS_FLAGS="$(MLFS_FLAGS)" PREFIX="$(PREFIX)"

mkfs: $(OBJ) libmlfs
	$(CC) $(OPT_ARGS) -o $(PREFIX)/mkfs.o -c src/mkfs/mkfs.c $(INCLUDES) $(MLFS_FLAGS)
	$(CC) $(OPT_ARGS) -o bin/mkfs.mlfs  $(PREFIX)/libmlfs.a $(INCLUDES) -L$(LIBSPDK_DIR) -lspdk $(LD_FLAGS) -DLIBFS -Wl,-rpath=$(abspath $(LIBSPDK_DIR)) -Wl,-rpath=$(abspath $(NVML_DIR)/nondebug) -ljson-c


########
#  Compile each source into an object
########
$(foreach bdir,$(BUILD_DIR),$(eval $(call make-goal,$(bdir))))
