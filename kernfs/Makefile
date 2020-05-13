########
#
#   Simplified Makefile, for each source we call a function (make-goal) to
#	compile, this is done in the last line of this file.
#
########

SUBDIR	:= io cache concurrency global ds mlfs storage .

PREFIX ?= build
SRC_DIR   := $(addprefix , $(SUBDIR))
BUILD_DIR := $(addprefix $(PREFIX)/, $(SUBDIR))
LIBFS_DIR := $(CURDIR)/../libfs

########
#  Source and Object files in their  module directories
########
SRC	:= $(foreach sdir, $(SRC_DIR), $(wildcard $(sdir)/*.c))
SRC := $(filter-out %mkfs.c %kernf.c,$(SRC))
OBJ	:= $(patsubst %.c,$(PREFIX)/%.o,$(SRC))
HDER := $(foreach sdir, $(SRC_DIR), $(wildcard $(sdir)/*.h))

########
#  Lib directories
########
#NVML_DIR := $(abspath $(LIBFS_DIR)/lib/nvml/src)
NVML_DIR := 
DPDK_DIR := $(abspath $(LIBFS_DIR)/lib/dpdk-16.07/x86_64-native-linuxapp-gcc)
SPDK_DIR := $(abspath $(LIBFS_DIR)/lib/spdk)
LIBSPDK_DIR := $(abspath $(LIBFS_DIR)/src/storage/spdk/)
CUCKOO_FILTER_DIR := $(abspath $(LIBFS_DIR)/lib/cuckoofilter)
CUCKOO_DIR := $(abspath $(LIBFS_DIR)/lib/libcuckoo)
#GLIB_DIR := $(abspath $(LIBFS_DIR)/lib/glib)
GLIB_DIR :=
JEMALLOC_DIR := $(abspath $(LIBFS_DIR)/lib/jemalloc-4.5.0)
STORAGE_ENGINE := $(wildcard $(LIBFS_DIR)/$(PREFIX)/storage/*.o)
#C_LIBCUCKOO_DIR := $(abspath $(LIBFS_DIR)/lib/libcuckoo-c/src)
#LIBSPDK := $(abspath $(LIBFS_DIR)/src/storage/spdk/libspdk.so)
API_DIR := $(abspath ../../nvm-file-indexing/api)

INCLUDES  := $(addprefix -I, . $(LIBSPDK_DIR) $(NVML_DIR)/include $(CUCKOO_DIR) $(CUCKOO_FILTER_DIR)/src $(GLIB_DIR)/ $(GLIB_DIR)/glib $(C_LIBCUCKOO_DIR) $(API_DIR))

LD_FLAGS = -lpthread -lm -L$(NVML_DIR)/nondebug/ -lpmem -lrt -ljson-c\
	-Wl,-rpath=$(abspath $(GLIB_DIR)/glib/.libs) -L$(abspath $(GLIB_DIR)/glib/.libs) \
	-Wl,-rpath=$(abspath $(API_DIR)/build) -L$(abspath $(API_DIR)/build) -lNVMIndexingAPI

#-lglib-2.0


#MLFS_FLAGS = -DUSE_SLAB -DMLFS_INFO -DKERNFS
MLFS_FLAGS = -DMLFS_INFO -DKERNFS -DSTORAGE_PERF
#MLFS_FLAGS = -DKERNFS
MLFS_FLAGS += -DBALLOC
#MLFS_FLAGS += -DDIGEST_OPT
#MLFS_FLAGS += -DIOMERGE
MLFS_FLAGS += -DCONCURRENT
MLFS_FLAGS += -DFCONCURRENT
#MLFS_FLAGS += -DUSE_SSD
#MLFS_FLAGS += -DUSE_HDD
#MLFS_FLAGS += -DMIGRATION
#MLFS_FLAGS += -DEXPERIMENTAL
#ifeq ($(PREFIX), hbuild)
#	MLFS_FLAGS += -DHASHTABLE -DINDEX_NAME=\"hash\"
#else
#ifneq ($(PREFIX), ebuild)
#    $(error "prefix must be ebuild or hbuild")
#endif
#	MLFS_FLAGS += -DINDEX_NAME=\"ext\ \"
#endif

OPT_ARGS ?= -g -O3 
AVX_FLAGS = -march=native


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

#CC = clang-3.8
CC = gcc9.2 --std=gnu99 -mrtm
CXX = g++ -std=c++11 -mrtm

define make-goal
$1/%.o: %.c
	$(CC) $(INCLUDES) -fPIC $(OPT_ARGS) $(AVX_FLAGS) -c $$< -o $$@ -DKERNFS $(MLFS_FLAGS)
endef

########
#  Phony targets
########
.PHONY: all checkdirs clean filesystem

all: $(BUILD_DIR) filesystem

clean:
	@rm -rf ./$(PREFIX)

########
#  Create dirs recursively
########
$(BUILD_DIR):
	@mkdir -p $@

########
#  Targets
########
filesystem: $(OBJ) $(SRC) $(HDER)
	#$(CC) -shared $(OPT_ARGS) $(AVX_FLAGS) -o $(PREFIX)/libkernfs.so $(OBJ) $(CUCKOO_FILTER_DIR)/libcuckoofilter.a $(LD_FLAGS) $(MLFS_FLAGS)
	$(CC) -shared $(OPT_ARGS) $(AVX_FLAGS) -o $(PREFIX)/libkernfs.so $(OBJ) $(LD_FLAGS) $(MLFS_FLAGS)

########
#  Compile each source into an object
########
$(foreach bdir,$(BUILD_DIR),$(eval $(call make-goal,$(bdir))))
