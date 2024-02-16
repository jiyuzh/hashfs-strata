Rethinking File Mapping for Persistent Memory
==================================

Strata is a research prototype file system, presented in SOSP 2017 ([Strata]).
We extend Strata so we can test a variety of PM file mapping structures. We
present the results of this in our FAST 2021 paper ([indexing])

We have tested on Ubuntu 20.04 LTS, Linux kernel 5.4.0 and gcc
version 9.2.

This repository contains initial source code and tests. Benchmarks will be
released soon. As a research prototype, Strata has several limitations,
described in the [limitations section](#limitations).

Strata requires at least there
partitions of NVM: operation log (1 - 2 GB), NVM shared area (It depends on
your test, and an undo log region (1 GB at most). I recommend to use more than 8 GB at least).

You first 

### Building Strata ###

Assume current directory is a project root directory.

Make sure to initialize the repository and sub-repositories first:

```shell
    git clone <repo>
    cd <repo>
    git submodule init
    git submodule update
```

##### 1. Change memory configuration

~~~shell
./utils/change_dev_size.py <dax0.0 GB> <SSD GB/0> <HDD GB/0> <dax1.0 GB>
~~~

This script does the following:

1. Opens `libfs/src/storage/storage.h`
2. Modifies`dev_size` array values with each storage size (the same as in your
   grub conf, see the [running Strata](#runningstrata) section) in bytes.
    - `dev_size[0]`: could be always 0 (not used)
    - `dev_size[1]`: dax0.0 size
    - `dev_size[2]`: SSD size : just put 0 for now
    - `dev_size[3]`: HDD size : put 0 for now
    - `dev_size[4]`: dax1.0 size

##### 2. Build dependent libraries (SPDK, NVML, JEMALLOC, SYSCALL_INTERCEPT)

~~~shell
cd libfs/lib

# Download sources. (If there are no source codes.)
make redownload
make

cd ../..
~~~

For SPDK build errors, please check a SPDK website (http://www.spdk.io/doc/getting_started.html)

For NVML build errors, please check a NVML repository (https://github.com/pmem/nvml/)

##### 3. Build File system

~~~shell
cd nvm-file-indexing/api
./make.sh
cd ../..

./remake.sh
~~~



### Jiyuan: Running HashFS

```shell
cd libfs/tests
./run_hashfs.sh
cd ../..
```



### Running Strata ###

##### 1. Setup NVM (DEV-DAX) devices

To set up the `/dev/dax*` devices, you will need to use ndctl.

Documentation is available here: https://docs.pmem.io/persistent-memory/getting-started-guide/what-is-ndctl

A script for reference is at `./reconfigure-pmem.sh`

##### 2. Setup storage size

This step requires rebuilding of Libfs and KernFS.

1. Open `libfs/src/storage/storage.h`
2. Modifie `dev_size` array values with each storage size in bytes.
   - `dev_size[0]`: could be always 0 (not used)
   - `dev_size[1]`: dax0.0 size
   - `dev_size[2]`: SSD size : just put 0 for now
   - `dev_size[3]`: HDD size : put 0 for now
   - `dev_size[4]`: dax1.0 size
   - `dev_size[5]`: dax2.0 size

##### 3. Formatting storages

The simplest way:

~~~shell
cd kernfs/tests
sudo MLFS_IDX_STRUCT=HASHFS ./mkfs.sh
cd ../..
~~~

If you encounter an error message, "mmap invalid argument",
it means kernel does not allow mmap for NVM emulation.
Usually, incorrect (or unaligned) setting of storage sizes (at step 3) causes
the problem.
Please make sure that your storage size is correct in "libfs/src/storage/storage.h"

##### 6. Run KernelFS

~~~shell
cd kernfs/tests
sudo MLFS_IDX_STRUCT=HASHFS ./run.sh kernfs
cd ../..
~~~

##### 7. Run testing program

~~~shell
cd libfs/tests
sudo MLFS_IDX_STRUCT=HASHFS ./run.sh iotest sw 2G 4K 1 #sequential write, 2GB file with 4K IO and 1 thread
cd ../..
~~~

### Select indexing mechanism

Set proper indexing mechanism to `MLFS_IDX_STRUCT` environment variable.
Please refer to `global.c` and benchmark scripts (E.g., `bench/filebench/run_server.sh`).

Some commonly used values are:

```
RADIX_TREES
EXTENT_TREES
HASHFS
GLOBAL_CUCKOO_HASH
LEVEL_HASH_TABLES
```

For example, if you want to run `hashfs` indexing:

```shell
# In one terminal,
cd kernfs/tests
sudo MLFS_IDX_STRUCT=HASHFS ./mkfs.sh
sudo MLFS_IDX_STRUCT=HASHFS ./run.sh kernfs

# In another terminal,
cd libfs/tests
sudo MLFS_IDX_STRUCT=HASHFS ./run.sh iotest sw 1G 4K 1
```

### Strata configuration ###

##### 1. LibFS configuration ######

In `libfs/Makefile`, search `MLFS_FLAGS` as keyword

~~~~shell
MLFS_FLAGS = -DLIBFS -DMLFS_INFO
#MLFS_FLAGS += -DCONCURRENT
MLFS_FLAGS += -DINVALIDATION
#MLFS_FLAGS += -DKLIB_HASH
MLFS_FLAGS += -DUSE_SSD
#MLFS_FLAGS += -DUSE_HDD
#MLFS_FLAGS += -DMLFS_LOG
~~~~

`DCONCURRENT` - allow parallelism in libfs <br/>
`DKLIB_HASH` - use klib hashing for log hash table <br/>
`DUSE_SSD`, `DUSE_HDD` - make LibFS to use SSD and HDD <br/>

##### 2. KernelFS configuration #####

~~~shell
#MLFS_FLAGS = -DKERNFS
MLFS_FLAGS += -DBALLOC
#MLFS_FLAGS += -DDIGEST_OPT
#MLFS_FLAGS += -DIOMERGE
#MLFS_FLAGS += -DCONCURRENT
#MLFS_FLAGS += -DFCONCURRENT
#MLFS_FLAGS += -DUSE_SSD
#MLFS_FLAGS += -DUSE_HDD
#MLFS_FLAGS += -DMIGRATION
#MLFS_FLAGS += -DEXPERIMENTAL
~~~

`DBALLOC` - use new block allocator (use it always) <br/>
`DIGEST_OPT` - use log coalescing <br/>
`DIOMERGE` - use io merging <br/>
`DCONCURRENT` - allow concurrent digest <br/>
`DMIGRATION` - allow data migration. It requires turning on `DUSE_SSD` <br/>

For debugging, DIGEST_OPT, DIOMERGE, DCONCURRENT is disabled for now

### Debugging ###

Here are some common issues and how we were able to resolve them.

1. `sudo ./run.sh kernfs` or `sudo ./run.sh iotest ...` segfaults.

- Make sure to run `sudo ./bin/mkfs.mlfs` on all devices used for testing.
  + `sudo ./bin/mkfs.mlfs 1` for `dax0.0` (required)
  + `sudo ./bin/mkfs.mlfs 4` for `dax1.0` (required)
  + `sudo ./bin/mkfs.mlfs 2` for SSD area (only if `DUSE_SSD` is defined)
  + `sudo ./bin/mkfs.mlfs 3` for HDD area (only if `DUSE_HDD` is defined)

### Limitations ###

1. KernelFS is currently implemented in user-level.
2. Leases are not fully implemented.
3. A directory could contain up to 1000 files.
4. mmap is not supported yet.
5. Benchmarks are not fully tested in all configurations. Working
   configurations are described in our paper.
6. Can only rename inside the same directory.
7. There are known bugs in fork.

### Future Documentation ###

For documentation on current work or for more detailed documentation
about a particular feature, please check the [docs][docs] directory.

Available topics:

- [SPDK Concurrency (for SSD operations)][spdk_doc]

[Strata]: http://www.cs.utexas.edu/~yjkwon/publication/strata/ "Strata project"
[docs]: docs/
[spdk_doc]: docs/concurrency.md
[indexing]: https://www.usenix.org/conference/fast21/presentation/neal "Rethinking File Mapping for Persistent Memory"
