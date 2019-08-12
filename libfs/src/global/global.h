#ifndef __global_h__
#define __global_h__

#include "types.h"
#include "defs.h"


#ifdef __cplusplus
extern "C" {
#endif

extern uint8_t g_ssd_dev;
extern uint8_t g_log_dev;
extern uint8_t g_hdd_dev;

/**
 * Allocates all structures using the global values.
 */

#define g_n_devices			4
#define g_root_dev			1
#define g_block_size_bytes	4096UL
#define g_block_size_mask   (g_block_size_bytes - 1)
#define g_block_size_shift	12UL
// 2 is for microbenchmark
// 6 is generally too big. but Redis AOF mode requires at least 6.
#define g_max_blocks_per_operation 5
#define g_hdd_block_size_bytes 4194304UL
#define g_hdd_block_size_shift 22UL
#define g_directory_shift  16UL
#define g_directory_mask ((1 << ((sizeof(inum_t) * 8) - g_directory_shift)) - 1)
#define g_segsize_bytes    (1ULL << 30)  // 1 GB
#define g_max_read_cache_blocks  (2 << 18) // 2 GB
//#define g_max_read_cache_blocks  (1 << 10) // 4 MB

#define g_fd_start  1000000

#define NINODES 50000

#define ROOTINO 1  // root i-number

#define NDIRECT 7
#define NINDIRECT (g_block_size_bytes / sizeof(addr_t))

#define N_FILE_PER_DIR (g_block_size_bytes / sizeof(struct mlfs_dirent))
#define g_max_open_files ((NDIRECT + 1) * N_FILE_PER_DIR)

/* FIXME: compute this */
#define MAXFILE 10000000

// maximum size of full path
//#define MAX_PATH 4096
#define MAX_PATH 1024

// Directory is a file containing a sequence of dirent structures.
#define DIRSIZ 28

#define SHM_START_ADDR (void *)0x7ff000000000UL
#define SHM_SIZE (200 << 20)
#define SHM_NAME "/mlfs_shm"

/**
 *
 * All global variables here are default set by global.c
 * but might be changed the .ini reader.
 * Please keep the "g_" prefix so we know what is global and
 * what isn't.
 *
 */
// extern uint	g_n_devices; /* old NDEV */
// extern uint	g_root_dev; /* old ROOTDEV */
// extern uint	g_max_open_files; /*old NFILE*/
// extern uint	g_block_size_bytes; /* block size in bytes, old BSIZE*/
// extern uint	g_max_blocks_per_operation; /* old MAXOPBLOCKS*/
// extern uint	g_log_size_blks; /* log size in blocks, old LOGSIZE*/
// extern uint	g_fs_size_blks; /* filesystem size in blocks, old FSSIZE*/
// extern uint	g_max_extent_size_blks; /* filesystem size in blocks, old MAX_EXTENT_SIZE*/
// extern uint	g_block_reservation_size_blks; /* reservation size in blocks, old BRESRV_SIZE*/
// extern uint	g_fd_start; /* offset start of fds used by mlfs, old FD_START*/
extern char pwd[MAX_PATH + 1];

typedef enum indexing_api_choice {
    EXTENT_TREES,
    GLOBAL_CUCKOO_HASH,
    GLOBAL_HASH_TABLE,
    LEVEL_HASH_TABLES,
    RADIX_TREES,
    NONE
} indexing_choice_t;

extern indexing_choice_t g_idx_choice;
extern bool g_idx_cached;

#define IDXAPI_IS_PER_FILE() (g_idx_choice == EXTENT_TREES || g_idx_choice == LEVEL_HASH_TABLES \
        || g_idx_choice == RADIX_TREES)

#define IDXAPI_IS_GLOBAL() (g_idx_choice == GLOBAL_HASH_TABLE || g_idx_choice == GLOBAL_CUCKOO_HASH)

indexing_choice_t get_indexing_choice(void);
bool get_indexing_is_cached(void);
void print_global_idx_stats(bool enable_perf_stats);

#ifdef __cplusplus
}
#endif

#ifdef LIBFS
#undef HASHCACHE
#else
#define HASHCACHE
#endif

#undef EXTCACHE
#define EXTCACHE

#endif
