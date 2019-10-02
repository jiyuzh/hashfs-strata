#ifndef _FS_H_
#define _FS_H_


#include "global/global.h"
#include "global/types.h"
#include "global/defs.h"
#include "log/log.h"
#include "filesystem/stat.h"
#include "filesystem/shared.h"
#include "global/mem.h"
#include "global/ncx_slab.h"
#include "filesystem/extents.h"
#include "filesystem/extents_bh.h"
#include "ds/uthash.h"
#include "ds/khash.h"

#include "filesystem/cache_stats.h"

#ifdef __cplusplus
extern "C" {
#endif
#include <sys/mman.h>

// iangneal: for API init
extern mem_man_fns_t strata_mem_man;
extern callback_fns_t strata_callbacks;
extern idx_spec_t strata_idx_spec;

// global variables
extern uint8_t fs_dev_id;
extern struct disk_superblock *disk_sb;
extern struct super_block *sb[g_n_devices + 1];


// libmlfs Disk layout:
// [ boot block | sb block | inode blocks | free bitmap | data blocks | log blocks ]
// [ inode block | free bitmap | data blocks | log blocks ] is a block group.
// If data blocks is full, then file system will allocate a new block group.
// Block group expension is not implemented yet.
#ifndef MAX_GET_BLOCKS_RETURN
#define MAX_GET_BLOCKS_RETURN 8
#define MAX_NUM_BLOCKS_LOOKUP 256
#endif

// directory entry cache
struct dirent_data {
	mlfs_hash_t hh;
	char name[DIRSIZ]; // key
	struct inode *inode;
	offset_t offset;
};

/* A bug note. UThash has a weird bug that
 * if offset is uint64_t type, it cannot find data
 * It is OK to use 32 bit because the offset does not
 * overflow 32 bit */
typedef struct dcache_key {
	uint32_t inum;
	uint32_t offset; // offset of directory inode / 4096.
} dcache_key_t;

// dirent array block (4KB) cache
struct dirent_block {
	dcache_key_t key;
	mlfs_hash_t hash_handle;
	uint8_t dirent_array[g_block_size_bytes];
	addr_t log_addr;
	uint32_t log_version;
};

typedef struct fcache_key {
	offset_t offset;
} fcache_key_t;

struct fcache_block {
	offset_t key;
	mlfs_hash_t hash_handle;
	uint32_t inum;
	addr_t log_addr;		// block # of update log
	uint8_t invalidate;
	uint32_t log_version;
	/* track what in-block offset the cached data for this block in the log
	 * starts from, non-zero offset happens when previous zero-start log
	 * expired while new write (append actually) to this block doesn't start
	 * from zero. The correct data from 0 locates on NVM shared area
	 * this offset should be 0 ~ g_block_size_bytes
	 */
	uint16_t start_offset;
	uint8_t is_data_cached;
	uint8_t *data;
	struct list_head l;	// entry for global list
};

struct cache_copy_list {
	uint8_t *dst_buffer;
	uint8_t *cached_data;
	uint32_t size;
	struct list_head l;
};

struct dlookup_data {
	mlfs_hash_t hh;
	char path[MAX_PATH];	// key: canonical path
	struct inode *inode;
};

typedef struct bmap_request_arr {
	// input
	offset_t start_offset; //offset from file start in bytes
	uint32_t blk_count; //num_blocks
	// output
	addr_t block_no[MAX_GET_BLOCKS_RETURN];
	uint32_t blk_count_found;
	uint8_t dev;
} bmap_req_arr_t;

typedef struct bmap_request {
	// input
	offset_t start_offset; //offset from file start in bytes
	uint32_t blk_count; //num_blocks
	// output
	addr_t block_no;
	uint32_t blk_count_found;
	uint8_t dev;
} bmap_req_t;

typedef struct pblk_lookup_arr {
	addr_t m_pblk[MAX_NUM_BLOCKS_LOOKUP];
	uint32_t m_lens[MAX_NUM_BLOCKS_LOOKUP];
	offset_t m_offsets[MAX_NUM_BLOCKS_LOOKUP];
	addr_t *m_pblk_dyn;
	uint32_t *m_lens_dyn;
	offset_t *m_offsets_dyn;
	uint8_t dyn;
	uint32_t size;
} pblk_lookup_t;

// statistics
typedef struct mlfs_libfs_stats {
	uint64_t digest_wait_tsc;
	uint64_t digest_wait_nr;

	uint64_t l0_search_tsc;
	uint64_t l0_search_nr;
        uint64_t fcache_lock_tsc;
        uint64_t fcache_lock_nr;
        uint64_t fcache_get_tsc;
        uint64_t fcache_get_nr;
        uint64_t fcache_val_tsc;
        uint64_t fcache_val_nr;
        uint64_t fcache_init_tsc;
        uint64_t fcache_init_nr;
        uint64_t fcache_all_tsc;
        uint64_t fcache_all_nr;

	uint64_t tree_search_tsc;
	uint64_t tree_search_nr;
	uint64_t log_write_tsc;
	uint64_t log_write_nr;
	uint64_t log_write_inode_tsc;
	uint64_t log_write_inode_nr;
	uint64_t log_write_data_tsc;
	uint64_t log_write_data_nr;
	uint64_t log_write_data_aligned_tsc;
	uint64_t log_write_data_aligned_nr;
	uint64_t log_aligned_wronly_tsc;
	uint64_t log_aligned_wronly_nr;
	uint64_t log_aligned_bh_tsc;
	uint64_t log_aligned_bh_nr;
	uint64_t log_hash_fc_add_tsc;
	uint64_t log_hash_fc_add_nr;
        uint64_t fc_add_zalloc_tsc;
        uint64_t fc_add_zalloc_nr;
	uint64_t log_write_data_unaligned_tsc;
	uint64_t log_write_data_unaligned_nr;
	uint64_t log_alloc_tsc;
	uint64_t log_alloc_nr;
	uint64_t loghdr_write_nr;
	uint64_t loghdr_write_tsc;
    uint64_t log_hash_update_nr;
    uint64_t log_hash_update_tsc;
	uint64_t log_commit_tsc;
	uint64_t log_commit_nr;
	uint64_t read_data_tsc;
	stats_dist_t read_data_bytes;
	uint64_t dir_search_tsc;
	uint64_t dir_search_nr_hit;
	uint64_t dir_search_nr_miss;
	uint64_t dir_search_nr_notfound;
	uint64_t ialloc_tsc;
	uint64_t ialloc_nr;
	uint64_t tmp_nr;
	uint64_t tmp_tsc;
	uint64_t bcache_search_tsc;
	uint64_t bcache_search_nr;
	uint64_t dir_search_ext_nr;
	uint64_t dir_search_ext_tsc;
	uint64_t path_storage_nr;
	uint64_t path_storage_tsc;
	stats_dist_t read_per_index;
    // hash table stuff
    uint64_t n_entries_read;
    uint64_t n_lookups;
    uint64_t hash_fn_tsc;
    uint64_t hash_fn_nr;
    uint64_t hash_loop_tsc;
    uint64_t hash_loop_nr;
    uint64_t hash_iter_nr;
    stats_dist_t hash_lookup_count;
    // Indexing cache rates
    cache_stats_t cache_stats;
    // Fragmentation stuff
    uint64_t n_files;
    uint64_t n_fragments;
    uint64_t n_blocks;
    double layout_score_derived;
} libfs_stat_t;

extern struct lru g_fcache_head;

extern libfs_stat_t g_perf_stats;
extern uint8_t enable_perf_stats;

extern pthread_rwlock_t *icache_rwlock;
extern pthread_rwlock_t *dcache_rwlock;
extern pthread_rwlock_t *dlookup_rwlock;
extern pthread_rwlock_t *invalidate_rwlock;
extern pthread_rwlock_t *g_fcache_rwlock;

extern struct dirent_block *dirent_hash[g_n_devices + 1];
extern struct inode *inode_hash[g_n_devices + 1];
extern struct dlookup_data *dlookup_hash[g_n_devices + 1];

static inline struct inode *icache_find(uint8_t dev, uint32_t inum)
{
	struct inode *inode;

	mlfs_assert(dev == g_root_dev);

	pthread_rwlock_rdlock(icache_rwlock);

	HASH_FIND(hash_handle, inode_hash[dev], &inum,
        		sizeof(uint32_t), inode);

	pthread_rwlock_unlock(icache_rwlock);

	return inode;
}

// Inodes per block.
#define IPB           (g_block_size_bytes / sizeof(struct dinode))

// Block containing inode i
/*
#define IBLOCK(i, disk_sb)  ((i/IPB) + disk_sb.inode_start)
*/
static inline addr_t get_inode_block(uint8_t dev, uint32_t inum)
{
	return (inum / IPB) + disk_sb[dev].inode_start;
}

static inline void init_api_idx_struct(uint8_t dev, struct inode *inode) {
    // iangneal: indexing API init.
    if (IDXAPI_IS_PER_FILE() && inode->itype == T_FILE) {
        static bool notify = false;

        if (!notify) {
            printf("Init API extent trees!!!\n");
            notify = true;
        }

        paddr_range_t direct_extents = {
            .pr_start      = get_inode_block(dev, inode->inum),
            .pr_blk_offset = (sizeof(struct dinode) * (inode->inum % IPB)) + 64,
            .pr_nbytes     = 64
        };

        idx_struct_t *tmp = (idx_struct_t*)mlfs_zalloc(sizeof(*inode->ext_idx));
        int init_err;

        switch(g_idx_choice) {
            case EXTENT_TREES_TOP_CACHED:
                g_idx_cached = true;
            case EXTENT_TREES:
                init_err = extent_tree_fns.im_init_prealloc(&strata_idx_spec,
                                                            &direct_extents,
                                                            tmp);
                break;
            case LEVEL_HASH_TABLES:
                init_err = levelhash_fns.im_init_prealloc(&strata_idx_spec,
                                                          &direct_extents,
                                                          tmp);
                break;
            case RADIX_TREES:
                init_err = radixtree_fns.im_init_prealloc(&strata_idx_spec,
                                                          &direct_extents,
                                                          tmp);
                break;
            default:
                panic("Invalid choice!!!\n");
        }

        FN(tmp, im_set_caching, tmp, g_idx_cached);

        if (init_err) {
            fprintf(stderr, "Error in extent tree API init: %d\n", init_err);
            panic("Could not initialize API per-inode structure!\n");
        }

        if (tmp->idx_fns->im_set_stats) {
            FN(tmp, im_set_stats, tmp, enable_perf_stats);
        }

        inode->ext_idx = tmp;
    }
}

static inline struct inode *icache_alloc_add(uint8_t dev, uint32_t inum)
{
	struct inode *inode;
	pthread_rwlockattr_t rwlattr;

	mlfs_assert(dev == g_root_dev);

	inode = (struct inode *)mlfs_zalloc(sizeof(*inode));

	if (!inode)
		panic("Fail to allocate inode\n");

	inode->dev = dev;
	inode->inum = inum;
	inode->i_ref = 1;

	inode->_dinode = (struct dinode *)inode;

#if 0
	pthread_rwlockattr_setpshared(&rwlattr, PTHREAD_PROCESS_SHARED);

	pthread_rwlock_init(&inode->fcache_rwlock, &rwlattr);
	inode->fcache = NULL;
	inode->n_fcache_entries = 0;

#ifdef KLIB_HASH
	mlfs_info("allocate hash %u\n", inode->inum);
	inode->fcache_hash = kh_init(fcache);
#endif

	INIT_LIST_HEAD(&inode->i_slru_head);

	pthread_spin_init(&inode->de_cache_spinlock, PTHREAD_PROCESS_SHARED);
	inode->de_cache = NULL;
#endif

	//pthread_rwlock_wrlock(icache_rwlock);

	HASH_ADD(hash_handle, inode_hash[dev], inum,
			sizeof(uint32_t), inode);

	//pthread_rwlock_unlock(icache_rwlock);

	return inode;
}

static inline struct inode *icache_add(struct inode *inode)
{
	uint32_t inum = inode->inum;
	// here seem suspicious, ialloc also initializes this rwlock
	pthread_rwlock_init(&inode->i_rwlock, NULL);

	pthread_rwlock_wrlock(icache_rwlock);

	HASH_ADD(hash_handle, inode_hash[inode->dev], inum,
	 		sizeof(uint32_t), inode);

	pthread_rwlock_unlock(icache_rwlock);

	return inode;
}

static inline int icache_del(struct inode *ip)
{
	pthread_rwlock_wrlock(icache_rwlock);

	HASH_DELETE(hash_handle, inode_hash[ip->dev], ip);

	pthread_rwlock_unlock(icache_rwlock);

	return 0;
}

#ifdef KLIB_HASH
static struct fcache_block *fcache_find(struct inode *inode, offset_t key)
{
#define fcache_stats
#undef fcache_stats
	khiter_t k;
	struct fcache_block *fc_block = NULL;
    uint64_t start_tsc, total_tsc;
   
#ifdef fcache_stats
    if (enable_perf_stats)
        total_tsc = asm_rdtscp();
#endif

	if (inode->fcache_hash == NULL) {
#ifdef fcache_stats
        if (enable_perf_stats)
            start_tsc = asm_rdtscp();
#endif

		pthread_rwlock_wrlock(&inode->fcache_rwlock);
		inode->fcache_hash = kh_init(fcache);
		pthread_rwlock_unlock(&inode->fcache_rwlock);

#ifdef fcache_stats
        if (enable_perf_stats) {
            g_perf_stats.fcache_init_tsc += (asm_rdtscp() - start_tsc);
            g_perf_stats.fcache_init_nr++;
        }
#endif
	}

#ifdef fcache_stats
    if (enable_perf_stats)
        start_tsc = asm_rdtscp();
#endif

	pthread_rwlock_rdlock(&inode->fcache_rwlock);
    
#ifdef fcache_stats
    if (enable_perf_stats) {
        g_perf_stats.fcache_lock_tsc += (asm_rdtscp() - start_tsc);
    }

    if (enable_perf_stats)
        start_tsc = asm_rdtscp();
#endif

	k = kh_get(fcache, inode->fcache_hash, key);

#ifdef fcache_stats
    if (enable_perf_stats) {
        g_perf_stats.fcache_get_tsc += (asm_rdtscp() - start_tsc);
        g_perf_stats.fcache_get_nr++;
    }
#endif

	if (k == kh_end(inode->fcache_hash)) {
#ifdef fcache_stats
        if (enable_perf_stats)
            start_tsc = asm_rdtscp();
#endif
	
        pthread_rwlock_unlock(&inode->fcache_rwlock);
        
#ifdef fcache_stats
        if (enable_perf_stats) {
            g_perf_stats.fcache_lock_tsc += (asm_rdtscp() - start_tsc);
            g_perf_stats.fcache_lock_nr++;

            g_perf_stats.fcache_all_tsc += (asm_rdtscp() - total_tsc);
            g_perf_stats.fcache_all_nr++;
        }
#endif
		return NULL;
	}

#ifdef fcache_stats
    if (enable_perf_stats)
        start_tsc = asm_rdtscp();
#endif
	
    fc_block = kh_value(inode->fcache_hash, k);
    
#ifdef fcache_stats
    if (enable_perf_stats) {
        g_perf_stats.fcache_val_tsc += (asm_rdtscp() - start_tsc);
        g_perf_stats.fcache_val_nr++;
    }

    if (enable_perf_stats)
        start_tsc = asm_rdtscp();
#endif
	
    pthread_rwlock_unlock(&inode->fcache_rwlock);
    
#ifdef fcache_stats
    if (enable_perf_stats) {
        g_perf_stats.fcache_lock_tsc += (asm_rdtscp() - start_tsc);
        g_perf_stats.fcache_lock_nr++;

        g_perf_stats.fcache_all_tsc += (asm_rdtscp() - total_tsc);
        g_perf_stats.fcache_all_nr++;
    }
#endif

	return fc_block;
}

// if cache data (instead of log), log_addr and start_offset aren't used
static inline struct fcache_block *fcache_alloc_add(struct inode *inode,
		offset_t key, addr_t log_addr, uint16_t start_offset)
{
	struct fcache_block *fc_block;
	khiter_t k;
	int ret;
    uint64_t start_tsc;

    if (enable_perf_stats)
        start_tsc = asm_rdtscp();

#define USE_FCACHE_POOL
#undef USE_FCACHE_POOL
#ifdef USE_FCACHE_POOL
    if (!inode->fcache_block_pool) {
        inode->fcache_block_pool = (struct fcache_block*) mmap(NULL, 1024 * 1024 * 1024, 
                PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
        if (inode->fcache_block_pool == (void*)-1) {
            perror("pool");
            panic("no mmap!");
        }
    }
    fc_block = inode->fcache_block_pool + inode->pool_pointer;
    inode->pool_pointer++;
#else
	fc_block = (struct fcache_block *)mlfs_zalloc(sizeof(*fc_block));
#endif
	if (!fc_block)
		panic("Fail to allocate fcache block\n");
    if (enable_perf_stats) {
        g_perf_stats.fc_add_zalloc_tsc += (asm_rdtscp() - start_tsc);
        g_perf_stats.fc_add_zalloc_nr++;
    }
    
    //start_cache_stats();
	fc_block->key = key;
	fc_block->log_addr = log_addr;
	fc_block->invalidate = 0;
	fc_block->is_data_cached = 0;
	fc_block->inum = inode->inum;
    fc_block->start_offset = start_offset;
	inode->n_fcache_entries++;
	INIT_LIST_HEAD(&fc_block->l);
    //end_cache_stats(&(g_perf_stats.cache_stats));

	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	if (inode->fcache_hash == NULL) {
		inode->fcache_hash = kh_init(fcache);
	}

	k = kh_put(fcache, inode->fcache_hash, key, &ret);
	if (ret < 0)
		panic("fail to insert fcache value");
	/*
	else if (!ret) {
		kh_del(fcache, inode->fcache_hash, k);
		k = kh_put(fcache, inode->fcache_hash, key, &ret);
	}
	*/

	kh_value(inode->fcache_hash, k) = fc_block;
	//mlfs_info("add key %u @ inode %u\n", key, inode->inum);

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return fc_block;
}

/*!
 * try to delete key from inode's fcache hashtable
 * @param[in] inode the file
 * @param[in] key offset at block size granularity
 * @return 0: already deleted 1: deleted successfully
 */
static inline int fcache_del(struct inode *inode,
        offset_t key)
{
	khint_t k;
    int ret = 0;
	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	k = kh_get(fcache, inode->fcache_hash, key);

	if (k != kh_end(inode->fcache_hash)) {
		kh_del(fcache, inode->fcache_hash, k);
		inode->n_fcache_entries--;
        ret = 1;
	}

	/*
	if (k != kh_end(inode->fcache_hash)) {
		kh_del(fcache, inode->fcache_hash, k);
		inode->n_fcache_entries--;
		mlfs_debug("del key %u @ inode %u\n", fc_block->key, inode->inum);
	}
	*/

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return ret;
}

static inline int fcache_del_all(struct inode *inode)
{
	khiter_t k;
	struct fcache_block *fc_block;
	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	for (k = kh_begin(inode->fcache_hash);
			k != kh_end(inode->fcache_hash); k++) {
		if (kh_exist(inode->fcache_hash, k)) {
			fc_block = kh_value(inode->fcache_hash, k);

			if (fc_block && fc_block->is_data_cached) {
				list_del(&fc_block->l);
				mlfs_free(fc_block->data);
			} else if (fc_block) {
#ifndef USE_FCACHE_POOL
				mlfs_free(fc_block);
#endif
			}
		}
	}

	mlfs_debug("destroy hash %u\n", inode->inum);
	kh_destroy(fcache, inode->fcache_hash);
	inode->fcache_hash = NULL;
	pthread_rwlock_unlock(&inode->fcache_rwlock);
	return 0;
}
// UTHash version
#else
static inline struct fcache_block *fcache_find(struct inode *inode, offset_t key)
{
	struct fcache_block *fc_block = NULL;

	pthread_rwlock_rdlock(&inode->fcache_rwlock);

	HASH_FIND(hash_handle, inode->fcache, &key,
        		sizeof(offset_t), fc_block);

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return fc_block;
}

static inline struct fcache_block *fcache_alloc_add(struct inode *inode,
		offset_t key, addr_t log_addr)
{
	struct fcache_block *fc_block;
	fc_block = (struct fcache_block *)mlfs_zalloc(sizeof(*fc_block));
	if (!fc_block)
		panic("Fail to allocate fcache block\n");

	fc_block->key = key;
	fc_block->inum = inode->inum;
	fc_block->log_addr = log_addr;
	fc_block->invalidate = 0;
	fc_block->is_data_cached = 0;
	INIT_LIST_HEAD(&fc_block->l);

	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	HASH_ADD(hash_handle, inode->fcache, key,
	 		sizeof(offset_t), fc_block);
	inode->n_fcache_entries++;

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return fc_block;
}

static inline int fcache_del(struct inode *inode,
		struct fcache_block *fc_block)
{
	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	HASH_DELETE(hash_handle, inode->fcache, fc_block);
	inode->n_fcache_entries--;

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return 0;
}

static inline int fcache_del_all(struct inode *inode)
{
	struct fcache_block *item, *tmp;

	pthread_rwlock_wrlock(&inode->fcache_rwlock);

	HASH_ITER(hash_handle, inode->fcache, item, tmp) {
		HASH_DELETE(hash_handle, inode->fcache, item);
		if (item->is_data_cached) {
			list_del(&item->l);
			mlfs_free(item->data);
		}
		mlfs_free(item);
	}
	HASH_CLEAR(hash_handle, inode->fcache);

	inode->n_fcache_entries = 0;

	pthread_rwlock_unlock(&inode->fcache_rwlock);

	return 0;
}
#endif

static inline struct inode *de_cache_find(struct inode *dir_inode,
		const char *_name, offset_t *offset)
{
	struct dirent_data *dirent_data;

	HASH_FIND_STR(dir_inode->de_cache, _name, dirent_data);

	if (dirent_data) {
		*offset = dirent_data->offset;
		return dirent_data->inode;
	} else {
		*offset = 0;
		return NULL;
	}
}

static inline struct inode *de_cache_alloc_add(struct inode *dir_inode,
		const char *name, struct inode *inode, offset_t _offset)
{
	struct dirent_data *_dirent_data;

	_dirent_data = (struct dirent_data *)mlfs_zalloc(sizeof(*_dirent_data));
	if (!_dirent_data)
		panic("Fail to allocate dirent data\n");

	strcpy(_dirent_data->name, name);

	_dirent_data->inode = inode;
	_dirent_data->offset = _offset;

	pthread_spin_lock(&dir_inode->de_cache_spinlock);

	HASH_ADD_STR(dir_inode->de_cache, name, _dirent_data);

	dir_inode->n_de_cache_entry++;

	pthread_spin_unlock(&dir_inode->de_cache_spinlock);

	return dir_inode;
}

static inline int de_cache_del(struct inode *dir_inode, const char *_name)
{
	struct dirent_data *dirent_data;

	HASH_FIND_STR(dir_inode->de_cache, _name, dirent_data);
	if (dirent_data) {
		pthread_spin_lock(&dir_inode->de_cache_spinlock);
		HASH_DEL(dir_inode->de_cache, dirent_data);
		dir_inode->n_de_cache_entry--;
		pthread_spin_unlock(&dir_inode->de_cache_spinlock);
	}

	return 0;
}

static inline struct inode *dlookup_find(uint8_t dev, const char *path)
{
	struct dlookup_data *_dlookup_data;

	pthread_rwlock_rdlock(dlookup_rwlock);

	HASH_FIND_STR(dlookup_hash[dev], path, _dlookup_data);

	pthread_rwlock_unlock(dlookup_rwlock);

	if (!_dlookup_data)
		return NULL;
	else
		return _dlookup_data->inode;
}

static inline struct inode *dlookup_alloc_add(uint8_t dev,
		struct inode *inode, const char *_path)
{
	struct dlookup_data *_dlookup_data;

	_dlookup_data = (struct dlookup_data *)mlfs_zalloc(sizeof(*_dlookup_data));
	if (!_dlookup_data)
		panic("Fail to allocate dlookup data\n");

	strcpy(_dlookup_data->path, _path);

	_dlookup_data->inode = inode;

	pthread_rwlock_wrlock(dlookup_rwlock);

	HASH_ADD_STR(dlookup_hash[dev], path, _dlookup_data);

	pthread_rwlock_unlock(dlookup_rwlock);

	return inode;
}

static inline int dlookup_del(uint8_t dev, const char *path)
{
	struct dlookup_data *_dlookup_data;

	pthread_rwlock_wrlock(dlookup_rwlock);

	HASH_FIND_STR(dlookup_hash[dev], path, _dlookup_data);
	if (_dlookup_data)
		HASH_DEL(dlookup_hash[dev], _dlookup_data);

	pthread_rwlock_unlock(dlookup_rwlock);

	return 0;
}

//forward declaration
struct fs_stat;

void shared_slab_init(uint8_t shm_slab_index);

void read_superblock(uint8_t dev);
void read_root_inode(uint8_t dev);

int read_ondisk_inode(uint8_t dev, uint32_t inum, struct dinode *dip);
int sync_inode_ext_tree(uint8_t dev, struct inode *inode);
struct inode* icreate(uint8_t dev, uint8_t type);
struct inode* ialloc(uint8_t dev, uint32_t inum, struct dinode *dip);
int idealloc(struct inode *inode);
struct inode* idup(struct inode*);
struct inode* iget(uint8_t dev, uint32_t inum);
void iput(struct inode*);
void iunlockput(struct inode*);
void iupdate(struct inode*);
int itrunc(struct inode *inode, offset_t length);
int bmap(struct inode *ip, struct bmap_request *bmap_req);
int bmap_hashfs(struct inode *ip, struct bmap_request_arr *bmap_req_arr);

int dir_check_entry_fast(struct inode *dir_inode);
struct inode* dir_lookup(struct inode*, char*, offset_t *);
int dir_get_linux_dirent(struct inode *dir_inode, struct linux_dirent *buf, offset_t *p_off, size_t nbytes);
int dir_add_entry(struct inode *inode, char *name, uint32_t inum);
int dir_remove_entry(struct inode *inode,char *name, uint32_t inum);
int dir_change_entry(struct inode *dir_inode, char *oldname, char *newname);
int namecmp(const char*, const char*);
struct inode* namei(const char*);
struct inode* nameiparent(const char*, char*);
ssize_t readi_unopt(struct inode*, uint8_t *dst, offset_t off, size_t io_size);
ssize_t readi(struct inode*, uint8_t *dst, offset_t off, size_t io_size);
void stati(struct inode*, struct stat *);
size_t add_to_log(struct inode*, uint8_t*, offset_t, size_t);
int check_log_invalidation(struct fcache_block *_fcache_block);
uint8_t *get_dirent_block(struct inode *dir_inode, offset_t offset);
void show_libfs_stats(const char *title);
void reset_libfs_stats();

//APIs for debugging.
uint32_t dbg_get_iblkno(uint32_t inum);
void dbg_dump_inode(uint8_t dev, uint32_t inum);
void dbg_check_inode(void *data);
void dbg_check_dir(void *data);
void dbg_dir_dump(uint8_t dev, uint32_t inum);
void dbg_path_walk(const char *path);

// mempool slab for libfs
extern ncx_slab_pool_t *mlfs_slab_pool;
// mempool on top of shared memory
extern ncx_slab_pool_t *mlfs_slab_pool_shared;
extern uint8_t shm_slab_index;

extern pthread_rwlock_t *shm_slab_rwlock;
extern pthread_rwlock_t *shm_lru_rwlock;

extern uint64_t *bandwidth_consumption;


static inline void irdlock(struct inode *ip)
{
	pthread_rwlock_rdlock(&ip->i_rwlock);
	// It seems that no one is using I_BUSY. comment it out
	// ip->flags |= I_BUSY;
}

static inline void iwrlock(struct inode *ip)
{
	pthread_rwlock_wrlock(&ip->i_rwlock);
}

static inline void iunlock(struct inode *ip)
{
	pthread_rwlock_unlock(&ip->i_rwlock);
	// It seems that no one is using I_BUSY. comment it out
	// ip->flags &= ~I_BUSY;
}

static inline void calculate_fragmentation(void) {
    if (IDXAPI_IS_HASHFS()) {
        printf("HashFS doesn't suffer from traditional fragmentation.\n");
        return;
    }

    uint32_t end = find_next_zero_bit(sb[g_root_dev]->s_inode_bitmap,
        sb[g_root_dev]->ondisk->ninodes, 1);

    size_t total_blocks = 0;
    size_t total_fragments = 0;
    size_t total_files = 0;

    for (uint32_t inum = 0; inum < end; ++inum) {
        struct inode *ip = iget(g_root_dev, inum);

        if (! ((ip->flags & I_VALID) && (ip->itype == T_FILE))) continue;

        size_t nblocks = ip->size >> g_block_size_shift;
        nblocks += (ip->size % g_block_size_bytes) > 0;

        if (!nblocks) continue;
        
        total_files++;

        size_t nfound = 0;
        size_t nsearch = 0;

        /* Search for all the blocks. bmap will only return contiguous block
         * ranges, so the number of searches is equivalent to the number of
         * fragments.
         */
        while (nfound < nblocks) {
            struct bmap_request bmap_req;
            bmap_req.start_offset = nfound << g_block_size_shift;
            bmap_req.blk_count = nblocks - nfound;
            int ret = bmap(ip, &bmap_req);

            mlfs_assert(ret != -EIO);

            nfound += bmap_req.blk_count_found;
            nsearch++;
        }

        total_blocks += nblocks;
        total_fragments += nsearch;
    }

    g_perf_stats.n_files = total_files;
    g_perf_stats.n_fragments = total_fragments;
    g_perf_stats.n_blocks = total_blocks;

    double blocks_per_file = (double)total_blocks / (double)total_files;
    double fragments_per_file = (double)total_fragments / (double)total_files;
    double blocks_optimal_per_file = (blocks_per_file + 1.0) - fragments_per_file;

    g_perf_stats.layout_score_derived = fmin(blocks_optimal_per_file / blocks_per_file, 1.0);
}

static void print_fragmentation(void) {
    printf("FRAGMENTATION CALCULATION\n");
    printf("---- # of files:             %lu\n", g_perf_stats.n_files);
    printf("---- # of blocks (total):    %lu\n", g_perf_stats.n_blocks);
    printf("---- # of fragments (total): %lu\n", g_perf_stats.n_fragments);
    printf("---- OVERALL LAYOUT SCORE:   %.2f\n", g_perf_stats.layout_score_derived);
}

// Bitmap bits per block
#define BPB           (g_block_size_bytes*8)

// Block of free map containing bit for block b
#define BBLOCK(b, disk_sb) (b/BPB + disk_sb.bmap_start)

#ifdef __cplusplus
}
#endif

#endif
