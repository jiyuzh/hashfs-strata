#include "indexing_api_interface.h"
#include "storage/storage.h"

#if 0
#define trace_me() \
    fflush(stdout); \
    do { \
        fprintf(stdout, "[%s():%d] trace\n",  \
                __func__, __LINE__); \
        fflush(stdout); \
    } while (0)
#else
#define trace_me() 0
#endif

mem_man_fns_t strata_mem_man = {
    .mm_malloc = malloc,
    .mm_free   = free
};

ssize_t nvm_write(paddr_t blk, off_t off, size_t nbytes, const char* buf) {
    trace_me();
#ifdef LIBFS
    panic("shouldn't get here!\n");
#endif
    return dax_write_unaligned(g_root_dev, (uint8_t*)buf, blk, off, nbytes);
}

ssize_t nvm_read(paddr_t blk, off_t off, size_t nbytes, char* buf) {
    trace_me();
#ifdef STORAGE_PERF
    uint64_t tsc_begin = asm_rdtscp();
#endif
    ssize_t ret = dax_read_unaligned(g_root_dev, buf, blk, off, nbytes);
#ifdef STORAGE_PERF
    g_perf_stats.path_storage_tsc += asm_rdtscp() - tsc_begin;
    g_perf_stats.path_storage_nr++;
#endif
    return ret;
}

extern uint8_t *dax_addr[];

ssize_t nvm_get_addr(paddr_t blk, off_t off, char** buf) {
    trace_me();
#ifdef STORAGE_PERF
    uint64_t tsc_begin = asm_rdtscp();
#endif
    *buf = dax_addr[g_root_dev] + (blk * g_block_size_bytes) + off;
#if 0
    if (blk >= disk_sb[g_root_dev].inode_start && 
        (blk + (off / g_block_size_bytes) < 
            (disk_sb[g_root_dev].inode_start + (disk_sb[g_root_dev].ninodes*sizeof(struct dinode)/g_block_size_bytes)))) {

        paddr_t blk_from_start = blk - disk_sb[g_root_dev].inode_start;
        paddr_t off_from_start = off + (blk_from_start * g_block_size_bytes);
        int inum = off_from_start / sizeof(struct dinode); 
        struct inode *ip = icache_find(g_root_dev, inum);
        if (ip) {
            *buf = (char*)ip->l1.addrs;
        }
    }
#endif
#ifdef STORAGE_PERF
    g_perf_stats.path_storage_tsc += asm_rdtscp() - tsc_begin;
    g_perf_stats.path_storage_nr++;
#endif
    return 0;
}

pthread_mutex_t alloc_mutex = PTHREAD_MUTEX_INITIALIZER; 

static inline void balloc_lock(void) {
    int err = pthread_mutex_lock(&alloc_mutex);
    if_then_panic(err, "Could not lock! %s\n", strerror(err));
}

static inline void balloc_unlock(void) {
    int err = pthread_mutex_unlock(&alloc_mutex);
    if_then_panic(err, "Could not unlock! %s\n", strerror(err));
}

static inline ssize_t alloc_generic(size_t nblk,
                                    paddr_t* pblk,
                                    enum alloc_type a_type) {
    trace_me();

#if defined(STORAGE_PERF) && defined(KERNFS)
    uint64_t tsc_begin = asm_rdtscp();
#endif

    balloc_lock();
    struct super_block *sblk = sb[g_root_dev];

    int r = mlfs_new_blocks(sblk, pblk, nblk, 0, 0, a_type, 0);
    if (r > 0) {
#ifdef KERNFS
        balloc_undo_log(*pblk, r, 0);
#endif
        bitmap_bits_set_range(sblk->s_blk_bitmap, *pblk, r);
        sblk->used_blocks += r;
    } else if (r == -ENOSPC) {
        balloc_unlock();
        panic("Failed to allocate block -- no space!\n");
    } else if (r == -EINVAL) {
        balloc_unlock();
        panic("Failed to allocate block -- invalid arguments!\n");
    } else {
        balloc_unlock();
        panic("Failed to allocate block -- unknown error!\n");
    }

    balloc_unlock();

#if defined(STORAGE_PERF) && defined(KERNFS)
    if (enable_perf_stats) {
        g_perf_stats.balloc_tsc += asm_rdtscp() - tsc_begin;
        g_perf_stats.balloc_nblk += r;
        g_perf_stats.balloc_nr++;
    }
#endif

    return (ssize_t)r;
}

ssize_t alloc_metadata_blocks(size_t nblocks, paddr_t* pblk) {
    trace_me();
    return alloc_generic(nblocks, pblk, TREE);
}

ssize_t alloc_data_blocks(size_t nblocks, paddr_t *pblk) {
    trace_me();
    return alloc_generic(nblocks, pblk, DATA);
}


static inline ssize_t dealloc_generic(size_t nblk, paddr_t pblk) {
    trace_me();

#ifdef STORAGE_PERF
    uint64_t tsc_begin = asm_rdtscp();
#endif

    balloc_lock();

    struct super_block *sblk = sb[g_root_dev];
    int ret = mlfs_free_blocks_node(sblk, pblk, nblk, 0, 0);
    if (ret == 0) {
#ifdef KERNFS
        balloc_undo_log(pblk, nblk, 1);
#endif
        bitmap_bits_free(sblk->s_blk_bitmap, pblk, nblk);
        sblk->used_blocks -= nblk;
        ret = nblk;
    }

    balloc_unlock();
    
#if defined(STORAGE_PERF) && defined(KERNFS)
    if (enable_perf_stats) {
        g_perf_stats.balloc_tsc += asm_rdtscp() - tsc_begin;
        g_perf_stats.balloc_nblk += ret;
        g_perf_stats.balloc_nr++;
    }
#endif

    return (ssize_t) ret;
}

ssize_t dealloc_metadata_blocks(size_t nblocks, paddr_t pblk) {
    trace_me();
    return dealloc_generic(nblocks, pblk);
}

ssize_t dealloc_data_blocks(size_t nblocks, paddr_t pblk) {
    trace_me();
    return dealloc_generic(nblocks, pblk);
}

int get_dev_info(device_info_t* di) {
    trace_me();
    di->di_block_size  = g_block_size_bytes;
    di->di_size_blocks = sb[g_root_dev]->ondisk->ndatablocks;
    return 0;
}

int log_change(inum_t inum, void *nvm_addr, size_t nbytes) {

}

callback_fns_t strata_callbacks = {
    .cb_write            = nvm_write,
    .cb_read             = nvm_read,
    .cb_get_addr         = nvm_get_addr,
    .cb_alloc_metadata   = alloc_metadata_blocks,
    .cb_alloc_data       = alloc_data_blocks,
    .cb_dealloc_metadata = dealloc_metadata_blocks,
    .cb_dealloc_data     = dealloc_data_blocks,
    .cb_get_dev_info     = get_dev_info,
    .cb_log_change       = log_change
};

idx_spec_t strata_idx_spec = {
    .idx_callbacks = &strata_callbacks,
    .idx_mem_man   = &strata_mem_man
};
