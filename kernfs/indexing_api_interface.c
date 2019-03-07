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
    printf("shouldn't get here!\n");
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

static inline ssize_t alloc_generic(size_t nblk,
                                    paddr_t* pblk,
                                    enum alloc_type a_type) {
    trace_me();
    struct super_block *sblk = sb[g_root_dev];

    int r = mlfs_new_blocks(sblk, pblk, nblk, 0, 0, a_type, 0);
    if (r > 0) {
      bitmap_bits_set_range(sblk->s_blk_bitmap, *pblk, r);
      sblk->used_blocks += r;
    } else if (r == -ENOSPC) {
      panic("Failed to allocate block -- no space!\n");
    } else if (r == -EINVAL) {
      panic("Failed to allocate block -- invalid arguments!\n");
    } else {
      panic("Failed to allocate block -- unknown error!\n");
    }

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
    struct super_block *sblk = sb[g_root_dev];
    int ret = mlfs_free_blocks_node(sblk, pblk, nblk, 0, 0);
    if (ret == 0) {
        bitmap_bits_free(sblk->s_blk_bitmap, pblk, nblk);
        sblk->used_blocks -= nblk;
        ret = nblk;
    }

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

callback_fns_t strata_callbacks = {
    .cb_write            = nvm_write,
    .cb_read             = nvm_read,
    .cb_alloc_metadata   = alloc_metadata_blocks,
    .cb_alloc_data       = alloc_data_blocks,
    .cb_dealloc_metadata = dealloc_metadata_blocks,
    .cb_dealloc_data     = dealloc_data_blocks,
    .cb_get_dev_info     = get_dev_info
};

idx_spec_t strata_idx_spec = {
    .idx_callbacks = &strata_callbacks,
    .idx_mem_man   = &strata_mem_man
};
