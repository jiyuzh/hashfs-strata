#include "undo_log.h"

#ifndef KERNFS
    #define KERNFS_CHECK() panic("Only for kernfs!\n")
#else
    #define KERNFS_CHECK() 0
#endif

extern uint8_t *dax_addr[];

#define get_addr(blk, off) (void*)(dax_addr[g_root_log_dev] + (blk * g_block_size_bytes) + off)

#define offset(p1, p2) (((char*)(p1)) - ((char*)(p2)))

/**
 * The current pointer where the next log entry will go. Access atomically.
 */
static void *curp = NULL;
static uint64_t logsz;

/**
 * Treats the log like a big circular buffer.
 */
static inline void _incr_log_ptr(void *ptr, size_t sz) {
    void *nextp = ptr + sz;
    if (nextp - get_addr(0, 0) > logsz) {
        nextp -= logsz;
    }

    curp = nextp;
}

static inline void* _get_next_aligned(size_t sz) {
    void *current = curp; 
    void *next = curp + sz;
    
    if (next - get_addr(0, 0) > logsz) {
        next -= logsz;
    }

    return next;
}

#define incr_curp(sz) _incr_log_ptr((void*)curp, sz)
#define set_next(v) v = (typeof(v))_get_next_aligned(sizeof(*v)); incr_curp(sizeof(*v))

static mlfs_undo_meta_t *startp = NULL;
static mlfs_undo_meta_t *commitp = NULL;
static bool tx_in_progress;

static int recover_undo_log(mlfs_undo_meta_t *startp);

int init_undo_log(void) {
    KERNFS_CHECK();
    int err;

    mlfs_undo_meta_t *rootp = (mlfs_undo_meta_t*)get_addr(0, 0);
    logsz = dev_size[g_root_log_dev];

    int nstart = 0, ncommit = 0;
    
    // Find the current pointer and ensure consistency.
    for (mlfs_undo_meta_t *mp = rootp; offset(mp, rootp) < logsz; ++mp) {
        if (mp->mb_type == LOG_UNINITIALIZED) {
            curp = mp;
            break;
        } else if (mp->mb_type == LOG_START) {
            startp = mp;
            ++nstart;
        } else if (mp->mb_type == LOG_COMMIT) {
            commitp = mp;
            ++ncommit;
        }
    }

    // Inconsistent state.
    if (ncommit > nstart || nstart - ncommit > 1) {
        fprintf(stderr, "ncommit == %d, nstart == %d\n", ncommit, nstart);
        panic("Inconsistent undo log---there's a bug somewhere!");
    }

    // Check if we need to repair.
    if (ncommit < nstart) {
        fprintf(stderr, "ncommit == %d, nstart == %d\n", ncommit, nstart);
        err = recover_undo_log(startp);
        if (err) goto abort;
    }

    // After repair, we're good to go.

    return 0;

abort:
    fprintf(stderr, "Could not init log: errno %d, '%s'\n", -err, strerror(-err));
    return err;
}

int shutdown_undo_log(void) {
    KERNFS_CHECK();

    // If we actually call shutdown, all transactions should be complete.
    if (tx_in_progress) {
        panic("Inconsistent shutdown state!\n");
    }

    return 0;
}

int persist_dirty_objects_nvm(void);

int recover_undo_log(mlfs_undo_meta_t *startp) {
    KERNFS_CHECK();

    if (!startp) panic("Cannot recover from nullptr!\n");

    void *rootp = get_addr(0, 0);

    mlfs_undo_meta_t *mp;
    for (mp = startp; offset(mp, rootp) < logsz; ) {

        mlfs_undo_skip_t *sp = (mlfs_undo_skip_t*)mp;
        mlfs_balloc_undo_ent_t *bp = (mlfs_balloc_undo_ent_t*)mp;
        mlfs_idx_undo_ent_t *ip = (mlfs_idx_undo_ent_t*)mp;

        switch(mp->mb_type) {
            case LOG_START:
            case LOG_COMMIT:
                panic("If you're recovering, you shouldn't hit a commit block.");
                return -1;

            case LOG_SKIP:
                mp += sp->sb_skip_bytes;
                break;

            case LOG_BALLOC_ENTRY:
                balloc_undo_log_rollback(bp);
                mp += sizeof(*bp);
                break;

            case LOG_IDX_ENTRY:
                idx_undo_log_rollback(ip);
                mp += sizeof(*ip) + ip->idx_nbytes;
                break;

            case LOG_UNINITIALIZED:
                return 0;

            default:
                panic("Undefined value %u\n", mp->mb_type);
                return -1;
        }
    }

#ifdef KERNFS
    persist_dirty_objects_nvm();
#endif

    pmem_memset_persist(startp, 0, mp - startp);

    return 0;
} 


/*******************************************************************************
 * Common functions
 ******************************************************************************/

int undo_log_start_tx(void) {
    if (!__sync_bool_compare_and_swap(&tx_in_progress, false, true)) {
        panic("TX already in progress!\n");
    }

    // Force alignment
    set_next(startp);

    startp->mb_type = LOG_START;
    nvm_persist_struct_ptr(startp);

    return 0;
}

int undo_log_commit_tx(void) {
    if (!__sync_bool_compare_and_swap(&tx_in_progress, true, false)) {
        panic("TX already ended or never started!\n");
    }

    // Force alignment
    set_next(commitp);

    commitp->mb_type = LOG_COMMIT;
    nvm_persist_struct_ptr(commitp);

    return 0;
}

/*******************************************************************************
 * Block allocator functions
 ******************************************************************************/

int balloc_undo_log(paddr_t start_block, uint32_t nblk, char orig_val) {
    mlfs_balloc_undo_ent_t *ent;

#ifdef KERNFS
    uint64_t start_tsc = asm_rdtscp();
#endif

    set_next(ent);

    unsigned status = 0;

    if ((status = _xbegin ()) == _XBEGIN_STARTED) {
        ent->mb_type     = LOG_BALLOC_ENTRY;
        ent->mb_start    = start_block;
        ent->mb_nblk     = nblk;
        ent->mb_orig_val = orig_val;

        _xend ();
        nvm_persist_struct_ptr(ent);
    } else {
        ent->mb_start    = start_block;
        ent->mb_nblk     = nblk;
        ent->mb_orig_val = orig_val;
        nvm_persist_struct_ptr(ent);

        ent->mb_type     = LOG_BALLOC_ENTRY;
        nvm_persist_struct_ptr(ent);
    }

#ifdef KERNFS
    if (enable_perf_stats) {
        g_perf_stats.undo_tsc += asm_rdtscp() - start_tsc;
        g_perf_stats.undo_nr++;
    }
#endif

    return 0;
}

int balloc_undo_log_rollback(mlfs_balloc_undo_ent_t *ent) {
    uint64_t start_tsc;

    if (enable_perf_stats) start_tsc = asm_rdtscp();

    struct super_block *sblk = sb[g_root_dev];
    unsigned status = 0;

    if (ent->mb_orig_val) {
        bitmap_bits_free(sblk->s_blk_bitmap, ent->mb_start, ent->mb_nblk);
    } else {
        bitmap_bits_set_range(sblk->s_blk_bitmap, ent->mb_start, ent->mb_nblk);
    }

    return 0;
}

/*******************************************************************************
 * Indexing structure functions
 ******************************************************************************/
int idx_undo_log(uint64_t dev_byte_offset, size_t nbytes, void *nvm_ptr) {
    mlfs_idx_undo_ent_t *ent;

#ifdef KERNFS
    uint64_t start_tsc = asm_rdtscp();
#endif

    set_next(ent);
    void *nvm_loc = curp;
    incr_curp(nbytes);

    // Flush the data first
    pmem_memcpy_persist(nvm_loc, nvm_ptr, nbytes);

    // Then the other metadata
    ent->idx_nbytes      = nbytes;
    ent->idx_byte_offset = dev_byte_offset;
    nvm_persist_struct_ptr(ent);

    // Treating the mb_type as a valid bit.
    ent->mb_type         = LOG_IDX_ENTRY;
    nvm_persist_struct_ptr(ent);

#ifdef KERNFS
    if (enable_perf_stats) {
        g_perf_stats.undo_tsc += asm_rdtscp() - start_tsc;
        g_perf_stats.undo_nr++;
    }
#endif

    return 0;
}

int idx_undo_log_rollback(mlfs_idx_undo_ent_t *ent) {
#ifdef KERNFS
    uint64_t start_tsc = 0;

    if (enable_perf_stats) {
        start_tsc = asm_rdtscp();
    }
#endif

    void *nvm_ptr = get_addr(0, ent->idx_byte_offset);
    void *orig_data = ((void*)ent) + ent->idx_nbytes;
    pmem_memcpy_persist(nvm_ptr, orig_data, ent->idx_nbytes);

#ifdef KERNFS
    if (enable_perf_stats) {
        g_perf_stats.undo_tsc += asm_rdtscp() - start_tsc;
        g_perf_stats.undo_nr++;
    }
#endif

    return 0;
}

/*******************************************************************************
 * Utility functions
 ******************************************************************************/
static void print_entry(const char* type, uint64_t start, uint64_t nbytes) {
    printf("[%8lu <> %8lu] %s\n", start, start + nbytes - 1, type);
}

int undo_log_sanity_check(bool display) {
    void *rootp = get_addr(0, 0);

    for (void *p = rootp; offset(p, rootp) < logsz; ) {
        mlfs_undo_meta_t *mp = (mlfs_undo_meta_t*)p;

        mlfs_undo_skip_t *sp = (mlfs_undo_skip_t*)mp;
        mlfs_balloc_undo_ent_t *bp = (mlfs_balloc_undo_ent_t*)mp;
        mlfs_idx_undo_ent_t *ip = (mlfs_idx_undo_ent_t*)mp;

        switch(mp->mb_type) {
            case LOG_START:
            case LOG_COMMIT:
                print_entry(mp->mb_type == LOG_START ? "START TX" : "COMMIT TX", 
                        offset(p, rootp), sizeof(*mp));
                p += sizeof(*mp);
                break;

            case LOG_SKIP:
                print_entry("-align-", offset(p, rootp), sp->sb_skip_bytes);
                p += sp->sb_skip_bytes;
                break;

            case LOG_BALLOC_ENTRY:
                print_entry("BALLOC", offset(p, rootp), sizeof(*bp));
                p += sizeof(*bp);
                break;

            case LOG_IDX_ENTRY:
                print_entry("IDX UPDATE", offset(p, rootp), sizeof(*ip) + ip->idx_nbytes);
                p += sizeof(*ip) + ip->idx_nbytes;

            case LOG_UNINITIALIZED:
                return 0;

            default:
                panic("Undefined value %u\n", mp->mb_type);
        }
    }

    return -1;
}
