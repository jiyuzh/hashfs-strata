#ifndef _UNDO_LOG_H_
#define _UNDO_LOG_H_

#ifdef __cplusplus
#define _Static_assert static_assert
extern "C" {
#endif

#include <immintrin.h>

#include "shared.h"
#include "storage/storage.h"
#include "fs.h"
#include "balloc.h"

/**
 *
 * (iangneal): We need this for accurate indexing update performance numbers...
 * and of course, correctness.
 *
 * This will be the physicalized log (required for correctness) that will be used
 * during digest requests. It will log the following:
 *
 * 1) Block allocations/deallocations.
 * 2) Indexing structure modifications that are not atomic.
 * 3) [MAYBE] Inode allocations/deallocations (although this may be done by the
 *      LibFS side, I don't recall). [UPDATE] Currently allocated by LibFS.
 *
 * The structure of the log should be as follows:
 *
 *  [DIGEST BEGIN]
 *  [UNDO ALLOCATION]
 *  [UNDO STRUCTURE GROWTH]
 *  ...
 *  [DIGEST END]
 *
 *  The size of each one of these entries should be a cacheline in size. Ergo,
 *  we can build an atomic log via TSX instructions.
 *
 *  For recovery, the protocol should be:
 *
 *  1) If [DIGEST END] is present, do nothing.
 *  2) If [DIGEST END] is NOT present, execute all undos, then replay from the 
 *      application log. For this, for indexing structures, you would need an
 *      option were you could update/override entries, because if an insert
 *      goes through, then you undo the allocations and play it back, you may
 *      technically get different blocks with concurrent digests. But we can
 *      worry about this later.
 *
 * I'll use a third DAX device for this undo log.
 *
 */

typedef enum mlfs_undo_meta_type {
    LOG_UNINITIALIZED = 0,
    LOG_START,
    LOG_COMMIT,
    // An entry that denotes an entry left blank for alignment's sake
    LOG_SKIP,
    // For actual log entries
    LOG_BALLOC_ENTRY,
    LOG_IDX_ENTRY,
} mlfs_undo_meta_type_t;

typedef struct mlfs_undo_meta_block {
    mlfs_undo_meta_type_t mb_type;
    uint64_t mb_next_byte_offset;
    uint8_t _padding[48];
} mlfs_undo_meta_t;

typedef struct mlfs_undo_skip_block {
    mlfs_undo_meta_type_t mb_type;
    uint64_t sb_skip_bytes;
} mlfs_undo_skip_t;

_Static_assert(sizeof(mlfs_undo_meta_t) == 64, "must be cache line size!");

int init_undo_log(void);

int shutdown_undo_log(void);

int undo_log_sanity_check(bool display);

int undo_log_start_tx(void);

int undo_log_commit_tx(void);

/*******************************************************************************
 * Block allocator interface
 ******************************************************************************/

typedef struct mlfs_balloc_undo_ent {
    mlfs_undo_meta_type_t mb_type;
    uint64_t mb_start;
    uint32_t mb_nblk;
    uint32_t mb_orig_val;
} mlfs_balloc_undo_ent_t;

_Static_assert(sizeof(mlfs_balloc_undo_ent_t) < 64, "must be smaller than cache line!");
_Static_assert(sizeof(mlfs_balloc_undo_ent_t) % 2 == 0, "must be a power of 2!");

/**
 * start_block: which is the first block in the bitmap being modified.
 * nblk: Number of blocks (ergo entries) being changed.
 * orig_val: The original state of the blocks. This is indicated so that this 
 *      function can be used for allocations (orig_val=0) and deallocations
 *      (orig_val=1).
 */
int balloc_undo_log(paddr_t start_block, uint32_t nblk, char orig_val);

/**
 * Undo the changes to the block allocator bitmap. It is later persisted to a
 * call to persist_dirty_objects_nvm() in the recover_undo_log() function.
 */
int balloc_undo_log_rollback(mlfs_balloc_undo_ent_t *ent);

/*******************************************************************************
 * Indexing structure interface
 ******************************************************************************/

typedef struct mlfs_idx_struct_undo_ent {
    mlfs_undo_meta_type_t mb_type;
    uint64_t idx_byte_offset;
    size_t idx_nbytes;
} mlfs_idx_undo_ent_t;

_Static_assert(sizeof(mlfs_idx_undo_ent_t) <= 64, "must be smaller than cache line!");
_Static_assert(sizeof(mlfs_idx_undo_ent_t) % 2 == 0, "must be a power of 2!");

/** 
 * Log the original content of the extent tree node before committing changes,
 * so that if there is a crash before the end of the digest, we can recover
 * and replay the digest.
 */
int idx_undo_log(uint64_t dev_byte_offset, size_t nbytes, void *nvm_ptr);

/**
 * Apply the original values.
 */
int idx_undo_log_rollback(mlfs_idx_undo_ent_t *ent);

#ifdef __cplusplus
}
#endif

#endif
