/* GLIB - Library of useful routines for C programming
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Modified by the GLib Team and others 1997-2000.  See the AUTHORS
 * file for a list of people on the GLib Team.  See the ChangeLog
 * files for a list of changes.  These files are distributed with
 * GLib at ftp://ftp.gtk.org/pub/gtk/.
 */

#ifndef __PMEM_NVM_IDX_G_HASH_MOD_H__
#define __PMEM_NVM_IDX_G_HASH_MOD_H__ 1

#ifdef __cplusplus
extern "C" {
#define _Static_assert static_assert
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <libpmem.h>
#include <stdatomic.h>
#include <immintrin.h>

#include "fs.h"
#include "storage/storage.h"
#include "global/global.h"
#include "shared.h"

// from common
#include "common/common.h"

// local includes
#include "hash_functions.h"

#define HASHFS_TOMBSTONE_VAL ((paddr_t)~0) - 1
#define HASHFS_EMPTY_VAL (paddr_t)~0
#define HASHFS_ENT_IS_TOMBSTONE(x) (x == ((paddr_t)~0) - 1)
#define HASHFS_ENT_IS_EMPTY(x) (x == (paddr_t)~0)
#define HASHFS_ENT_IS_VALID(x) (!HASHFS_ENT_IS_EMPTY(x) && !HASHFS_ENT_IS_TOMBSTONE(x))

#define HASHFS_ENT_SET_TOMBSTONE(x) (x = ((paddr_t)~0) - 1)
#define HASHFS_ENT_SET_EMPTY(x) (x = (paddr_t)~0)
#define HASHFS_ENT_SET_VAL(x,v) (x = v)

#define HASHFS_MAKEKEY(inum, lblk) (((uint64_t)inum << 32) | lblk)
/*
#define HASHCACHE

This is the hash table meta-data that is persisted to NVRAM, that we may read
it and know everything we need to know in order to reconstruct it in memory.
typedef struct device_hashtable_metadata {
  // Metadata for the in-memory state.
  uint32_t size;
  uint32_t mod;
  uint32_t mask;
  uint32_t nnodes;
  uint32_t noccupied;
  // Metadata about the on-disk state.
  size_t  blksz;
  paddr_t nvram_size;
  paddr_t range_size;
  paddr_t data_start;
} dev_hash_metadata_t;

typedef struct hashtable_stats {
    uint64_t n_lookups;
    uint64_t n_min_ents_per_lookup;
    uint64_t n_max_ents_per_lookup;
    uint64_t n_ents;
    STAT_FIELD(loop_time);
} hash_stats_t;

static void print_hashtable_stats(hash_stats_t *s) {
    printf("hashtable stats: \n");
    printf("\tAvg. collisions: %.2f\n", (double)s->n_ents / (double)s->n_lookups);
    printf("\tMin. collisions: %llu\n", s->n_min_ents_per_lookup);
    printf("\tMax. collisions: %llu\n", s->n_max_ents_per_lookup);
    PFIELD(s, loop_time);
}
*/
//need to include global.h, shared.h, fs.h
typedef struct pmem_nvm_hashtable_volatile_metadata {
	paddr_t *entries;
	hash_func_t hash_func;
} pmem_nvm_hash_vol_t;

typedef struct pmem_nvm_hashtable_index {
    int             is_pmem;
    int             valid;
    int 	    meta_size;
    int             size;
    int             mod;
    unsigned        mask;
    int             nnodes;
    int             noccupied;  /* nnodes + tombstones */

    paddr_t entries_blk; /* persists */
    //paddr_t *entries; /* run-time */
    uint64_t num_entries;
    //paddr_t         metadata;
    //paddr_t         data;
    //char           *data_ptr;
    //paddr_t         nvram_size;
    //size_t          blksz;
    //size_t          range_size;
    //size_t          nblocks;
    //hash_func_t      hash_func;
    //int              ref_count;

    // callbacks
    //callback_fns_t *idx_callbacks;
    //mem_man_fns_t  *idx_mem_man;

    // device infomation
    //device_info_t  *devinfo;

    // concurrency
    //pthread_rwlock_t *locks;
    //pthread_mutex_t *metalock;

    // -- CACHING
    //bool do_lock;
    //bool do_cache;
    //pthread_rwlock_t *cache_lock;
    //int8_t* cache_state;
    // array of entries
    //hash_ent_t *cache;

    // -- STATS
    //bool enable_stats;
    //hash_stats_t stats;
} pmem_nvm_hash_idx_t;

extern uint8_t *dax_addr[];
extern pmem_nvm_hash_idx_t *pmem_ht;



void
pmem_nvm_hash_table_new (struct disk_superblock *sblk,
                    hash_func_t       hash_func
                    );

void
pmem_nvm_hash_table_close ();

int pmem_nvm_hash_table_insert(inum_t         inum,
                          paddr_t             lblk,
                          paddr_t         *index
                          );

int pmem_nvm_hash_table_remove(inum_t         inum,
                          paddr_t             lblk,
                          paddr_t        *value);

int pmem_nvm_hash_table_lookup(inum_t inum, paddr_t lblk,
    paddr_t *val);

int pmem_nvm_hash_table_contains(inum_t inum, paddr_t lblk);

unsigned pmem_nvm_hash_table_size();

int pmem_nvm_hash_table_insert_simd(uint32_t inum, uint32_t lblk, uint32_t len, uint64_t *pblks);
int pmem_nvm_hash_table_lookup_simd(uint32_t inum, uint32_t lblk, uint32_t len, uint64_t *pblks);
int pmem_nvm_hash_table_remove_simd(uint32_t inum, uint32_t lblk, uint32_t len);


extern uint64_t reads;
extern uint64_t writes;
extern uint64_t blocks;


#ifdef __cplusplus
}
#endif

#endif /* __NVM_IDX_G_HASH_MOD_H__ */
