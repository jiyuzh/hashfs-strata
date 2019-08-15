/*
  Copyright (C) 2010 Tomash Brechko.  All rights reserved.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __NVM_IDX_CUCKOO_HASH_IMPL__
#define __NVM_IDX_CUCKOO_HASH_IMPL__ 1

#include <stddef.h>
#include <stdbool.h>

#include <libpmem.h>
#include <stdatomic.h>
#include <immintrin.h>
#include "fs.h"
#include "storage/storage.h"
#include "global/global.h"
#include "shared.h"

#include "common/common.h"

#ifdef __cplusplus
extern "C" {
#define _Static_assert static_assert
#endif  /* __cplusplus */

#define CUCKOO_HASH_FAILED  ((void *) -1)

// typedef struct pmem_cuckoo_hash_item {
//     paddr_t key;
//     paddr_t value;
//     uint32_t index;
//     uint32_t range;
// } pmem_cuckoo_item_t;

extern uint8_t *dax_addr[];

typedef struct pmem_cuckoo_hash_elem {
    paddr_t key;
    uint32_t hash1;
    uint32_t hash2;
} pmem_cuckoo_elem_t;

typedef struct pmem_nvm_cuckoo_volatile_metadata {
    pmem_cuckoo_elem_t *entries;
} pmem_nvm_cuckoo_vol_t;


#define sz sizeof(pmem_cuckoo_elem_t)

_Static_assert(sz <= 64, "Must be <= cache line size!");
_Static_assert(sz == 1 || !(sz & (sz - 1)) , "Must be a power of 2!");

#undef sz

#define CUCKOO_MAGIC 0xcafebabe
typedef struct pmem_cuckoo_meta {
    int is_pmem;
    uint32_t meta_size;
    uint32_t magic;
    paddr_t entries_blk; // elem_start_blk;
    size_t max_size;
} pmem_cuckoo_metadata_t;

typedef struct pmem_cuckoo_hash {
    pmem_cuckoo_metadata_t meta;
    // cuckoo_elem_t *table;
    bool do_stats;
} pmem_nvm_cuckoo_idx_t;

typedef struct pmem_cuckoo_hash_stats {
    uint64_t ncachelines_written;
    uint64_t nblocks_inserted;
    uint64_t nwrites;
} pmem_nvm_cuckoo_stats_t;

extern pmem_nvm_cuckoo_stats_t cstats;

/*
  cuckoo_hash_init(hash, power):

  Initialize the hash.  power controls the initial hash table size,
  which is (bin_size << power), i.e., 4*2^power.  Zero means one.

  Return 0 on success, -errno if initialization failed.
*/
int
pmem_cuckoo_hash_init(struct disk_superblock *sblk);


/*
  cuckoo_hash_destroy(hash):

  Destroy the hash, i.e., free memory.
*/
void
pmem_cuckoo_hash_destroy();

void
pmem_cuckoo_hash_close();


/*
  cuckoo_hash_count(hash):

  Return number of elements in the hash.
*/
static inline
size_t
pmem_cuckoo_hash_count()
{
  return 0;
}


/*
  cuckoo_hash_insert(hash, key, key_len, value):

  Insert new value into the hash under the given key.

  Return NULL on success, or the pointer to the existing element with
  the same key, or the constant CUCKOO_HASH_FAILED when operation
  failed (memory exhausted).  If you want to replace the existing
  element, assign the new value to result->value.  You likely will
  have to free the old value, and a new key, if they were allocated
  dynamically.
*/
int
pmem_cuckoo_hash_insert(paddr_t key, paddr_t *paddr);


/*
  cuckoo_hash_lookup(hash, key, key_len):

  Lookup given key in the hash.

  Return pointer to struct cuckoo_hash_item, or NULL if the key
  doesn't exist in the hash.
*/
int
pmem_cuckoo_hash_lookup(paddr_t key, paddr_t *value);

/*
  cuckoo_hash_lookup(hash, key, key_len):

  Lookup given key in the hash.

  Return pointer to struct cuckoo_hash_item, or NULL if the key
  doesn't exist in the hash.
*/
int
pmem_cuckoo_hash_update(paddr_t key, uint32_t size);

/*
  cuckoo_hash_remove(hash, hash_item):

  Remove the element from the hash that was previously returned by
  cuckoo_hash_lookup() or cuckoo_hash_next().

  It is safe to pass NULL in place of hash_item, so you may write
  something like

    cuckoo_hash_remove(hash, cuckoo_hash_lookup(hash, key, key_len));

  hash_item passed to cuckoo_hash_remove() stays valid until the next
  call to cuckoo_hash_insert() or cuckoo_hash_destroy(), so if you
  allocated the key and/or value dynamically, you may free them either
  before or after the call (they won't be referenced inside):

    struct cuckoo_hash_item *item = cuckoo_hash_lookup(hash, k, l);
    free((void *) item->key);
    cuckoo_hash_remove(hash, item);
    free(item->value);

  (that (void *) cast above is to cast away the const qualifier).
*/
int
pmem_cuckoo_hash_remove(paddr_t key, uint32_t *index);

#ifdef __cplusplus
}      /* extern "C" */
#endif  /* __cplusplus */


#endif  /* ! _CUCKOO_HASH_H */
