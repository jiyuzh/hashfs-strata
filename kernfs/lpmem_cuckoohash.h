#ifndef __NVM_IDX_CUCKOO_HASH__
#define __NVM_IDX_CUCKOO_HASH__ 1

#include <malloc.h>
#include <memory.h>
#include <string.h>

#include "common/common.h"
#include "lpmem_cuckoo_hash_impl.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAKEKEY(inum, key) (((uint64_t)inum << 32) | key)

typedef paddr_t hash_key_t;

//extern idx_fns_t cuckoohash_fns;

/*
 * Generic hash table functions.
 */



int pmem_cuckoohash_initialize();

ssize_t pmem_cuckoohash_create(inum_t inum, paddr_t lblk, paddr_t *new_paddr);

ssize_t pmem_cuckoohash_lookup(inum_t inum, paddr_t lblk, paddr_t* paddr);

ssize_t pmem_cuckoohash_remove(inum_t inum, paddr_t lblk, size_t size);

// int cuckoohash_set_caching(idx_struct_t *idx_struct, bool enable);
// int cuckoohash_set_locking(idx_struct_t *idx_struct, bool enable);
// int cuckoohash_persist_updates(idx_struct_t *idx_struct);
// int cuckoohash_invalidate_caches(idx_struct_t *idx_struct);

// void cuckoohash_print_global_stats(void);

#ifdef __cplusplus
}
#endif

#endif  // __NVM_IDX_CUCKOO_HASH__
