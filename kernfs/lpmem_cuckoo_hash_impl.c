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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "lpmem_cuckoo_hash_impl.h"

pmem_nvm_cuckoo_stats_t cstats = {0,}; 
pmem_nvm_cuckoo_idx_t *pmem_cuckoo = NULL;
pmem_nvm_cuckoo_vol_t *pmem_cuckoo_vol = NULL;

#define CUCKOO_SET_EMPTY(x) (x = (paddr_t) ~0)
#define CUCKOO_IS_EMPTY(x) (x == (paddr_t) ~0)

static inline
void
pmem_compute_hash(paddr_t key, uint32_t *h1, uint32_t *h2)
{
#if 0
    extern void hashlittle2(const void *key, size_t length,
                            uint32_t *pc, uint32_t *pb);

    /* Initial values are arbitrary.  */
    *h1 = 0x3ac5d673;
    *h2 = 0x6d7839d0;
    // TODO: might be inefficient
    hashlittle2(&key, sizeof(key), h1, h2);
#else
    *h1 = (uint32_t)key;
    *h2 = ~*h1;
#endif
    if (*h1 == *h2) {
        *h2 = ~*h2;
    }
}

static void pmem_nvm_cuckoo_flush(void* start, void* len) {
    if(pmem_cuckoo->meta.is_pmem) {
      pmem_persist(start, len);
    }
    else {
      pmem_msync(start, len);
    }
}

int
pmem_cuckoo_hash_init(struct disk_superblock *sblk)
{
    printf("inside cuckoo_new\n");
    pmem_cuckoo = dax_addr[g_root_dev] + (sblk->datablock_start * g_block_size_bytes);

    // Read metadata to see if it exists or not.
    if(pmem_cuckoo->meta.magic == CUCKOO_MAGIC) {
        printf("cuckoo exists\n");
        // pmem_cuckoo->meta.magic = ~CUCKOO_MAGIC;
        // pmem_nvm_cuckoo_flush(&(pmem_cuckoo->meta.magic), sizeof(int));
        pmem_cuckoo_vol = (pmem_nvm_cuckoo_vol_t *)malloc(sizeof(pmem_nvm_cuckoo_vol_t));
        pmem_cuckoo_vol->entries = dax_addr[g_root_dev] + (pmem_cuckoo->meta.entries_blk * g_block_size_bytes);
        // pmem_cuckoo_vol->meta.magic = CUCKOO_MAGIC;
        // pmem_nvm_cuckoo_flush(&(pmem_cuckoo->meta.magic), sizeof(int));
        return 0; 
    }

    printf("cuckoo does not exist\n");
    uint64_t ent_num_bytes = sizeof(pmem_cuckoo_elem_t) * sblk->ndatablocks;
    int ent_num_blocks_needed = 1 + ent_num_bytes / g_block_size_bytes;
    if(ent_num_bytes % g_block_size_bytes != 0) {
        ++ent_num_blocks_needed;
    }
    pmem_cuckoo->meta.meta_size = ent_num_blocks_needed;
    pmem_cuckoo->meta.entries_blk = sblk->datablock_start + 1;
    pmem_cuckoo->meta.max_size = sblk->ndatablocks - ent_num_blocks_needed;

    pmem_cuckoo_vol = (pmem_nvm_cuckoo_vol_t *)malloc(sizeof(pmem_nvm_cuckoo_vol_t));
    pmem_cuckoo_vol->entries = dax_addr[g_root_dev] + (pmem_cuckoo->meta.entries_blk * g_block_size_bytes);

    pmem_cuckoo->meta.is_pmem = pmem_is_pmem(pmem_cuckoo, ent_num_blocks_needed * g_block_size_bytes);
    memset(pmem_cuckoo_vol->entries, ~0, (ent_num_blocks_needed - 1) * g_block_size_bytes);

    pmem_nvm_cuckoo_flush(pmem_cuckoo, ent_num_blocks_needed * g_block_size_bytes);
    pmem_cuckoo->meta.magic = CUCKOO_MAGIC;
    pmem_nvm_cuckoo_flush(&(pmem_cuckoo->meta.magic), sizeof(int));

    return 0;
}

void
pmem_cuckoo_hash_destroy()
{
    if_then_panic(true, "Can't be in here!\n");
}

void
pmem_cuckoo_hash_close()
{
    free(pmem_cuckoo_vol);
}

#if 0
static inline
struct cuckoo_hash_elem *
bin_at(const struct cuckoo_hash *hash, uint32_t index)
{
    return hash->table + index;
}
#else
#define bin_at(hash, index) (hash)->table + (index)
#endif

void elem_at(uint32_t index, pmem_cuckoo_elem_t *ret) {
    *ret = pmem_cuckoo_vol->entries[index];
}

static inline
paddr_t
pmem_lookup(paddr_t key, uint32_t h1, uint32_t h2)
{
    uint32_t mod = pmem_cuckoo->meta.max_size;

    pmem_cuckoo_elem_t elem;
    elem_at(h1 % mod, &elem);

    if (elem.key == key)
    {
        return (h1 % mod) + pmem_cuckoo->meta.meta_size;
    }

    elem_at(h2 % mod, &elem);
    if (elem.key == key)
    {
        return (h2 % mod) + pmem_cuckoo->meta.meta_size;
    }

    return 0;
}


int
pmem_cuckoo_hash_lookup(paddr_t key, paddr_t *value)
{

    uint32_t h1, h2;
    pmem_compute_hash(key, &h1, &h2);

    *value = pmem_lookup(key, h1, h2);
    if (!(*value)) return -ENOENT;

    return 0;
}

int
pmem_cuckoo_hash_update(paddr_t key, uint32_t size)
{
    //deprecated, no ranges in this scheme
    // uint32_t h1, h2;
    // pmem_compute_hash(key, &h1, &h2);

    // cuckoo_elem_t *elem = lookup(hash, key, h1, h2);
    // if (!elem) return -ENOENT;

    // elem->hash_item.range = size;
    // nvm_persist_struct(elem->hash_item.range);
    // if (hash->do_stats) {
    //     INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(elem->hash_item.range));
    // }
    // return 0;
}

int
pmem_cuckoo_hash_remove(paddr_t key, uint32_t *index)
{
    uint32_t h1, h2;
    pmem_compute_hash(key, &h1, &h2);

    *index = pmem_lookup(key, h1, h2);
    if (!(*index)) return -ENOENT;

    //set tombstone
    //FIX THIS
    CUCKOO_SET_EMPTY(pmem_cuckoo_vol->entries[*index].key);
    pmem_nvm_cuckoo_flush(&(pmem_cuckoo_vol->entries[*index].key), sizeof(paddr_t));

    //make persistent

    return 0;
}

// static
// bool
// undo_insert(pmem_cuckoo_elem_t *item, int which_hash,
//             size_t max_depth)
// {
//     uint32_t mod = (uint32_t) cuckoo->meta.max_size;

//     for (size_t depth = 0; depth < max_depth; ++depth) {
//         uint32_t h2m = item->hash2 % mod;
//         struct cuckoo_hash_elem *elem = bin_at(hash, h2m);

//         struct cuckoo_hash_elem victim = *elem;

//         elem->hash_item = item->hash_item;
//         elem->hash1     = item->hash2;
//         elem->hash2     = item->hash1;

//         nvm_persist_struct(*elem);
//         if (hash->do_stats) {
//             INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
//         }

//         uint32_t h1m = victim.hash1 % mod;
//         if (h1m != h2m) {
//             assert(depth >= max_depth);

//             return true;
//         }

//         *item = victim;
//         nvm_persist_struct(*item);
//         if (hash->do_stats) {
//             INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*item));
//         }
//     }

//     return false;
// }



static int
pmem_cuckoo_insert_node(pmem_cuckoo_elem_t *item, uint32_t index) {
    int nretries = 0;
    while(1) {
        // uint32_t status = _xbegin();
        // if(status == _XBEGIN_STARTED) {
            pmem_cuckoo_vol->entries[index] = *item;
           //  _xend();
            pmem_nvm_cuckoo_flush(pmem_cuckoo_vol->entries + index, sizeof(pmem_cuckoo_elem_t));
            return 1;
        // } 
        if(++nretries > 10000) {
            panic("could not initiate transaction in cuckoo insert!\n");
        }
    }
}

static inline
bool
pmem_insert(pmem_cuckoo_elem_t *item, int first, int which_hash, paddr_t *paddr)
{
    size_t max_depth = (size_t) pmem_cuckoo->meta.max_size;

    uint32_t mod = (uint32_t) pmem_cuckoo->meta.max_size;

    if(first) {
        which_hash = 0; //0 == either
    }

    for (size_t depth = 0; depth < max_depth; ++depth) {
        pmem_cuckoo_elem_t elem;
        uint32_t h1m = item->hash1 % mod;
        uint32_t h2m = item->hash2 % mod;
        if(which_hash != 2) { //curr = 1 or either
            elem_at(h1m, &elem);
            
            if (CUCKOO_IS_EMPTY(elem.key)) {
                int success = pmem_cuckoo_insert_node(item, h1m);
                // *elem = *item;
                // nvm_persist_struct(*elem);
                // if (hash->do_stats) {
                //     INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
                // }
                if(unlikely(depth == 0)) {
                    *paddr = h1m + pmem_cuckoo->meta.meta_size;
                }
                if(success) return 1;
            }
        }
        if(which_hash != 1) { //which_hash = 2 or either
            elem_at(h2m, &elem);
            if(CUCKOO_IS_EMPTY(elem.key)) {
                int success = pmem_cuckoo_insert_node(item, h2m);
                if(unlikely(depth == 0)) {
                    *paddr = h2m + pmem_cuckoo->meta.meta_size;
                }
                if(success) return 1;
            }
        }
        pmem_cuckoo_elem_t victim = elem;
        uint32_t index = which_hash == 1 ? h1m : h2m;
        pmem_cuckoo_insert_node(item, index);
        if(unlikely(depth == 0)) {
            *paddr = index + pmem_cuckoo->meta.meta_size;
        }
        if(victim.hash1 % mod == index) {
            which_hash = 2;
        }
        else {
            which_hash = 1;
        }
        *item = victim;

        // *elem = *item;
        // nvm_persist_struct(*elem);
        // if (hash->do_stats) {
        //     INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
        // }

        // item->hash_item = victim.hash_item;
        // item->hash1     = victim.hash2;
        // item->hash2     = victim.hash1;
        // nvm_persist_struct(*item);
        // if (hash->do_stats) {
        //     INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*item));
        // }
    }

    if(first) {
        pmem_insert(item, 0, which_hash == 1 ? 2 : 1, paddr);
        return false;
    }
    else {
        return false;
    }
}


int
pmem_cuckoo_hash_insert(paddr_t key, paddr_t *paddr)
{
    uint32_t h1, h2;
    pmem_compute_hash(key, &h1, &h2);

    int found = pmem_lookup(key, h1, h2);
    if (found) {
        return found;
    }

    pmem_cuckoo_elem_t new_elem = {
      .key = key,
      .hash1 = h1,
      .hash2 = h2
    };

    if (pmem_insert(&new_elem, 1, 0, paddr)) {
        return -1;
    }

    // assert(new_elem.hash_item.key == key);
    // assert(new_elem.hash_item.value == value);
    // assert(new_elem.hash_item.index == index);
    // assert(new_elem.hash_item.range == range);
    // assert(new_elem.hash1 == h1);
    // assert(new_elem.hash2 == h2);

    return -1;
}
