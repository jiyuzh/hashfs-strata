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
pmem_cuckoo_hash_init(nvm_cuckoo_idx_t **ht, paddr_t meta_block, 
                 size_t max_entries, const idx_spec_t *idx_spec)
{
    printf("inside cuckoo_new\n");
    struct disk_superblock *sblk = sb[g_root_dev]->ondisk;
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
pmem_cuckoo_close()
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
    return pmem_cuckoo_vol->entries[index];
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

    elem_at(h2 % mod, &elem)
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
//8/13/2019
int
cuckoo_hash_update(const struct cuckoo_hash *hash, paddr_t key, uint32_t size)
{
    uint32_t h1, h2;
    pmem_compute_hash(key, &h1, &h2);

    cuckoo_elem_t *elem = lookup(hash, key, h1, h2);
    if (!elem) return -ENOENT;

    elem->hash_item.range = size;
    nvm_persist_struct(elem->hash_item.range);
    if (hash->do_stats) {
        INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(elem->hash_item.range));
    }
    return 0;
}

int
cuckoo_hash_remove(struct cuckoo_hash *hash, paddr_t key, paddr_t *value,
                   uint32_t *index, uint32_t *range)
{
    uint32_t h1, h2;
    compute_hash(key, &h1, &h2);

    cuckoo_elem_t *elem = lookup(hash, key, h1, h2);
    if (!elem) return -ENOENT;

    *value = elem->hash_item.value;
    *index = elem->hash_item.index;
    *range = elem->hash_item.range;

    // TODO: make pmem persistent
    memset((void*)elem, 0, sizeof(*elem));
    nvm_persist_struct(*elem);

    return 0;
}

static
bool
undo_insert(struct cuckoo_hash *hash, struct cuckoo_hash_elem *item,
            size_t max_depth)
{
    uint32_t mod = (uint32_t) hash->meta.max_size;

    for (size_t depth = 0; depth < max_depth; ++depth) {
        uint32_t h2m = item->hash2 % mod;
        struct cuckoo_hash_elem *elem = bin_at(hash, h2m);

        struct cuckoo_hash_elem victim = *elem;

        elem->hash_item = item->hash_item;
        elem->hash1     = item->hash2;
        elem->hash2     = item->hash1;

        nvm_persist_struct(*elem);
        if (hash->do_stats) {
            INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
        }

        uint32_t h1m = victim.hash1 % mod;
        if (h1m != h2m) {
            assert(depth >= max_depth);

            return true;
        }

        *item = victim;
        nvm_persist_struct(*item);
        if (hash->do_stats) {
            INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*item));
        }
    }

    return false;
}


static inline
bool
insert(struct cuckoo_hash *hash, struct cuckoo_hash_elem *item)
{
    size_t max_depth = (size_t) hash->meta.max_size;

    uint32_t mod = (uint32_t) hash->meta.max_size;

    for (size_t depth = 0; depth < max_depth; ++depth) {
        uint32_t h1m = item->hash1 % mod;
        cuckoo_elem_t *elem = bin_at(hash, h1m);

        if (elem->hash1 == elem->hash2 || (elem->hash1 % mod) != h1m) {
            *elem = *item;
            nvm_persist_struct(*elem);
            if (hash->do_stats) {
                INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
            }

            return true;
        }

        cuckoo_elem_t victim = *elem;

        *elem = *item;
        nvm_persist_struct(*elem);
        if (hash->do_stats) {
            INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*elem));
        }

        item->hash_item = victim.hash_item;
        item->hash1     = victim.hash2;
        item->hash2     = victim.hash1;
        nvm_persist_struct(*item);
        if (hash->do_stats) {
            INCR_NR_CACHELINE(&cstats, ncachelines_written, sizeof(*item));
        }
    }

    return undo_insert(hash, item, max_depth);
}


int
cuckoo_hash_insert(struct cuckoo_hash *hash, paddr_t key, paddr_t value, 
                   uint32_t index, uint32_t range)
{
    uint32_t h1, h2;
    compute_hash(key, &h1, &h2);

    struct cuckoo_hash_elem *elem = lookup(hash, key, h1, h2);
    if (elem) {
        return 0;
    }

    struct cuckoo_hash_elem new_elem = {
      .hash_item = { .key = key, .value = value, .index = index, .range = range },
      .hash1 = h1,
      .hash2 = h2
    };

    if (insert(hash, &new_elem)) {
        return -1;
    }

    assert(new_elem.hash_item.key == key);
    assert(new_elem.hash_item.value == value);
    assert(new_elem.hash_item.index == index);
    assert(new_elem.hash_item.range == range);
    assert(new_elem.hash1 == h1);
    assert(new_elem.hash2 == h2);

    return -1;
}
