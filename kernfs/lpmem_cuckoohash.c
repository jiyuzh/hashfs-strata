#include <stdbool.h>
#include "lpmem_cuckoohash.h"

int pmem_cuckoohash_initialize(struct disk_superblock *sblk) {
    int ret = pmem_cuckoo_hash_init(sblk);
    if_then_panic(ret, "could not allocate hash table");

    return 0;
}

void pmem_cuckoohash_close() {
    pmem_cuckoo_hash_close();
}
// if not exists, then the value was not already in the table, therefore
// success.
// returns 1 on success, 0 if key already existed
ssize_t pmem_cuckoohash_create(inum_t inum, paddr_t lblk, paddr_t *paddr) {
    
    printf("creating inum: %u, lblk %u at block ", inum, lblk); 
    hash_key_t k = MAKEKEY(inum, lblk);
    // Index: how many more logical blocks are contiguous before this one?
    int err = pmem_cuckoo_hash_insert(k, paddr);
	printf("%ul\n", *paddr);
    if (!err) {
        *paddr = 0;
        return -EEXIST;
    }
    

    // if (ht->do_stats) {
    //     INCR_STAT(&cstats, nwrites);
    //     ADD_STAT(&cstats, nblocks_inserted, nalloc);
    // }

    return 1;
}

/*
 * Returns 0 if found, or -errno otherwise.
 */
ssize_t pmem_cuckoohash_lookup(inum_t inum, paddr_t lblk, paddr_t* paddr) {

    hash_key_t k = MAKEKEY(inum, lblk);
    int err = pmem_cuckoo_hash_lookup(k, paddr);
    if (err) return err;

    if (*paddr != 0) return 1;

    return -ENOENT;
}

/*
 * Returns FALSE if the requested logical block was not present in any of the
 * two hash tables.
 */
ssize_t pmem_cuckoohash_remove(inum_t inum, paddr_t lblk) {

    ssize_t ret = 0;
    hash_key_t k = MAKEKEY(inum, lblk);
    paddr_t removed;
    uint32_t index;
    int err = pmem_cuckoo_hash_remove(k, &index);
    if (err) return err;

    return ret;
}

/*int cuckoohash_set_caching(idx_struct_t *idx_struct, bool enable) {
    return 0;
}

int cuckoohash_set_locking(idx_struct_t *idx_struct, bool enable) {
    return 0;
}

int cuckoohash_persist_updates(idx_struct_t *idx_struct) {
    return 0; 
}

int cuckoohash_invalidate_caches(idx_struct_t *idx_struct) {
    return 0;
}

void cuckoohash_set_stats(idx_struct_t *idx_struct, bool enable) {
    CUCKOOHASH(idx_struct, ht);
    ht->do_stats = enable;
}

void cuckoohash_print_stats(idx_struct_t *idx_struct) {
    cuckoohash_print_global_stats();
}

void cuckoohash_print_global_stats(void) {
    printf("CUCKOO HASH TABLE:\n");
    printf("\tInserts: %.1f blocks per op (%lu / %lu)\n",
        (float)cstats.nblocks_inserted / (float)cstats.nwrites,
        cstats.nblocks_inserted, cstats.nwrites);
    printf("\tInserts: %.1f cachelines per op (%lu / %lu)\n",
        (float)cstats.ncachelines_written / (float)cstats.nwrites, 
        cstats.ncachelines_written, cstats.nwrites);
}

void cuckoohash_clean_global_stats(void) {
    memset(&cstats, 0, sizeof(cstats));
}

idx_fns_t cuckoohash_fns = {
    .im_init               = cuckoohash_initialize,
    .im_init_prealloc      = NULL,
    .im_lookup             = cuckoohash_lookup,
    .im_create             = cuckoohash_create,
    .im_remove             = cuckoohash_remove,

    .im_set_caching        = cuckoohash_set_caching,
    .im_set_locking        = cuckoohash_set_locking,
    .im_persist            = cuckoohash_persist_updates,
    .im_invalidate         = cuckoohash_invalidate_caches,

    .im_set_stats          = cuckoohash_set_stats,
    .im_print_stats        = cuckoohash_print_stats,
    .im_print_global_stats = cuckoohash_print_global_stats,
    .im_clean_global_stats = cuckoohash_clean_global_stats
};*/
