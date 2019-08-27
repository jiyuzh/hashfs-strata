/* GLIB - Library of useful routines for C programming
 * Copyright (C) 1995-1997  Peter Mattis, Spencer Kimball and Josh MacDonald
 *
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

/*
 * MT safe
 */
#include <stdlib.h>
#include <assert.h>
#include <string.h>  /* memset */
#include <stdint.h>
#include <emmintrin.h>
#include <immintrin.h>
#include "lpmem_ghash.h"

#define G_DISABLE_ASSERT

#define MAX(x,y) (x > y ? x : y)

// stats
uint64_t reads;
uint64_t writes;
uint64_t blocks;
pmem_nvm_hash_idx_t *pmem_ht = NULL;
pmem_nvm_hash_vol_t *pmem_ht_vol = NULL;
typedef union {__m512i vec; uint64_t arr[8];} u512i_64;
typedef union {__m256i vec; uint32_t arr[8];} u256i_32;

#if 0
#define pthread_rwlock_rdlock(x) 0
#define pthread_rwlock_wrlock(x) 0
#define pthread_rwlock_unlock(x) 0
#elif 0
#define pthread_rwlock_rdlock(x) do { printf("rdlk\n"); pthread_rwlock_rdlock(x); } while (0)
#define pthread_rwlock_wrlock(x) do { printf("wrlk\n"); pthread_rwlock_wrlock(x); } while (0)
#define pthread_rwlock_unlock(x) do { printf("unlk\n"); pthread_rwlock_unlock(x); } while (0)
#elif 0
#define pthread_rwlock_rdlock(x) if_then_panic(0 != pthread_rwlock_tryrdlock(x), "Could not acquire rdlock!")
#define pthread_rwlock_wrlock(x) if_then_panic(0 != pthread_rwlock_trywrlock(x), "Could not acquire wrlock!")
#endif



#define HASH_TABLE_MIN_SHIFT 3  /* 1 << 3 == 8 buckets */

#define TRUE 1
#define FALSE 0

/* Each table size has an associated prime modulo (the first prime
 * lower than the table size) used to find the initial bucket. Probing
 * then works modulo 2^n. The prime modulo is necessary to get a
 * good distribution with poor hash functions.
 */
static const int prime_mod [] = {
  1,          /* For 1 << 0 */
  2,
  3,
  7,
  13,
  31,
  61,
  127,
  251,
  509,
  1021,
  2039,
  4093,
  8191,
  16381,
  32749,
  65521,      /* For 1 << 16 */
  131071,
  262139,
  524287,
  1048573,
  2097143,
  4194301,
  8388593,
  16777213,
  33554393,
  67108859,
  134217689,
  268435399,
  536870909,
  1073741789,
  2147483647  /* For 1 << 31 */
};

void pEntries() {
	for(size_t i = 0; i < 10; ++i) {
		printf("%lu: %lu\n", i, pmem_ht_vol->entries[i]);
	}
}

static void pmem_nvm_flush(void* start, uint32_t len) {
    if(pmem_ht->is_pmem) {
      pmem_persist(start, len);
    }
    else {
      pmem_msync(start, len);
    }
}

/*
 * nvm_hash_table_lookup_node:
 * @hash_table: our #nvm_hash_idx_t
 * @key: the key to lookup against
 * @hash_return: key hash return location
 *
 * Performs a lookup in the hash table, preserving extra information
 * usually needed for insertion.
 *
 * This function first computes the hash value of the key using the
 * user's hash function.
 *
 * If an entry in the table matching @key is found then this function
 * returns the index of that entry in the table, and if not, the
 * index of an unused node (empty or tombstone) where the key can be
 * inserted.
 *
  The computed hash value is returned in the variable pointed to
 * by @hash_return. This is to save insertions from having to compute
 * the hash record again for the new record.
 *
 * Returns: index of the described node
 */
#define SEQ_STEP
#pragma GCC push_options
//#pragma GCC optimize ("unroll-loops")

static inline uint32_t
pmem_nvm_hash_table_lookup_node (paddr_t        key,
                          paddr_t  *ent_return,
                          uint32_t      *hash_return/*,
                          bool           force*/) {

  uint32_t node_index;
  uint32_t hash_value;
  uint32_t mod;
  uint32_t first_tombstone = 0;
  int have_tombstone = FALSE;
#ifdef SEQ_STEP
  uint32_t step = 1;
#else
  uint32_t step = 0;
#endif
  paddr_t cur;
  paddr_t *entries = pmem_ht_vol->entries;
  hash_value = pmem_ht_vol->hash_func(key);
  mod = pmem_ht->mod;
  node_index = hash_value % mod;
  cur = entries[node_index];
  uint64_t count = 0;
  *hash_return = hash_value;
  count++;

  while (!HASHFS_ENT_IS_EMPTY(cur)) {
    if (cur == key && HASHFS_ENT_IS_VALID(cur)) {
      *ent_return = cur;
      return node_index;
    }
    else if (HASHFS_ENT_IS_TOMBSTONE(cur) && !have_tombstone) {
      first_tombstone = node_index;
      have_tombstone = TRUE;
    }
#ifndef SEQ_STEP
    step++;
#endif
    uint32_t new_idx = (node_index + step) % mod;
    node_index = new_idx;
    cur = entries[node_index];
    
    count++;
  }


end:
  if (have_tombstone) {
    *ent_return = entries[first_tombstone];
    return first_tombstone;
  }
  *ent_return = entries[node_index];

  return node_index;
}

void pmem_mod_simd32(__m256i *vals, __m256i *ret) {
  u256i_32 *tempVals = (u256i_32*) vals;
  u256i_32 *tempMods = (u256i_32*) ret;
  uint32_t mod = pmem_ht->mod;
  for(int i = 0; i < 8; ++i) {
    tempMods->arr[i] = tempVals->arr[i] % mod;
  }

  // __mmask8 oneMask = _cvtu32_mask8(~0);
  // __m256i mod_vec = _mm256_maskz_set1_epi32 (oneMask, pmem_ht->mod); // set mod vector
  // __m256i quotient = _mm256_div_epi32(*vals, mod_vec); //divide hash_values by mod (SVML)
  // __m256i mult_result = _mm256_mask_mul_epu32(quotient, oneMask, quotient, mod_vec); // multiply quotient by mod
  // *ret = _mm256_mask_sub_epi32 (mult_result, oneMask, *vals, mult_result); //subtract mult result from hash_values
  
}

static void printVec_simd64(char* what, __m512i *vec) {
    printf("%s: ", what);
    u512i_64 *temp = (u512i_64*)vec;
    for(size_t i = 0; i < 8; ++i) {
	    printf("%lu ", temp->arr[i]);
    }
    printf("\n");
}


static void printVec_simd32(char* what, __m256i *vec) {
    printf("%s: ", what);
    u256i_32 *temp = (u256i_32*)vec;
    for(size_t i = 0; i < 8; ++i) {
	printf("%lu ", temp->arr[i]);
    }
    printf("\n");
}

static void printMask_simd8(char* what, __mmask8 *mask) {
    printf("%s: ", what);
    uint32_t m = _cvtmask8_u32(*mask);
    int pOf2[8] = {1, 2, 4, 8, 16, 32, 64, 128};
    int i;
    for(i = 0; i < 8; ++i) {
	if((m & pOf2[i]) == 0) {
	    printf("0 ");
	}
	else {
	    printf("1 ");
	}
    } 
   printf("\n"); 

}

static void directHash_simd64(__m512i *keys, __m256i *node_indices) {
  __mmask8 oneMask = _cvtu32_mask8(~0); // ones
  __m256i hash_values = _mm512_cvtepi64_epi32(*keys); // direct hash with truncation to 32-bit
  pmem_mod_simd32(&hash_values, node_indices);
}

static void pmem_make_key_simd64(__m512i *inums, __m512i *lblks, __m512i *keys) {
  __mmask8 oneMask = _cvtu32_mask8(~0); //zeros
  *keys = _mm512_mask_mov_epi64(*inums, oneMask, *inums); // keys = inums
  *keys = _mm512_rol_epi64(*keys, 32); // rotate left 32 bits
  *keys = _mm512_or_epi64(*keys, *lblks); // & with lblks
}

void pmem_nvm_hash_table_lookup_node_simd64(__m512i *keys, __m256i *node_indices, __mmask8 *failure, __mmask8 searching) {

#ifdef SEQ_STEP
  uint32_t step = 1;
#else
  uint32_t step = 0;
#endif

  __mmask8 oneMask = _cvtu32_mask8(~0); // ones
  __m512i tombstone_vec = _mm512_set1_epi64(HASHFS_TOMBSTONE_VAL); //vector of tombstones for comparison
  __m512i empty_val = _mm512_set1_epi64(HASHFS_EMPTY_VAL);
  __m256i first_tombstone = _mm256_maskz_set1_epi32(oneMask, 0);
  __mmask8 found_tombstone = _cvtu32_mask8(0); // zeroes
  directHash_simd64(keys, node_indices);
  __m512i cur = _mm512_mask_i32gather_epi64 (empty_val, oneMask, *node_indices, (void const*)(pmem_ht_vol->entries), 8);
  
  // if it's zero, we're done
  *failure = _cvtu32_mask8(0);

  while(_cvtmask8_u32(searching) != 0) {
    searching = _mm512_mask_cmpneq_epi64_mask(searching, *keys, cur); //0 if key == cur || searching = 0, 1 otherwise
    __mmask8 seeking_tombstone = _knot_mask8(found_tombstone);
    __mmask8 is_tombstone = _mm512_mask_cmpeq_epi64_mask(searching, cur, tombstone_vec); //1 if cur == tombstone and still searching
    __mmask8 found_and_seeking = _kand_mask8(is_tombstone, seeking_tombstone); //1 if tombstone, seeking tombstone, still searching
    first_tombstone = _mm256_mask_mov_epi32(first_tombstone, found_and_seeking, *node_indices); //if(mask) node_indices else original
    found_tombstone = _kor_mask8(found_and_seeking, found_tombstone); //if already found or found this time, update found

    __mmask8 is_empty = _mm512_mask_cmpeq_epi64_mask(searching, cur, empty_val); //1 if empty and searching, 0 otherwise
    *failure = _kor_mask8(*failure, is_empty);
    __mmask8 found_t_and_empty = _kand_mask8(is_empty, found_tombstone);
    *node_indices = _mm256_mask_mov_epi32 (*node_indices, found_t_and_empty, first_tombstone);

    searching = _kandn_mask8(is_empty, searching);
#ifndef SEQ_STEP
    step++;
#endif
    __m256i step_vec = _mm256_maskz_set1_epi32(oneMask, step);
    *node_indices = _mm256_mask_add_epi32(*node_indices, searching, *node_indices, step_vec);
    pmem_mod_simd32(node_indices, node_indices);
    cur = _mm512_mask_i32gather_epi64 (cur, searching, *node_indices, (void const*)(pmem_ht_vol->entries), 8);

  }
  
}

#pragma GCC pop_options

/*
 * Send help.
 *
 * Also I think we can only use this in libfs.
 */

/*
 * nvm_hash_table_remove_node:
 * @hash_table: our #nvm_hash_idx_t
 * @node: pointer to node to remove
 * @notify: %TRUE if the destroy notify handlers are to be called
 *
 * Removes a node from the hash table and updates the node count.
 * The node is replaced by a tombstone. No table resize is performed.
 *
 * If @notify is %TRUE then the destroy notify functions are called
 * for the key and value of the hash node.
 */
static void pmem_nvm_hash_table_remove_node (int              i//,
                                        /*paddr_t         *pblk,
                                        size_t          *old_precursor,
                                        size_t          *old_size*/) {
  
#ifndef SIMPLE_ENTRIES
  // ent->size = 0;
  // ent->index = 0;
#endif
  paddr_t *entries = pmem_ht_vol->entries;
  HASHFS_ENT_SET_TOMBSTONE(entries[i]);
  pmem_nvm_flush((void*)(entries + i), sizeof(paddr_t));
  pmem_ht->nnodes -= 1;
  pmem_nvm_flush((void*)(&(pmem_ht->nnodes)), sizeof(int));
}

/**
 * nvm_hash_table_new:
 * @hash_func: a function to create a hash value from a key
 * @key_equal_func: a function to check two keys for equality
 *
 * Creates a new #nvm_hash_idx_t with a reference count of 1.
 *
 * Hash values returned by @hash_func are used to determine where keys
 * are stored within the #nvm_hash_idx_t data structure. The direct_hash(),
 * g_int_hash(), g_int64_hash(), g_double_hash() and g_str_hash()
 * functions are provided for some common types of keys.
 * If @hash_func is %NULL, direct_hash() is used.
 *
 * @key_equal_func is used when looking up keys in the #nvm_hash_idx_t.
 * The g_direct_equal(), g_int_equal(), g_int64_equal(), g_double_equal()
 * and g_str_equal() functions are provided for the most common types
 * of keys. If @key_equal_func is %NULL, keys are compared directly in
 * a similar fashion to g_direct_equal(), but without the overhead of
 * a function call. @key_equal_func is called with the key from the hash table
 * as its first parameter, and the user-provided key to check against as
 * its second.
 *
 * Returns: a new #nvm_hash_idx_t
 */

void
pmem_nvm_hash_table_new(struct disk_superblock *sblk,
                   hash_func_t       hash_func
                   //size_t            block_size,
                   //size_t            range_size,
                   //paddr_t           metadata_location,
                   //const idx_spec_t *idx_spec
                   ) {
  pmem_ht = (pmem_nvm_hash_idx_t*)(dax_addr[g_root_dev] + (sblk->datablock_start * g_block_size_bytes));
  if(pmem_ht->valid == 1) {
    printf("ht exists\n");
    pmem_ht_vol = (pmem_nvm_hash_vol_t *)malloc(sizeof(pmem_nvm_hash_vol_t));
    pmem_ht_vol->hash_func = hash_func ? hash_func : nvm_idx_direct_hash;
    pmem_ht_vol->entries = (paddr_t*)(dax_addr[g_root_dev] + (pmem_ht->entries_blk * g_block_size_bytes));
    return;
  }
  printf("ht does not exist\n");
  uint64_t ent_num_bytes = sizeof(paddr_t) * sblk->ndatablocks;
  int ent_num_blocks_needed = 1 + ent_num_bytes / g_block_size_bytes;
  if(ent_num_bytes % g_block_size_bytes != 0) {
    ++ent_num_blocks_needed;
  }
  pmem_ht->meta_size = ent_num_blocks_needed;
  pmem_ht->num_entries = sblk->ndatablocks - ent_num_blocks_needed;
  pmem_ht->entries_blk = sblk->datablock_start + 1;

  //need to update num blocks available somewhere?
  pmem_ht_vol = (pmem_nvm_hash_vol_t *)malloc(sizeof(pmem_nvm_hash_vol_t));
  pmem_ht_vol->hash_func = hash_func ? hash_func : nvm_idx_direct_hash;
  pmem_ht_vol->entries = (paddr_t*)(dax_addr[g_root_dev] + (pmem_ht->entries_blk * g_block_size_bytes));
  pmem_ht->nnodes = 0;
  pmem_ht->noccupied = 0;
  pmem_ht->mod = pmem_ht->num_entries;
  pmem_ht->mask = pmem_ht->num_entries;
  pmem_ht->size = 0;

  pmem_ht->is_pmem = pmem_is_pmem(pmem_ht, ent_num_blocks_needed * g_block_size_bytes);
  memset(pmem_ht_vol->entries, ~0, (ent_num_blocks_needed - 1) * g_block_size_bytes);
  pmem_nvm_flush((void*)(pmem_ht), ent_num_blocks_needed * g_block_size_bytes);
  pmem_ht->valid = 1;
  pmem_nvm_flush((void*)(&(pmem_ht->valid)), sizeof(int));
  return;
}


void pmem_nvm_hash_table_close() {
	free(pmem_ht_vol);  
}


/*
 * nvm_hash_table_insert_node:
 * @hash_table: our #nvm_hash_idx_t
 * @node_index: pointer to node to insert/replace
 * @key_hash: key hash
 * @key: (nullable): key to replace with, or %NULL
 * @value: value to replace with
 * @keep_new_key: whether to replace the key in the node with @key
 * @reusing_key: whether @key was taken out of the existing node
 *
 * Inserts a value at @node_index in the hash table and updates it.
 *
 * If @key has been taken out of the existing node (ie it is not
 * passed in via a nvm_hash_table_insert/replace) call, then @reusing_key
 * should be %TRUE.
 *
 * Returns: %TRUE if the key did not exist yet
 */

static int
pmem_nvm_hash_table_insert_node(uint32_t node_index, uint32_t key_hash,
                           paddr_t new_key
                           //, size_t new_index, size_t new_range
                           )
{
  paddr_t *entries = pmem_ht_vol->entries;
  paddr_t ent = entries[node_index];

  // todo consider bookkeeping (nnodes, noccupied?)
  paddr_t expected = (paddr_t)~0;
  if(HASHFS_ENT_IS_TOMBSTONE(ent)) {
    expected -= 1;
  }
  int success = atomic_compare_exchange_strong(entries + node_index, &expected, new_key);
  if(success) {
    pmem_nvm_flush((void*)(entries + node_index), sizeof(paddr_t));
  }
  return success;
    

#ifndef SIMPLE_ENTRIES
    // ent->index = new_index;
    // ent->size = new_range;
#endif
}

/**
 * nvm_hash_table_lookup:
 * @hash_table: a #nvm_hash_idx_t
 * @key: the key to look up
 *
 * Looks up a key in a #nvm_hash_idx_t. Note that this function cannot
 * distinguish between a key that is not present and one which is present
 * and has the value %NULL. If you need this distinction, use
 * nvm_hash_table_lookup_extended().
 *
 * Returns: (nullable): the associated value, or %NULL if the key is not found
 */
int pmem_nvm_hash_table_lookup(inum_t inum, paddr_t lblk,
    paddr_t *val) {
  uint32_t node_index;
  uint32_t hash_return;
  paddr_t ent;
  paddr_t key = HASHFS_MAKEKEY(inum, lblk);
  node_index = pmem_nvm_hash_table_lookup_node(key, &ent, &hash_return/*,force*/);

  *val = node_index + pmem_ht->meta_size;
  int success = HASHFS_ENT_IS_VALID(ent);
  return success;
}

static inline int 
pmem_nvm_hash_table_lookup_internal_simd64(__m512i *inums, __m512i *lblks, __m256i *val, __mmask8 to_find) {
    //create keys vector
  __mmask8 zeroMask = _cvtu32_mask8(0); //zeros
  __mmask8 failure = _cvtu32_mask8(0);
  *val = _mm256_maskz_set1_epi32 (zeroMask, 0);
  __m512i keys;
  pmem_make_key_simd64(inums, lblks, &keys);
  pmem_nvm_hash_table_lookup_node_simd64(&keys, val, &failure, to_find);
  return _cvtmask8_u32(failure) == 0;
}

int pmem_nvm_hash_table_lookup_simd64(uint32_t inum, uint32_t lblk, uint32_t len, uint64_t *pblks) {
  u512i_64 inum_vec;
	u512i_64 lblk_vec;
	uint32_t pOfTwo[8] = {1, 2, 4, 8, 16, 32, 64, 128};
	uint32_t to_do = 0;
	for(size_t i = 0; i < len; ++i) {
		inum_vec.arr[i] = inum;
		lblk_vec.arr[i] = lblk + i;
		to_do |= pOfTwo[i];
	}

  __mmask8 to_find = _cvtu32_mask8(to_do);

	u256i_32 indices;
  int success = pmem_nvm_hash_table_lookup_internal_simd64(&(inum_vec.vec), &(lblk_vec.vec), &(indices.vec), to_find);
  if(!success) {
    return success;
  }
  uint32_t meta_size = pmem_ht->meta_size;
  for(size_t i = 0; i < len; ++i) {
    pblks[i] = ((uint64_t)indices.arr[i]) + ((uint64_t)meta_size);
  }
  return success;
}
/*
 * nvm_hash_table_insert_internal:
 * @hash_table: our #nvm_hash_idx_t
 * @key: the key to insert
 * @value: the value to insert
 * @keep_new_key: if %TRUE and this key already exists in the table
 *   then call the destroy notify function on the old key.  If %FALSE
 *   then call the destroy notify function on the new key.
 *
 * Implements the common logic for the nvm_hash_table_insert() and
 * nvm_hash_table_replace() functions.
 *
 * Do a lookup of @key. If it is found, replace it with the new
 * @value (and perhaps the new @key). If it is not found, create
 * a new node.
 *
 * Returns: %TRUE if the key did not exist yet
 */
static inline int
pmem_nvm_hash_table_insert_internal (paddr_t    key,
                                paddr_t    *index//,
                                //size_t     index,
                                //size_t     size
                                )
{

  /*
   * iangneal: for concurrency reasons, we can't do lookup -> insert, as another
   * thread may come in and use the slot we just looked for.
   * This is basically a copy of lookup_node, but with write locks.
   */
  uint32_t first_tombstone = 0;
  int have_tombstone = 0;
#ifdef SEQ_STEP
  uint32_t step = 1;
#else
  uint32_t step = 0;
  uint32_t tombstone_step;
#endif
  paddr_t *entries = pmem_ht_vol->entries;
  int mod = pmem_ht->mod;
  uint32_t hash_value = pmem_ht_vol->hash_func(key);
  uint32_t node_index = hash_value % mod;
  paddr_t cur = entries[node_index];

  while (!HASHFS_ENT_IS_EMPTY(cur)) {
    if (cur == key && HASHFS_ENT_IS_VALID(cur)) {
      printf("already exists: %lx (trying to insert: %lx at index %u)\n",
        cur, key, node_index);
      return 0;
    } else if (HASHFS_ENT_IS_TOMBSTONE(cur) && !have_tombstone) {
      // keep lock until we decide we don't need it
      first_tombstone = node_index;
      have_tombstone = 1;
#ifndef SEQ_STEP
      tombstone_step = step;
#endif
    }
#ifndef SEQ_STEP
    step++;
#endif
    uint32_t new_idx = (node_index + step) % mod;
    node_index = new_idx;
    cur = entries[node_index];
  }
  int success = 0;
  if (have_tombstone) {
#ifndef SEQ_STEP
    step = tombstone_step;
#endif
    node_index = first_tombstone;
    cur = entries[first_tombstone];
  }

  while(!success) {
      if(!HASHFS_ENT_IS_VALID(cur)) {
        success = pmem_nvm_hash_table_insert_node(
          node_index, hash_value, key);
      }
      if(!success) {

#ifndef SEQ_STEP
        ++step;
#endif
        node_index = (node_index + step) % mod;
        cur = entries[node_index];
      }
    }
  pmem_ht->nnodes = pmem_ht->nnodes + 1;
  pmem_nvm_flush((void*)(&(pmem_ht->nnodes)), sizeof(int));
  *index = node_index + pmem_ht->meta_size;
  return true;
}

/**
 * nvm_hash_table_insert:
 * @hash_table: a #nvm_hash_idx_t
 * @key: a key to insert
 * @value: the value to associate with the key
 *
 * Inserts a new key and value into a #nvm_hash_idx_t.
 *
 * If the key already exists in the #nvm_hash_idx_t its current
 * value is replaced with the new value. If you supplied a
 * @value_destroy_func when creating the #nvm_hash_idx_t, the old
 * value is freed using that function. If you supplied a
 * @key_destroy_func when creating the #nvm_hash_idx_t, the passed
 * key is freed using that function.
 *
 * Returns: %TRUE if the key did not exist yet
 */
int
pmem_nvm_hash_table_insert (inum_t     inum,
                       paddr_t lblk,
                       paddr_t     *index//,
                       //size_t      index,
                       //size_t      size
                       )
{
  paddr_t key = HASHFS_MAKEKEY(inum, lblk);
  return pmem_nvm_hash_table_insert_internal(key, index);//, index, size);
}

void pmem_find_next_invalid_entry_simd64(__m256i *node_indices, uint32_t duplicates) {
  

#ifdef SEQ_STEP
  uint32_t step = 1;
#else
  panic("simd insert doesn't work with this!");
  uint32_t step = 0;
#endif

  __mmask8 oneMask = _cvtu32_mask8(~0); // ones
  __m512i tombstone_vec = _mm512_set1_epi64(HASHFS_TOMBSTONE_VAL); //vector of tombstones for comparison
  __m512i empty_val = _mm512_set1_epi64(HASHFS_EMPTY_VAL);

  __mmask8 searching = _cvtu32_mask8(duplicates);
  __m256i step_vec = _mm256_maskz_set1_epi32(oneMask, step);
  *node_indices = _mm256_mask_add_epi32(*node_indices, searching, *node_indices, step_vec);
  pmem_mod_simd32(node_indices, node_indices);

  __m512i cur = _mm512_mask_i32gather_epi64 (empty_val, oneMask, *node_indices, (void const*)(pmem_ht_vol->entries), 8);
  
  

  while(_cvtmask8_u32(searching) != 0) {
    __mmask8 is_tombstone = _mm512_mask_cmpeq_epi64_mask(searching, cur, tombstone_vec); //1 if cur == tombstone and still searching
    __mmask8 is_empty = _mm512_mask_cmpeq_epi64_mask(searching, cur, empty_val); //1 if empty and searching, 0 otherwise
    __mmask8 tombstone_or_empty = _kor_mask8(is_empty, is_tombstone);

    searching = _kandn_mask8(tombstone_or_empty, searching);

#ifndef SEQ_STEP
    step++;
#endif
    
    *node_indices = _mm256_mask_add_epi32(*node_indices, searching, *node_indices, step_vec);
    pmem_mod_simd32(node_indices, node_indices);
    cur = _mm512_mask_i32gather_epi64 (cur, searching, *node_indices, (void const*)(pmem_ht_vol->entries), 8);

  }
}

static inline int pmem_nvm_hash_table_insert_internal_simd64(__m512i *inums, __m512i *lblks, __m256i *indices, __mmask8 to_find) {
    //create keys vector
  __mmask8 zeroMask = _cvtu32_mask8(0); //zeros
  __mmask8 oneMask = _cvtu32_mask8(~0);
  __mmask8 notFound = _cvtu32_mask8(0);
  __m512i keys;
  pmem_make_key_simd64(inums, lblks, &keys);
  pmem_nvm_hash_table_lookup_node_simd64(&keys, indices, &notFound, to_find);
  if(_cvtmask8_u32(_kxor_mask8(to_find, notFound)) != 0) {
    return false;
  }
  u256i_32 *node_indices = (u256i_32*) indices;
  uint32_t pOfTwo[8] = {1, 2, 4, 8, 16, 32, 64, 128};
  
  uint32_t duplicates_mask = 0;
  do {
    duplicates_mask = 0;
    for(uint32_t i = 0; i < 8; ++i) {
      for(uint32_t j = i; j < 8; ++j) {
        if(i != j && node_indices->arr[i] == node_indices->arr[j]) {
          duplicates_mask |= pOfTwo[j];
        }
      }
    }
    duplicates_mask &= to_find;
    if(duplicates_mask != 0) {
      pmem_find_next_invalid_entry_simd64(indices, duplicates_mask);
    }
  } while(duplicates_mask != 0);
  _mm512_mask_i32scatter_epi64(pmem_ht_vol->entries, to_find, *indices, keys, 8);

  return true;

}

int pmem_nvm_hash_table_insert_simd64(uint32_t inum, uint32_t lblk, uint32_t len, uint64_t *pblks){
  u512i_64 inum_vec;
	u512i_64 lblk_vec;
	uint32_t pOfTwo[8] = {1, 2, 4, 8, 16, 32, 64, 128};
	uint32_t to_do = 0;
	for(size_t i = 0; i < len; ++i) {
		inum_vec.arr[i] = inum;
		lblk_vec.arr[i] = lblk + i;
		to_do |= pOfTwo[i];

	}
  __mmask8 to_find = _cvtu32_mask8(to_do);
	u256i_32 indices;
  int success = pmem_nvm_hash_table_insert_internal_simd64(&(inum_vec.vec), &(lblk_vec.vec), &(indices.vec), to_find);
  uint32_t meta_size = pmem_ht->meta_size;
  for(size_t i = 0; i < len; ++i) {
    pblks[i] = ((uint64_t)indices.arr[i]) + ((uint64_t)meta_size);
  }
  return success;
}
/*
 * nvm_hash_table_remove_internal:
 * @hash_table: our #nvm_hash_idx_t
 * @key: the key to remove
 * @notify: %TRUE if the destroy notify handlers are to be called
 * Returns: %TRUE if a node was found and removed, else %FALSE
 *
 * Implements the common logic for the nvm_hash_table_remove() and
 * nvm_hash_table_steal() functions.
 *
 * Do a lookup of @key and remove it if it is found, calling the
 * destroy notify handlers only if @notify is %TRUE.
 */
static int
pmem_nvm_hash_table_remove_internal (paddr_t         key,
                                paddr_t        *index
                                )
{
#ifdef SEQ_STEP
  uint32_t step = 1;
#else
  uint32_t step = 0;
#endif

  paddr_t *entries = pmem_ht_vol->entries;
  
  int mod = pmem_ht->mod;
  uint32_t hash_value = pmem_ht_vol->hash_func(key);
  uint32_t node_index = hash_value % mod;
  paddr_t cur = entries[node_index];

  while (!HASHFS_ENT_IS_EMPTY(cur)) {
    if (cur == key && HASHFS_ENT_IS_VALID(cur)) {
	break;
    }
#ifndef SEQ_STEP
    step++;
#endif
    uint32_t new_idx = (node_index + step) % mod;
    cur = entries[new_idx];
    node_index = new_idx;
  }
  
  if (HASHFS_ENT_IS_VALID(cur)) {
      pmem_nvm_hash_table_remove_node(node_index);
      *index = node_index + pmem_ht->meta_size;
      return 1;
  }
  *index = 0;
  return 0;
}

static inline
int pmem_nvm_hash_table_remove_internal_simd64(__m512i *inums, __m512i *lblks, __mmask8 to_remove) {
  
  __mmask8 zeroMask = _cvtu32_mask8(0); //zeros
  __mmask8 failure = _cvtu32_mask8(0);
  __m256i node_indices = _mm256_maskz_set1_epi32 (zeroMask, 0);
  __m512i keys;
  pmem_make_key_simd64(inums, lblks, &keys);
  pmem_nvm_hash_table_lookup_node_simd64(&keys, &node_indices, &failure, to_remove);

  __mmask8 to_tombstone = _knot_mask8(failure);
  __m512i tombstone_val = _mm512_set1_epi64(HASHFS_TOMBSTONE_VAL); //vector of tombstones for comparison

  _mm512_mask_i32scatter_epi64(pmem_ht_vol->entries, to_tombstone, node_indices, tombstone_val, 8);

  return _cvtmask8_u32(failure) == 0;

}

int pmem_nvm_hash_table_remove_simd64(uint32_t inum, uint32_t lblk, uint32_t len){
  u512i_64 inum_vec;
	u512i_64 lblk_vec;
	uint32_t pOfTwo[8] = {1, 2, 4, 8, 16, 32, 64, 128};
	uint32_t to_do = 0;
	for(size_t i = 0; i < len; ++i) {
		inum_vec.arr[i] = inum;
		lblk_vec.arr[i] = lblk + i;
		to_do |= pOfTwo[i];
	}

  __mmask8 to_find = _cvtu32_mask8(to_do);

  int success = pmem_nvm_hash_table_remove_internal_simd64(&(inum_vec.vec), &(lblk_vec.vec), to_find);
  return success;
}

/**
 * nvm_hash_table_remove:
 * @hash_table: a #nvm_hash_idx_t
 * @key: the key to remove
 *
 * Removes a key and its associated value from a #nvm_hash_idx_t.
 *
 * If the #nvm_hash_idx_t was created using nvm_hash_table_new_full(), the
 * key and value are freed using the supplied destroy functions, otherwise
 * you have to make sure that any dynamically allocated values are freed
 * yourself.
 *
 * Returns: %TRUE if the key was found and removed from the #nvm_hash_idx_t
 */
int
pmem_nvm_hash_table_remove (inum_t         inum,
                       paddr_t             lblk,
                       paddr_t        *removed
                       )
{
  paddr_t key = HASHFS_MAKEKEY(inum, lblk);
  return pmem_nvm_hash_table_remove_internal(key, removed
                                        //, nprevious, ncontiguous
                                        );
}


/**
 * nvm_hash_table_size:
 * @hash_table: a #nvm_hash_idx_t
 *
 * Returns the number of elements contained in the #nvm_hash_idx_t.
 *
 * Returns: the number of key/value pairs in the #nvm_hash_idx_t.
 */
uint32_t pmem_nvm_hash_table_size () {
  return pmem_ht->nnodes;
}

/*
 * Hash functions.
 */
