#ifndef __NVM_IDX_HASH_FUNCTIONS_H__
#define __NVM_IDX_HASH_FUNCTIONS_H__

#include <stdint.h>

#include "xxhash.h"
#include <emmintrin.h>
#include <immintrin.h>

// score = 99.779449191701261
static uint64_t
hash(uint64_t x)
{
    x *= UINT64_C(0x8c98cab1667ed515);
    x ^= x >> 57;
    x ^= x >> 21;
    x ^= UINT64_C(0xac274618482b6398);
    x ^= x >> 3;
    x *= UINT64_C(0x6908cb6ac8ce9a09);
    return x;
}

static uint32_t
hash_64_32(uint64_t x)
{
    x *= UINT64_C(0x8c98cab1667ed515);
    x ^= x >> 57;
    x ^= x >> 21;
    x ^= UINT64_C(0xac274618482b6398);
    x ^= x >> 3;
    x *= UINT64_C(0x6908cb6ac8ce9a09);
    return (uint32_t)x;
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

static void
mixHash_simd64_helper(__m512i *first, __m512i *second, __m512i *third, int right, uint32_t shiftCount) {
  *first = _mm512_sub_epi64(*first, *second); //first = first - second
  *first = _mm512_sub_epi64(*first, *third); //first = first - third
  __m512i bsTemp;
  if(right) {
    bsTemp = _mm512_srli_epi64(*third, shiftCount); //>>
  }
  else {
    bsTemp = _mm512_slli_epi64(*third, shiftCount); //<<
  }
  *first = _mm512_xor_epi64(*first, bsTemp); //first = first XOR (third (<< || >>) shiftcount)
}


static void mixHash_simd64(__m512i *c, __m256i *node_indices) {
  int RIGHT = 1;
  int LEFT = 0;
  __mmask8 oneMask = _cvtu32_mask8(~0); // ones
  __m512i a = _mm512_set1_epi64(0xff51afd7ed558ccdL);
  __m512i b = _mm512_set1_epi64(0xc4ceb9fe1a85ec53L);
  mixHash_simd64_helper(&a, &b, c, RIGHT, 13);
  mixHash_simd64_helper(&b, c, &a, LEFT, 8);
  mixHash_simd64_helper(c, &a, &b, RIGHT, 13);
  mixHash_simd64_helper(&a, &b, c, RIGHT, 12);
  mixHash_simd64_helper(&b, c, &a, LEFT, 16);
  mixHash_simd64_helper(c, &a, &b, RIGHT, 5);
  mixHash_simd64_helper(&a, &b, c, RIGHT, 3);
  mixHash_simd64_helper(&b, c, &a, LEFT, 10);
  mixHash_simd64_helper(c, &a, &b, RIGHT, 15);

  __m256i hash_values = _mm512_cvtepi64_epi32(*c); // direct hash with truncation to 32-bit
  pmem_mod_simd32(&hash_values, node_indices);
}


typedef paddr_t (*hash_func_t)(paddr_t key);

// https://gist.github.com/badboy/6267743
static inline laddr_t nvm_idx_hash6432shift(paddr_t key) {
  key = (~key) + (key << 18); // key = (key << 18) - key - 1;
  key = key ^ (key >> 31);
  key = key * 21; // key = (key + (key << 2)) + (key << 4);
  key = key ^ (key >> 11);
  key = key + (key << 6);
  key = key ^ (key >> 22);
  return (laddr_t) key;
}

static inline laddr_t jenkins_hash(paddr_t a) {
    a = (a+0x7ed55d16) + (a<<12);
    a = (a^0xc761c23c) ^ (a>>19);
    a = (a+0x165667b1) + (a<<5);
    a = (a+0xd3a2646c) ^ (a<<9);
    a = (a+0xfd7046c5) + (a<<3);
    a = (a^0xb55a4f09) ^ (a>>16);
    return (laddr_t)a;
}

static inline laddr_t murmur64(paddr_t h) {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >> 33;
    return (laddr_t)h;
}

static inline laddr_t mix(paddr_t c)
{
    paddr_t a = 0xff51afd7ed558ccdL;
    paddr_t b = 0xc4ceb9fe1a85ec53L;
	a=a-b;  a=a-c;  a=a^(c >> 13);
	b=b-c;  b=b-a;  b=b^(a << 8);
	c=c-a;  c=c-b;  c=c^(b >> 13);
	a=a-b;  a=a-c;  a=a^(c >> 12);
	b=b-c;  b=b-a;  b=b^(a << 16);
	c=c-a;  c=c-b;  c=c^(b >> 5);
	a=a-b;  a=a-c;  a=a^(c >> 3);
	b=b-c;  b=b-a;  b=b^(a << 10);
	c=c-a;  c=c-b;  c=c^(b >> 15);
	return (laddr_t)c;
}

static inline laddr_t nvm_idx_direct_hash (paddr_t v) {
  return (laddr_t)(v & 0xFFFFFFFF);
}

static inline laddr_t nvm_idx_combo_hash (paddr_t v) {
    return (laddr_t)((v & 0xFFFFFFFF) ^ ((v >> 32) & 0xFFFFFFFF));
}

static inline laddr_t nvm_xxhash(paddr_t v) {
    static uint64_t seed = 0;   /* or any other value */
    return (laddr_t)XXH32(&v, sizeof(v), seed);
}

#endif  //__HASH_FUNCTIONS_H__
