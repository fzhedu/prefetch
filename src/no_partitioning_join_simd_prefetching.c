#ifndef _SIMD_PREFETCHING
#define _SIMD_PREFETCHING
#include <assert.h>
#include <immintrin.h>
#include "prefetch.h"
#include "tuple_buffer.h"
#define WORDSIZE 8
// target for 8B keys and 8B payload
int64_t probe_simd(hashtable_t *ht, relation_t *rel, void *output) {
  int64_t matches = 0;
  int32_t new_add = 0;
  __mmask8 m_match = 0, m_have_tuple = 0, m_new_cells = -1;
  __m512i v_offset = _mm512_set1_epi64(0), v_addr_offset = _mm512_set1_epi64(0),
          v_base_offset_upper =
              _mm512_set1_epi64(rel->num_tuples * sizeof(tuple_t)),
          v_tuple_cell = _mm512_set1_epi64(0), v_base_offset,
          v_left_size = _mm512_set1_epi64(8),
          v_bucket_offset = _mm512_set1_epi64(0), v_ht_cell,
          v_factor = _mm512_set1_epi64(ht->hash_mask),
          v_shift = _mm512_set1_epi64(ht->skip_bits), v_cell_hash,
          v_ht_pos = _mm512_set1_epi64(0), v_neg_one512 = _mm512_set1_epi64(-1),
          v_ht_upper, v_zero512 = _mm512_set1_epi64(0),
          v_next_addr = _mm512_set1_epi64(0),
          v_write_index = _mm512_set1_epi64(0),
          v_ht_addr = _mm512_set1_epi64(ht->buckets),
          v_word_size = _mm512_set1_epi64(WORDSIZE),
          v_tuple_size = _mm512_set1_epi64(sizeof(tuple_t)),
          v_bucket_size = _mm512_set1_epi64(sizeof(bucket_t)),
          v_next_off = _mm512_set1_epi64(8), v_left_payload, v_right_payload,
          v_payload_off = _mm512_set1_epi64(24);
  chainedtuplebuffer_t *chainedbuf = (chainedtuplebuffer_t *)output;

  uint64_t cur_offset = 0, vector_scale = 8, base_off[16], *left_payload,
           *right_payload;
  left_payload = (uint64_t *)&v_left_payload;
  right_payload = (uint64_t *)&v_right_payload;

  for (int i = 0; i <= vector_scale; ++i) {
    base_off[i] = i * sizeof(tuple_t);
  }
  v_base_offset = _mm512_load_epi64(base_off);
  for (uint64_t cur = 0; cur < rel->num_tuples || m_have_tuple;) {
///////// step 1: load new tuples' address offsets
// the offset should be within MAX_32INT_
// the tail depends on the number of joins and tuples in each bucket
#if !SEQPREFETCH
    _mm_prefetch((char *)(((void *)rel->tuples) + cur_offset + PDIS),
                 _MM_HINT_T0);
    _mm_prefetch((char *)(((void *)rel->tuples) + cur_offset + PDIS + 64),
                 _MM_HINT_T0);
    _mm_prefetch((char *)(((void *)rel->tuples) + cur_offset + PDIS + 128),
                 _MM_HINT_T0);
    _mm_prefetch((char *)(((void *)rel->tuples) + cur_offset + PDIS + 192),
                 _MM_HINT_T0);
    _mm_prefetch((char *)(((void *)rel->tuples) + cur_offset + PDIS + 256),
                 _MM_HINT_T0);
#endif
    // directly use cur, instead of cur_offset to control the offset to rel.
    // In this case, using step = 16 to gather data, but step is larger
    // than the scale 1,2,4 or 8
    v_offset = _mm512_add_epi64(_mm512_set1_epi64(cur_offset), v_base_offset);
    v_addr_offset = _mm512_mask_expand_epi64(
        v_addr_offset, _mm512_knot(m_have_tuple), v_offset);
    // count the number of empty tuples
    m_new_cells = _mm512_knot(m_have_tuple);
    new_add = _mm_popcnt_u32(m_new_cells);
    cur_offset = cur_offset + base_off[new_add];
    cur = cur + new_add;
    m_have_tuple = _mm512_cmpgt_epi64_mask(v_base_offset_upper, v_addr_offset);
    ///// step 2: load new cells from right tuples;
    m_new_cells = _mm512_kand(m_new_cells, m_have_tuple);
    // maybe need offset within a tuple
    v_tuple_cell = _mm512_mask_i64gather_epi64(
        v_tuple_cell, m_new_cells, v_addr_offset, ((void *)rel->tuples), 1);
    ///// step 3: load new values from hash tables;
    // hash the cell values
    v_cell_hash = _mm512_and_epi64(v_tuple_cell, v_factor);
    v_cell_hash = _mm512_srlv_epi64(v_cell_hash, v_shift);
    v_cell_hash = _mm512_mullo_epi64(v_cell_hash, v_bucket_size);
    v_ht_pos =
        _mm512_mask_add_epi64(v_next_addr, m_new_cells, v_cell_hash, v_ht_addr);

    /////////////////// random access
    v_ht_cell = _mm512_mask_i64gather_epi64(
        v_neg_one512, m_have_tuple, _mm512_add_epi64(v_ht_pos, v_tuple_size), 0,
        1);  // note the offset of the tuple in %bucket_t%

    ///// step 4: compare;
    m_match = _mm512_cmpeq_epi64_mask(v_tuple_cell, v_ht_cell);
    m_match = _mm512_kand(m_match, m_have_tuple);
    new_add = _mm_popcnt_u32(m_match);
    matches += new_add;
    v_next_addr = _mm512_mask_i64gather_epi64(
        v_zero512, m_have_tuple, _mm512_add_epi64(v_ht_pos, v_next_off), 0, 1);
    m_have_tuple = _mm512_kand(_mm512_cmpneq_epi64_mask(v_next_addr, v_zero512),
                               m_have_tuple);
    v_left_payload = _mm512_mask_i64gather_epi64(
        v_neg_one512, m_match, _mm512_add_epi64(v_addr_offset, v_word_size),
        ((void *)rel->tuples), 1);
    v_right_payload = _mm512_mask_i64gather_epi64(
        v_neg_one512, m_match, _mm512_add_epi64(v_ht_pos, v_payload_off), 0, 1);
// to scatter
#if !SCATTER
    tuple_t *joinres = cb_next_n_writepos(chainedbuf, new_add);
    v_write_index = _mm512_mask_expand_epi64(v_zero512, m_match, v_base_offset);
    _mm512_mask_i64scatter_epi64((void *)joinres, m_match, v_write_index,
                                 v_left_payload, 1);
    v_write_index = _mm512_add_epi64(v_write_index, v_word_size);
    _mm512_mask_i64scatter_epi64((void *)joinres, m_match, v_write_index,
                                 v_right_payload, 1);
#else
    for (int i = 0; (i < vector_scale) && m_match;
         ++i, m_match = (m_match >> 1)) {
      if (m_match & 1) {
        tuple_t *joinres = cb_next_writepos(chainedbuf);
        joinres->key = left_payload[i];
        joinres->payload = right_payload[i];
      }
    }
#endif
  }
  return matches;
}
#endif
