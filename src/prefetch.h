/*
 * Copyright [2012-2015] DaSE@ECNU
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * /multicore-hashjoins-0.2-ICDE13/src/prefetch.h
 *
 *  Created on: Dec 18, 2018
 *      Author: claims
 *		   Email:
 *
 * Description:
 *
 */

#ifndef SRC_PREFETCH_H_
#define SRC_PREFETCH_H_
#include <assert.h>
#include <immintrin.h>
#include "types.h"
#include "npj_types.h"
typedef struct amac_state_t scalar_state_t;
typedef struct StateSIMD StateSIMD;
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define LIKELY(expr) __builtin_expect(!!(expr), 1)

#define ScalarStateSize 1
#define PDIS 640
#define SIMDStateSize 5

#define LOAD_FACTOR 1
#define MULTI_TUPLE (BUCKET_SIZE - 1)
#define REPEAT_PROBE 2
#define SLEEP_TIME 0
#define VECTOR_SCALE 8
#define DIR_PREFETCH 1
#define SEQPREFETCH 1
#define A 1
#define B 10000078
#define SIMD_A _mm512_set1_epi64(A)
#define SIMD_B _mm512_set1_epi64(B)

#if KNL
#define _mm512_mullo_epi64(a, b) _mm512_mullo_epi32(a, b)
#endif
//#define _mm512_mask_i64scatter_epi64(addr, mask, idx, v, scale) \
  _mm512_mask_compressstoreu_epi64(addr, mask, v);
struct amac_state_t {
  int64_t tuple_id;
  bucket_t *b;
  int16_t stage;
};
struct StateSIMD {
  __m512i key;
  __m512i payload;
  __m512i tb_off;
  __m512i ht_off;
  __mmask8 m_have_tuple;
  char stage;
};

#endif /* SRC_PREFETCH_H_ */
