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
#include "types.h"
#include "npj_types.h"
typedef struct amac_state_t amac_state_t;

#define AMACBufferSize 30
#define SEQ_DIS 30
#define MULTI_TUPLE (BUCKET_SIZE - 1)
#define REPEAT_PROBE 2
#define SLEEP_TIME 0
struct amac_state_t {
  int64_t tuple_id;
  bucket_t* b;
  int16_t stage;
};

#endif /* SRC_PREFETCH_H_ */
