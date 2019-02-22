#include "no_partitioning_join.h"

int64_t pipeline_raw(hashtable_t *ht, relation_t *rel, void *output) {
  uint32_t i, j;
  int64_t matches;
  const uint32_t hashmask = ht->hash_mask;
  const uint32_t skipbits = ht->skip_bits;
  matches = 0;

#ifdef JOIN_RESULT_MATERIALIZE
  chainedtuplebuffer_t *chainedbuf = (chainedtuplebuffer_t *)output;
#endif

  for (i = 0; i < rel->num_tuples; i++) {
    // filter
    if (rel->tuples[i].key * A < B) {
      continue;
    }
    // probe
    intkey_t idx = HASH(rel->tuples[i].key, hashmask, skipbits);
    bucket_t *b = ht->buckets + idx;

    do {
      if (b->count == 0) {
        break;
      }
      // match + count
      if (rel->tuples[i].key == b->tuples[0].key) {
        matches++;
#if 0
        /* copy to the result buffer */
        tuple_t *joinres = cb_next_writepos(chainedbuf);
        joinres->key = b->tuples[0].payload;       /* R-rid */
        joinres->payload = rel->tuples[i].payload; /* S-rid */
#endif
      }
      b = b->next; /* follow overflow pointer */
    } while (b);
  }

  return matches;
}

int64_t pipeline_AMAC(hashtable_t *ht, relation_t *rel, void *output) {
  int64_t matches = 0;
  int16_t k = 0, done = 0, j = 0;
  scalar_state_t state[ScalarStateSize];
  const uint32_t hashmask = ht->hash_mask;
  const uint32_t skipbits = ht->skip_bits;
  chainedtuplebuffer_t *chainedbuf = (chainedtuplebuffer_t *)output;

  // init # of the state
  for (int i = 0; i < ScalarStateSize; ++i) {
    state[i].stage = 1;
  }
  for (uint64_t cur = 0; (cur < rel->num_tuples) || (done < ScalarStateSize);) {
    k = (k >= ScalarStateSize) ? 0 : k;

    switch (state[k].stage) {
      case 1: {
        if (cur >= rel->num_tuples) {
          ++done;
          state[k].stage = 3;
          ++k;
          break;
        }
#if SEQPREFETCH
        _mm_prefetch((char *)(rel->tuples + cur) + PDIS, _MM_HINT_T0);
#endif
        // filter
        if (rel->tuples[cur].key * A < B) {
          ++cur;
          continue;
        }
        // probe
        intkey_t idx = HASH(rel->tuples[cur].key, hashmask, skipbits);
        state[k].b = ht->buckets + idx;
        //__builtin_prefetch(state[k].b, 0, 1);
        _mm_prefetch((char *)(state[k].b), _MM_HINT_T0);

        state[k].tuple_id = cur;
        state[k].stage = 0;
        ++cur;
        ++k;
      } break;
      case 0: {
        bucket_t *b = state[k].b;
//  _mm_lfence();
//#pragma unroll(2)
#if MULTI_TUPLE
        for (int16_t j = 0; j < b->count; ++j) {
#else
        if (b->count == 0) {
          state[k].stage = 1;
          // ++k;
          break;
        }
#endif
          if (rel->tuples[state[k].tuple_id].key == b->tuples[0].key) {
            ++matches;
#if 0
            /* copy to the result buffer */
            tuple_t *joinres = cb_next_writepos(chainedbuf);
#if SEQPREFETCH
            _mm_prefetch((char *)(((void *)joinres) + PDIS), _MM_HINT_T0);
#endif
            joinres->key = b->tuples[0].payload; /* R-rid */
            joinres->payload =
                rel->tuples[state[k].tuple_id].payload; /* S-rid */
#endif
          }
#if MULTI_TUPLE
        }
#endif
        b = b->next; /* follow overflow pointer */
        if (b) {
          state[k].b = b;
          // __builtin_prefetch(state[k].b, 0, 1);
          _mm_prefetch((char *)(state[k].b), _MM_HINT_T0);

          ++k;
        } else {
          state[k].stage = 1;
          //++k;
        }
      } break;
      default: { ++k; }
    }
  }

  return matches;
}

/**
 * Just a wrapper to call the build and probe for each thread.
 *
 * @param param the parameters of the thread, i.e. tid, ht, reln, ...
 *
 * @return
 */
volatile char g_lock;
volatile uint64_t total_num;
void *pipeline_thread(void *param) {
  int rv;
  total_num = 0;
  arg_t *args = (arg_t *)param;
  struct timeval t1, t2;
  int deltaT = 0;
  /* allocate overflow buffer for each thread */
  bucket_buffer_t *overflowbuf;
  init_bucket_buffer(&overflowbuf);

#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_initPerformanceMonitor(NULL, NULL);
    PCM_start();
  }
#endif

  /* wait at a barrier until each thread starts and start timer */
  BARRIER_ARRIVE(args->barrier, rv);

#ifndef NO_TIMING
  /* the first thread checkpoints the start time */
  if (args->tid == 0) {
    gettimeofday(&args->start, NULL);
    startTimer(&args->timer1);
    startTimer(&args->timer2);
    args->timer3 = 0; /* no partitionig phase */
  }
#endif
  gettimeofday(&t1, NULL);
  /* insert tuples from the assigned part of relR to the ht */
  build_hashtable_mt(args->ht, &args->relR, &overflowbuf);

  /* wait at a barrier until each thread completes build phase */
  BARRIER_ARRIVE(args->barrier, rv);
  if (args->tid == 0) {
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    printf("--------build costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
    print_hashtable(args->ht);
    printf("size of bucket_t = %d\n", sizeof(bucket_t));
  }
#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_stop();
    PCM_log("========== Build phase profiling results ==========\n");
    PCM_printResults();
    PCM_start();
  }
  /* Just to make sure we get consistent performance numbers */
  BARRIER_ARRIVE(args->barrier, rv);
#endif

#ifndef NO_TIMING
  /* build phase finished, thread-0 checkpoints the time */
  if (args->tid == 0) {
    stopTimer(&args->timer2);
  }
#endif
  ////////// compact, do two branches in the integration

  /*chainedtuplebuffer_t *chainedbuf_compact = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_simd_amac_compact2(args->ht, &args->relS, chainedbuf_compact);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---- COMPACT2 probe costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_compact);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  */  //

  chainedtuplebuffer_t *chainedbuf_compact1 = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        pipeline_smv(args->ht, &args->relS, chainedbuf_compact1);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---- SMV pipeline costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_compact1);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////raw GP probe
  /*  chainedtuplebuffer_t *chainedbuf_rp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_hashtable_raw_prefetch(args->ht, &args->relS, chainedbuf_rp);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("------RAW GP probe costs time (ms) = %lf\n", deltaT * 1.0 /
  1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_rp);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  */
  ////////////////GP probe
  /*chainedtuplebuffer_t *chainedbuf_gp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = probe_gp(args->ht, &args->relS, chainedbuf_gp);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-------- GP  probe costs time (ms) = %lf\n", deltaT * 1.0 /
  1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_gp);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  */
  ////////////////AMAC probe
  chainedtuplebuffer_t *chainedbuf_amac = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = pipeline_AMAC(args->ht, &args->relS, chainedbuf_amac);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("--------AMAC pipeline costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_amac);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////SIMD GP probe
  /*
  chainedtuplebuffer_t *chainedbuf_simd_gp = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        probe_simd_gp(args->ht, &args->relS, chainedbuf_simd_gp);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-----SIMD GP probe costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_gp);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  */

  ////////////////SIMD AMAC probe
  chainedtuplebuffer_t *chainedbuf_simd_amac = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        pipeline_simd_amac(args->ht, &args->relS, chainedbuf_simd_amac);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---SIMD AMAC pipeline costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_amac);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  ////////////////SIMD AMAC probe raw
  chainedtuplebuffer_t *chainedbuf_simd_amac_raw = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results =
        pipeline_simd_amac_raw(args->ht, &args->relS, chainedbuf_simd_amac_raw);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("---  SIMD AMAC RAW costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd_amac_raw);
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }
  ////////////////SIMD probe full vectorization
  chainedtuplebuffer_t *chainedbuf_simd = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = pipeline_simd(args->ht, &args->relS, chainedbuf_simd);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("--------SIMD pipeline costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf_simd);
  //////////////////
  if (args->tid == 0) {
    puts("+++++sleep begin+++++");
  }
  sleep(SLEEP_TIME);
  if (args->tid == 0) {
    puts("+++++sleep end  +++++");
  }

  //////// raw probe in scalar
  chainedtuplebuffer_t *chainedbuf = chainedtuplebuffer_init();
  for (int rp = 0; rp < REPEAT_PROBE; ++rp) {
    BARRIER_ARRIVE(args->barrier, rv);
    gettimeofday(&t1, NULL);
    args->num_results = pipeline_raw(args->ht, &args->relS, chainedbuf);
    lock(&g_lock);
    total_num += args->num_results;
    unlock(&g_lock);
    BARRIER_ARRIVE(args->barrier, rv);
    if (args->tid == 0) {
      printf("total result num = %lld\t", total_num);
      gettimeofday(&t2, NULL);
      deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
      printf("-------- RAW pipeline costs time (ms) = %lf\n",
             deltaT * 1.0 / 1000);
      total_num = 0;
    }
  }
  chainedtuplebuffer_free(chainedbuf);

//------------------------------------
#ifdef JOIN_RESULT_MATERIALIZE
  args->threadresult->nresults = args->num_results;
  args->threadresult->threadid = args->tid;
// args->threadresult->results = (void *)chainedbuf;
#endif

#ifndef NO_TIMING

  /* for a reliable timing we have to wait until all finishes */
  BARRIER_ARRIVE(args->barrier, rv);

  /* probe phase finished, thread-0 checkpoints the time */
  if (args->tid == 0) {
    stopTimer(&args->timer1);
    gettimeofday(&args->end, NULL);
  }
#endif

#ifdef PERF_COUNTERS
  if (args->tid == 0) {
    PCM_stop();
    PCM_log("========== Probe phase profiling results ==========\n");
    PCM_printResults();
    PCM_log("===================================================\n");
    PCM_cleanup();
  }
  /* Just to make sure we get consistent performance numbers */
  BARRIER_ARRIVE(args->barrier, rv);
#endif

  /* clean-up the overflow buffers */
  free_bucket_buffer(overflowbuf);

  return 0;
}

result_t *PIPELINE(relation_t *relR, relation_t *relS, int nthreads) {
  hashtable_t *ht;
  int64_t result = 0;
  int32_t numR, numS, numRthr, numSthr; /* total and per thread num */
  int i, rv;
  cpu_set_t set;
  arg_t args[nthreads];
  pthread_t tid[nthreads];
  pthread_attr_t attr;
  pthread_barrier_t barrier;

  result_t *joinresult = 0;
  joinresult = (result_t *)malloc(sizeof(result_t));

#ifdef JOIN_RESULT_MATERIALIZE
  joinresult->resultlist =
      (threadresult_t *)alloc_aligned(sizeof(threadresult_t) * nthreads);
#endif

  uint32_t nbuckets = (relR->num_tuples / BUCKET_SIZE);
  allocate_hashtable(&ht, nbuckets);

  numR = relR->num_tuples;
  numS = relS->num_tuples;
  numRthr = numR / nthreads;
  numSthr = numS / nthreads;

  rv = pthread_barrier_init(&barrier, NULL, nthreads);
  if (rv != 0) {
    printf("Couldn't create the barrier\n");
    exit(EXIT_FAILURE);
  }

  pthread_attr_init(&attr);
  for (i = 0; i < nthreads; i++) {
    int cpu_idx = get_cpu_id(i);

    DEBUGMSG(1, "Assigning thread-%d to CPU-%d\n", i, cpu_idx);

#if AFFINITY
    CPU_ZERO(&set);
    CPU_SET(cpu_idx, &set);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
#endif
    args[i].tid = i;
    args[i].ht = ht;
    args[i].barrier = &barrier;

    /* assing part of the relR for next thread */
    args[i].relR.num_tuples = (i == (nthreads - 1)) ? numR : numRthr;
    args[i].relR.tuples = relR->tuples + numRthr * i;
    numR -= numRthr;

    /* assing part of the relS for next thread */
    args[i].relS.num_tuples = (i == (nthreads - 1)) ? numS : numSthr;
    args[i].relS.tuples = relS->tuples + numSthr * i;
    numS -= numSthr;

    args[i].threadresult = &(joinresult->resultlist[i]);

    rv = pthread_create(&tid[i], &attr, pipeline_thread, (void *)&args[i]);
    if (rv) {
      printf("ERROR; return code from pthread_create() is %d\n", rv);
      exit(-1);
    }
  }

  for (i = 0; i < nthreads; i++) {
    pthread_join(tid[i], NULL);
    /* sum up results */
    result += args[i].num_results;
  }
  joinresult->totalresults = result;
  joinresult->nthreads = nthreads;

  destroy_hashtable(ht);

  return joinresult;
}
