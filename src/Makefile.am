AUTOMAKE_OPTIONS = foreign

# Enable PERSIST_RELATIONS and JOIN_RESULT_MATERIALIZE to write tables to file.
DEFINES = #-DPERSIST_RELATIONS -DJOIN_RESULT_MATERIALIZE
MYLDFLAGS = -lnuma
MISC = 

if KEY_8B
DEFINES += -DKEY_8B
endif

if PERF_COUNTERS
DEFINES += -DPERF_COUNTERS
MYLDFLAGS += -lperf
MISC += -I../lib/intel-pcm-1.7/ -L../lib/intel-pcm-1.7/lib/
LIBS += -lperf
endif

if DEBUG
DEFINES += -DDEBUG
endif

if PADDEDBUCKET
DEFINES += -DPADDED_BUCKET=1
else
DEFINES += -DPADDED_BUCKET=0
endif

if NOTIMING
DEFINES += -DNO_TIMING
endif

if SYNCSTATS
DEFINES += -DSYNCSTATS
endif

if SKEW_HANDLING
DEFINES += -DSKEW_HANDLING
endif

if PREFETCH_NPJ
DEFINES += -DPREFETCH_NPJ
endif

if USE_SWWC_OPTIMIZED_PART
DEFINES += -DUSE_SWWC_OPTIMIZED_PART
endif

if KNL
DEFINES += -DKNL=1 -mavx512f -mavx512pf -mavx2 -mbmi2 -mavx
else
DEFINES += -DKNL=0 -mavx512f -mavx512cd -mavx512bw -mavx512dq -mavx2 -mbmi2 -mavx
endif
# if available, add the following flags
# -mavx, -mtune=niagara2, -mcpu=ultrasparc -msse4.2
AM_CFLAGS = -Wall -ansi -std=c11 -Wno-format -pedantic \
	    -g  $(DEFINES) -w -fpermissive 

AM_LDFLAGS =
CFLAGS = -O3

bin_PROGRAMS = mchashjoins
mchashjoins_SOURCES =  	npj_params.h prj_params.h types.h npj_types.h 	\
			no_partitioning_join.h no_partitioning_join.c 	\
			parallel_radix_join.h parallel_radix_join.c   	\
			no_partitioning_join_simd_prefetching.c  tree_binary.c\
			perf_counters.h perf_counters.c	tree_binary_smv.c	pipeline_smv.c	\
			cpu_mapping.h cpu_mapping.c 	pipeline.c		\
			genzipf.h genzipf.c generator.h generator.c 	\
			lock.h rdtsc.h task_queue.h barrier.h affinity.h\
			tuple_buffer.h		prefetch.h		tree_node.h	\
			main.c 

mchashjoins_LDFLAGS = $(AM_LDFLAGS) $(MYLDFLAGS) -pthread
mchashjoins_CFLAGS  = $(AM_CFLAGS) $(MISC)
