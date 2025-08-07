#pragma once


#ifdef __cplusplus
extern "C" {
#endif

#include "../inc/common.h"


#ifndef BUCKET_CAPACITY
#define BUCKET_CAPACITY 2
#endif /* BUCKET_CAPACITY */


typedef struct bucket_t {
	lock_t latch;
	int count;
	tuple_t tuples[BUCKET_CAPACITY];
	struct bucket_t *next;
} __attribute__( ( aligned( CACHELINE_SIZE ) ) ) bucket_t;

typedef struct {
	bucket_t *buckets;
	size_t num_buckets;
} hashtable_t;

typedef struct bucket_buffer_t bucket_buffer_t;
struct bucket_buffer_t {
	bucket_buffer_t *next;
	int count;
	bucket_t buf[OVERFLOW_BUF_SIZE];
};


static inline void init_bucket_buffer(bucket_buffer_t ** ppbuf,
	numa_cfg_t *numa_cfg) {
	bucket_buffer_t * overflowbuf;
	overflowbuf = (bucket_buffer_t *) alloc_weighted(sizeof(bucket_buffer_t), numa_cfg);
	overflowbuf->count = 0;
	overflowbuf->next = NULL;
	*ppbuf = overflowbuf;
}

static inline void get_new_bucket(bucket_t ** result, bucket_buffer_t ** buf,
	numa_cfg_t *numa_cfg) {
	if( (*buf)->count < OVERFLOW_BUF_SIZE ) {
		*result = (*buf)->buf + (*buf)->count;
		(*buf)->count ++;
	} else {
		/* need to allocate new buffer */
		bucket_buffer_t * new_buf = (bucket_buffer_t*) alloc_weighted(sizeof(bucket_buffer_t), numa_cfg);
		new_buf->count = 1;
		new_buf->next  = *buf;
		*buf	= new_buf;
		*result = new_buf->buf;
	}
}

static inline void free_bucket_buffer(bucket_buffer_t *overflowbuf) {
	size_t size = sizeof(bucket_buffer_t);
	do {
		bucket_buffer_t * tmp = overflowbuf->next;
		dealloc_weighted(overflowbuf, size);
		overflowbuf = tmp;
	} while (overflowbuf);
}


void nphj_sc(const datameta_t*, timekeeper_t * const);
// void aux_nphj_sc(const datameta_t*, timekeeper_t * const);
// void hc_nphj_sc(const datameta_t*, timekeeper_t * const);


#ifdef __cplusplus
}
#endif