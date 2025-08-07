#pragma once


#ifdef __cplusplus
extern "C" {
#endif

#include "../inc/common.h"
#include "../inc/memaccess.h"

#define FANOUT_PASS_1 ( 1 << (NUM_RADIX_BIT / NUM_PASS) )
#define FANOUT_PASS_2 ( 1 << (NUM_RADIX_BIT - (NUM_RADIX_BIT / NUM_PASS)) )
#define PADDING_UNIT_NUM (SWWCB_SIZE / TUPLE_T_SIZE)
#define TUPLE_PER_SWWCB PADDING_UNIT_NUM


typedef struct part_t {
	int _tid;

	my_cnt_t hashmask;
	my_cnt_t bitskip;

	relation_t rel;
	relation_t tmp;

	my_cnt_t **hist;
	my_cnt_t *output_offset;

	// short rel_flag;		/* 0: R, 1: S */
} part_t;


typedef union {
	struct {
		tuple_t tuples[TUPLE_PER_SWWCB];
	} tuples;
	struct {
		tuple_t tuples[TUPLE_PER_SWWCB-1];
		my_cnt_t slot;
	} data;
} swwcb_t;


typedef struct task_t task_t;
typedef struct task_list_t task_list_t;
typedef struct task_queue_t task_queue_t;

struct task_t {
	relation_t r_rel;
	// relation_t r_tmp;
	relation_t s_rel;
	// relation_t s_tmp;
	task_t *next;

#if NUM_PASS != 1
	my_cnt_t r_global_offset;
	my_cnt_t s_global_offset;
#endif /* NUM_PASS != 1 */
};

struct task_list_t {
	task_t *tasks;
	task_list_t *next;
	my_cnt_t curr;
};

struct task_queue_t {
	task_t *head;
	task_list_t *free_list;
	my_cnt_t count;
	my_cnt_t alloc_num;

	// Unused due to OpenMP parallelization
	// lock_t lock;
	// lock_t alloc_lock;
};


static inline task_queue_t * task_queue_init(size_t alloc_num) {
	task_queue_t *ret = (task_queue_t *) malloc(sizeof(task_queue_t));
	ret->free_list = (task_list_t *) malloc(sizeof(task_list_t));
	ret->free_list->tasks = (task_t *) malloc(alloc_num * sizeof(task_t));	// could be modified to alloc on NVM
	ret->free_list->curr = 0;
	ret->free_list->next = NULL;
	ret->count = 0;
	ret->alloc_num = alloc_num;
	ret->head = NULL;
	// ret->lock = 0;
	// ret->alloc_lock = 0;

	return ret;
}

static inline void task_queue_free(task_queue_t *tq) {
	task_list_t *tmp = tq->free_list;
	while(tmp) {
		free(tmp->tasks);													// dealloc_memory(tmp->tasks, sizeof(task_t) * tmp->alloc_num)
		task_list_t *tmp2 = tmp->next;
		free(tmp);
		tmp = tmp2;
	}
	free(tq);
}

static inline void task_queue_add(task_queue_t *tq, task_t *t) {
	t->next = tq->head;
	tq->head = t;
	(tq->count) ++;
}

static inline task_t * task_queue_get_slot(task_queue_t *tq) {
	task_list_t *l = tq->free_list;
	task_t *ret;
	if(l->curr < tq->alloc_num) {
		ret = &(l->tasks[l->curr]);
		(l->curr) ++;
	} else {
		task_list_t * nl = (task_list_t *) malloc(sizeof(task_list_t));
		nl->tasks = (task_t *) malloc(tq->alloc_num * sizeof(task_t));		// could be modified to alloc on NVM
		nl->curr = 1;
		nl->next = tq->free_list;
		tq->free_list = nl;
		ret = &(nl->tasks[0]);
	}

	return ret;
}

static inline void nontemp_store_swwcb(void *dst, void *src) {
#if SWWCB_SIZE == 64
	write_nt_64(dst, src);
#elif SWWCB_SIZE == 128
	memcpy_nt_128(dst, src);
#elif SWWCB_SIZE == 256
	memcpy_nt_256(dst, src);
#elif SWWCB_SIZE == 512
	memcpy_nt_512(dst, src);
#elif SWWCB_SIZE == 1024
	memcpy_nt_1024(dst, src);
#elif SWWCB_SIZE == 2048
	memcpy_nt_2048(dst, src);
#elif SWWCB_SIZE == 4096
	memcpy_nt_2048(dst, src);
	memcpy_nt_2048(dst+2048, src+2048);
#elif SWWCB_SIZE == 8192
	memcpy_nt_2048(dst, src);
	memcpy_nt_2048(dst+2048, src+2048);
	memcpy_nt_2048(dst+4096, src+4096);
	memcpy_nt_2048(dst+6144, src+6144);
#else 	/* default setting*/

#ifdef USE_NVM
	/* NVM default setting, 256B */
	memcpy_nt_256(dst, src);
#else /* USE_NVM */
	/* DRAM default setting, 64B */
	write_nt_64(dst, src);
#endif /* USE_NVM */

#endif /* SWWCB_SIZE == 64 */

#ifdef ENABLE_FENCE
	sfence();
#endif /* ENABLE_FENCE */
}

static inline void bc_join(relation_t *r_rel, relation_t *s_rel,
	void *intermediate, my_cnt_t *matched_cnt, my_cnt_t *checksum) {
	my_cnt_t max_hashkey = NEXT_POW_2(r_rel->tuple_num);
	my_cnt_t hashmask = (max_hashkey - 1) << NUM_RADIX_BIT;
	my_cnt_t tmp_hashkey;

	memset(intermediate, 0, MY_CNT_T_SIZE * max_hashkey + MY_CNT_T_SIZE * r_rel->tuple_num);
	my_cnt_t *bucket = (my_cnt_t *) intermediate;
	my_cnt_t *next = (my_cnt_t *) (intermediate + MY_CNT_T_SIZE * max_hashkey);

	for (my_cnt_t idx = 0; idx < r_rel->tuple_num;) {
		tmp_hashkey = IDHASH( r_rel->tuples[idx].key, hashmask, NUM_RADIX_BIT);
		next[idx] = bucket[tmp_hashkey];

		/* we start pos's from 1 instead of 0 */
		bucket[tmp_hashkey] = ++ idx;
	}

	short matched_flag;

	for (my_cnt_t idx = 0; idx < s_rel->tuple_num; idx ++) {
		tmp_hashkey = IDHASH(s_rel->tuples[idx].key, hashmask, NUM_RADIX_BIT);

		for (my_cnt_t hit = bucket[tmp_hashkey]; hit > 0; hit = next[hit-1]) {
			matched_flag = (s_rel->tuples[idx].key == r_rel->tuples[hit-1].key);
			(*matched_cnt) += matched_flag;
			(*checksum) += (r_rel->tuples[hit-1].row_id + s_rel->tuples[idx].row_id) * matched_flag;
		}
	}

}


static inline void rdx_part_swwcb(part_t *part, swwcb_t *swwcb,
	my_cnt_t padding_num, my_cnt_t global_offset) {
	my_cnt_t hashmask = (part->hashmask) << part->bitskip;
	my_cnt_t bitskip = part->bitskip;

	my_cnt_t fanout = part->hashmask + 1;
	my_cnt_t **hist = part->hist;
	my_cnt_t *myhist = part->hist[part->_tid];
	my_cnt_t *output_offset = part->output_offset;

	relation_t rel = part->rel;
	relation_t tmp = part->tmp;

	/* compute the local histogram */
	my_cnt_t tmp_hashkey;
	for (my_cnt_t idx = 0; idx < rel.tuple_num; idx ++) {
		tmp_hashkey = IDHASH(rel.tuples[idx].key, hashmask, bitskip);
		/* tuple count of current partition of current thread */
		myhist[tmp_hashkey*2] ++;
	}

	/* compute the local prefix sum on histogram */
	my_cnt_t accum_num = 0;
	for (my_cnt_t idx = 0; idx < fanout; idx ++) {
		accum_num += myhist[idx*2];
		/* tuple count prefix sum of current partition of current thread */
		myhist[idx*2+1] = accum_num;
	}

	#pragma omp barrier

	/* determine the start and end of each SWWCB */
	for (my_cnt_t idx = 0; idx < part->_tid; idx ++) {
		for (my_cnt_t jdx = 0; jdx < fanout; jdx ++) {
			/* tuple count prefix sum of current partition of all threads */
			output_offset[jdx] += hist[idx][jdx*2+1];
		}
	}

	for (my_cnt_t idx = part->_tid; idx < THREAD_NUM; idx ++) {
		for (my_cnt_t jdx = 1; jdx < fanout; jdx ++) {
			/* tuple count prefix sum of current partition of all threads */
			output_offset[jdx] += hist[idx][(jdx-1)*2+1];
		}
	}

	/* initial updates on SWWCB slots */
	for (my_cnt_t idx = 0; idx < fanout; idx ++) {
		output_offset[idx] += idx * padding_num;
		swwcb[idx].data.slot = output_offset[idx] + global_offset;
	}

	/* copy tuples to their corresponding SWWCBs */
	my_cnt_t slot;
	my_cnt_t slot_mod;
	my_cnt_t remainder_start_pos;
	tuple_t *tmp_swwcb;
	for (my_cnt_t idx = 0; idx < rel.tuple_num; idx ++) {
		tmp_hashkey = IDHASH(rel.tuples[idx].key, hashmask, bitskip);
		slot = swwcb[tmp_hashkey].data.slot;
		tmp_swwcb = (tuple_t *) (swwcb + tmp_hashkey);
		slot_mod = slot & (TUPLE_PER_SWWCB - 1);
		tmp_swwcb[slot_mod] = rel.tuples[idx];

		if (slot_mod == TUPLE_PER_SWWCB - 1) {
			/* non-temporal store a SWWCB */
			nontemp_store_swwcb( 
				tmp.tuples+slot-(TUPLE_PER_SWWCB-1)-global_offset, 
				tmp_swwcb
			);
		}

		swwcb[tmp_hashkey].data.slot = slot + 1;
	}

	/* write out the remainders in the swwcbs */
	for (my_cnt_t idx = 0; idx < fanout; idx ++) {
		slot = swwcb[idx].data.slot;
		slot_mod = slot & (TUPLE_PER_SWWCB - 1);
		slot -= slot_mod;

		remainder_start_pos = (slot < output_offset[idx] + global_offset ) ? (output_offset[idx] + global_offset - slot) : 0;
		for (my_cnt_t jdx = remainder_start_pos; jdx < slot_mod; jdx ++) {
			memcpy(
				tmp.tuples+slot+jdx-global_offset,
				swwcb[idx].data.tuples+jdx,
				TUPLE_T_SIZE
			);
		}
	}
}


static inline void rdx_part(part_t *part, my_cnt_t padding_num) {
	my_cnt_t hashmask = (part->hashmask) << part->bitskip;
	my_cnt_t bitskip = part->bitskip;

	my_cnt_t fanout = part->hashmask + 1;
	my_cnt_t **hist = part->hist;
	my_cnt_t *myhist = part->hist[part->_tid];
	my_cnt_t *output_offset = part->output_offset;

	relation_t rel = part->rel;
	relation_t tmp = part->tmp;

	/* compute the local histogram */
	my_cnt_t tmp_hashkey;
	for (my_cnt_t idx = 0; idx < rel.tuple_num; idx ++) {
		tmp_hashkey = IDHASH(rel.tuples[idx].key, hashmask, bitskip);
		/* tuple count of current partition of current thread */
		myhist[tmp_hashkey*2] ++;
	}

	/* compute the local prefix sum on histogram */
	my_cnt_t accum_num = 0;
	for (my_cnt_t idx = 0; idx < fanout; idx ++) {
		accum_num += myhist[idx*2];
		/* tuple count prefix sum of current partition of current thread */
		myhist[idx*2+1] = accum_num;
	}

	#pragma omp barrier

	/* determine the start and end of each cluster */
	for (my_cnt_t idx = 0; idx < part->_tid; idx ++) {
		for (my_cnt_t jdx = 0; jdx < fanout; jdx ++) {
			/* tuple count prefix sum of current partition of all threads */
			output_offset[jdx] += hist[idx][jdx*2+1];
		}
	}

	for (my_cnt_t idx = part->_tid; idx < THREAD_NUM; idx ++) {
		for (my_cnt_t jdx = 1; jdx < fanout; jdx ++) {
			/* tuple count prefix sum of current partition of all threads */
			output_offset[jdx] += hist[idx][(jdx-1)*2+1] ;
		}
	}


	/* copy tuples to their corresponding clusters */
	for (my_cnt_t idx = 0; idx < rel.tuple_num; idx ++) {
		tmp_hashkey = IDHASH(rel.tuples[idx].key, hashmask, bitskip);
		memcpy(
			tmp.tuples + output_offset[tmp_hashkey] + tmp_hashkey * padding_num,
			rel.tuples + idx,
			TUPLE_T_SIZE
		);
		output_offset[tmp_hashkey] ++;
	}

	/* move back the output_offset pointer to the starting address */
	/* this code segments could be moved to main and only be executed for the 1st thread */
	for (my_cnt_t idx = 0; idx < fanout; idx ++) {
		output_offset[idx] -= myhist[idx*2];
		output_offset[idx] += idx * padding_num;
	}
}



void phj_rdx_bc(const datameta_t*, timekeeper_t * const);
// void aux_phj_rdx_bc(const datameta_t*, timekeeper_t * const);
// void hc_phj_rdx_bc(const datameta_t*, timekeeper_t * const);


#ifdef __cplusplus
}
#endif