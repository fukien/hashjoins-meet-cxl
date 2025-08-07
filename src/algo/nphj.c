#include "nphj.h"

extern numa_cfg_t numa_cfg_dest, numa_cfg_src;


void nphj_sc(const datameta_t *datameta, timekeeper_t * const timekeeper) {
	struct timespec time_start, time_end;



	/**************** LOAD DATA FROM DISK TO SRC ****************/
	char r_path[CHAR_BUFFER_LEN], s_path[CHAR_BUFFER_LEN];
	snprintf(
		r_path, CHAR_BUFFER_LEN, "%s/%s", 
		DATA_PATH_PREFIX, datameta->r_path_suffix
	);
	snprintf(
		s_path, CHAR_BUFFER_LEN, "%s/%s", 
		DATA_PATH_PREFIX, datameta->s_path_suffix
	);


	tuple_t *r_disk_data = (tuple_t *) map_disk_file(
		r_path, datameta->r_tuple_num * TUPLE_T_SIZE
	);
	tuple_t *s_disk_data = (tuple_t *) map_disk_file(
		s_path, datameta->s_tuple_num * TUPLE_T_SIZE
	);
	tuple_t *r_src_tuples = (tuple_t *) alloc_weighted(
		datameta->r_tuple_num * TUPLE_T_SIZE, &numa_cfg_src
	);
	tuple_t *s_src_tuples = (tuple_t *) alloc_weighted(
		datameta->s_tuple_num * TUPLE_T_SIZE, &numa_cfg_src
	);
	memcpy(r_src_tuples, r_disk_data, 
		datameta->r_tuple_num * TUPLE_T_SIZE
	);
	memcpy(s_src_tuples, s_disk_data, 
		datameta->s_tuple_num * TUPLE_T_SIZE
	);
	munmap(r_disk_data, datameta->r_tuple_num * TUPLE_T_SIZE);
	munmap(s_disk_data, datameta->s_tuple_num * TUPLE_T_SIZE);

	tuple_t *r_dest_tuples = (tuple_t *) alloc_weighted(
		datameta->r_tuple_num * TUPLE_T_SIZE, &numa_cfg_dest
	);
	tuple_t *s_dest_tuples = (tuple_t *) alloc_weighted(
		datameta->s_tuple_num * TUPLE_T_SIZE, &numa_cfg_dest
	);
#ifdef PREFAULT
	memset(r_dest_tuples, 0, datameta->r_tuple_num * TUPLE_T_SIZE);
	memset(s_dest_tuples, 0, datameta->s_tuple_num * TUPLE_T_SIZE);
#endif	/* PREFAULT */

	/**************** MEMCPY DATA FROM SRC TO DEST ****************/
	clock_gettime(CLOCK_REALTIME, &time_start);
	double __attribute__((unused)) r_memcpy_time = openmp_memcpy(
		r_dest_tuples, r_src_tuples, 
		datameta->r_tuple_num * TUPLE_T_SIZE
	);
	double __attribute__((unused)) s_memcpy_time = openmp_memcpy(
		s_dest_tuples, s_src_tuples, 
		datameta->s_tuple_num * TUPLE_T_SIZE
	);

	relation_t r_relation_work = {NULL, 0}, s_relation_work = {NULL, 0};
	r_relation_work.tuples = r_dest_tuples;
	r_relation_work.tuple_num = datameta->r_tuple_num;
	s_relation_work.tuples = s_dest_tuples;
	s_relation_work.tuple_num = datameta->s_tuple_num;


	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->memcpy = diff_sec(time_start, time_end);


	/**************** BUILD PHASE ****************/
	/****** HASHTABLE ALLOCATION ******/
	clock_gettime(CLOCK_REALTIME, &time_start);
	hashtable_t hashtable;
	hashtable.num_buckets = datameta->r_tuple_num / BUCKET_CAPACITY;
	hashtable.buckets = (bucket_t *) alloc_weighted(
		hashtable.num_buckets * sizeof(bucket_t), &numa_cfg_dest
	);

	my_cnt_t hashmask = hashtable.num_buckets - 1;
	my_cnt_t bitskip = 0;
	bucket_buffer_t *overflowbuf[THREAD_NUM];


	/****** HASHTABLE BUILDING ******/
	#pragma omp parallel num_threads(THREAD_NUM)
	{
		int _tid = omp_get_thread_num();
		struct timespec local_time_start, local_time_end;
		double __attribute__((unused)) local_time = 0.0;

#ifdef __linux__
		/**
		 * Assumes each OpenMP thread is pinned to a distinct physical core.
		 * Both numa_cfg_dest and numa_cfg_src use the same set of local cores.
		 */
		openmp_bind_core(_tid, &numa_cfg_dest);
#endif	/* __linux__ */


#ifdef PREFAULT
		size_t memset_num_buckets_thr = hashtable.num_buckets / THREAD_NUM;
		size_t memsset_num_bucket_size = (_tid  == THREAD_NUM - 1) ?
			sizeof(bucket_t) * (hashtable.num_buckets - _tid * memset_num_buckets_thr) :
			sizeof(bucket_t) * memset_num_buckets_thr;
		memset(hashtable.buckets+_tid*memset_num_buckets_thr, 0, memsset_num_bucket_size);
#endif	/* PREFAULT */

		init_bucket_buffer(&overflowbuf[_tid], &numa_cfg_dest);

		#pragma omp barrier
		clock_gettime(CLOCK_REALTIME, &local_time_start);
		tuple_t *dest;
		bucket_t *curr, *nxt;
		my_cnt_t tmp_hashkey;


		my_cnt_t tuple_num = r_relation_work.tuple_num / THREAD_NUM;
		tuple_t *tuples = r_relation_work.tuples + _tid * tuple_num;
		tuple_num = (_tid == THREAD_NUM-1) ?
			r_relation_work.tuple_num - _tid * tuple_num : tuple_num;

#ifdef PREFETCH_DISTANCE
		my_cnt_t prefetch_index = NEXT_POW_2(PREFETCH_DISTANCE);
		my_cnt_t hashkey_prefetch;


		for (my_cnt_t idx = 0; idx < tuple_num; idx ++) {

			if (prefetch_index < tuple_num) {
				hashkey_prefetch = IDHASH(tuples[prefetch_index++].key, hashmask, bitskip);
				__builtin_prefetch(hashtable.buckets + hashkey_prefetch, 1, 1);
			}

			tmp_hashkey = IDHASH(tuples[idx].key, hashmask, bitskip);
			curr = hashtable.buckets + tmp_hashkey;
			lock(&curr->latch);
			nxt = curr->next;

			if(curr->count == BUCKET_CAPACITY) {
				if(!nxt || nxt->count == BUCKET_CAPACITY) {
					bucket_t * b;
					get_new_bucket(&b, &overflowbuf[_tid],
						 &numa_cfg_dest);
					curr->next = b;
					b->next	= nxt;
					b->count = 1;
					dest = b->tuples;
				} else {
					dest = nxt->tuples + nxt->count;
					(nxt->count) ++;
				}
			} else {
				dest = curr->tuples + curr->count;
				(curr->count) ++;
			}

			unlock(&curr->latch);

			memcpy(dest, tuples+idx, TUPLE_T_SIZE);
		}
#else	/* PREFETCH_DISTANCE */
		for (my_cnt_t idx = 0; idx < tuple_num; idx ++) {
			tmp_hashkey = IDHASH(tuples[idx].key, hashmask, bitskip);
			curr = hashtable.buckets + tmp_hashkey;
			lock(&curr->latch);
			nxt = curr->next;

			if(curr->count == BUCKET_CAPACITY) {
				if(!nxt || nxt->count == BUCKET_CAPACITY) {
					bucket_t * b;
					get_new_bucket(&b, &overflowbuf[_tid], 
						&numa_cfg_dest);
					curr->next = b;
					b->next	= nxt;
					b->count = 1;
					dest = b->tuples;
				} else {
					dest = nxt->tuples + nxt->count;
					(nxt->count) ++;
				}
			} else {
				dest = curr->tuples + curr->count;
				(curr->count) ++;
			}

			unlock(&curr->latch);

			memcpy(dest, tuples+idx, TUPLE_T_SIZE);
		}
#endif	/* PREFETCH_DISTANCE */

		clock_gettime(CLOCK_REALTIME, &local_time_end);
		local_time = diff_sec(local_time_start, local_time_end);
	}

	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->buildpart = diff_sec(time_start, time_end);




	/**************** PROBE PHASE ****************/
	clock_gettime(CLOCK_REALTIME, &time_start);
	my_cnt_t matched_cnt = 0;
	my_cnt_t checksum = 0;
	#pragma omp parallel num_threads(THREAD_NUM) reduction(+:matched_cnt, checksum)
	{
		int _tid = omp_get_thread_num();
		struct timespec local_time_start, local_time_end;
		double __attribute__((unused)) local_time = 0.0;

#ifdef __linux__
		/**
		 * Assumes each OpenMP thread is pinned to a distinct physical core.
		 * Both numa_cfg_dest and numa_cfg_src use the same set of local cores.
		 */
		openmp_bind_core(_tid, &numa_cfg_dest);
#endif	/* __linux__ */

		my_cnt_t local_matched_cnt = 0;
		my_cnt_t local_checksum = 0;

		my_cnt_t tmp_hashkey;
		bucket_t *b;

		short matched_flag;

		my_cnt_t tuple_num = s_relation_work.tuple_num / THREAD_NUM;
		tuple_t * tuples = s_relation_work.tuples + _tid * tuple_num;
		tuple_num = (_tid == THREAD_NUM-1) ?
			s_relation_work.tuple_num - _tid * tuple_num : tuple_num;

		clock_gettime(CLOCK_REALTIME, &local_time_start);


#ifdef PREFETCH_DISTANCE
		my_cnt_t prefetch_index = NEXT_POW_2(PREFETCH_DISTANCE);
		my_cnt_t hashkey_prefetch;

		for (my_cnt_t idx = 0; idx < tuple_num; idx ++) {
			if (prefetch_index < tuple_num) {
				hashkey_prefetch = IDHASH(tuples[prefetch_index++].key, hashmask, bitskip);
				__builtin_prefetch(hashtable.buckets + hashkey_prefetch, 0, 1);
			}

			tmp_hashkey = IDHASH(tuples[idx].key, hashmask, bitskip);
			b = hashtable.buckets + tmp_hashkey;

			do {
				for (my_cnt_t jdx = 0; jdx < b->count; jdx++) {
					matched_flag = (tuples[idx].key == b->tuples[jdx].key);
					matched_cnt += matched_flag;
					checksum += (tuples[idx].row_id + b->tuples[jdx].row_id)*matched_flag;
				}
				
				/* follow overflow pointer */
				b = b->next;
			} while (b);
		}
#else	/* PREFETCH_DISTANCE */
		for (my_cnt_t idx = 0; idx < tuple_num; idx ++) {
			tmp_hashkey = IDHASH(tuples[idx].key, hashmask, bitskip);
			b = hashtable.buckets + tmp_hashkey;

			do {
				for (my_cnt_t jdx = 0; jdx < b->count; jdx++) {
					matched_flag = (tuples[idx].key == b->tuples[jdx].key);
					local_matched_cnt += matched_flag;
					local_checksum += (tuples[idx].row_id + b->tuples[jdx].row_id)*matched_flag;
				}

				/* follow overflow pointer */
				b = b->next;
			} while (b);
		}
#endif /* PREFETCH_DISTANCCE */

		clock_gettime(CLOCK_REALTIME, &local_time_end);
		local_time = diff_sec(local_time_start, local_time_end);

		matched_cnt += local_matched_cnt;
		checksum += local_checksum;
	}

	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->probejoin = diff_sec(time_start, time_end);

	timekeeper->total = timekeeper->memcpy + 
		timekeeper->buildpart + timekeeper->probejoin;

	purple();
	printf("matched_cnt: %zu\tcheck_sum: %zu\n", matched_cnt, checksum);

	dealloc_memory(hashtable.buckets, sizeof(bucket_t) * hashtable.num_buckets);
	for (int idx = 0; idx < THREAD_NUM; idx ++) {
		free_bucket_buffer(overflowbuf[idx]);
	}

	munmap(r_src_tuples, datameta->r_tuple_num * TUPLE_T_SIZE);
	munmap(s_src_tuples, datameta->s_tuple_num * TUPLE_T_SIZE);
	munmap(r_dest_tuples, datameta->r_tuple_num * TUPLE_T_SIZE);
	munmap(s_dest_tuples, datameta->s_tuple_num * TUPLE_T_SIZE);
}
