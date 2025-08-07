#include "phj.h"

extern numa_cfg_t numa_cfg_dest, numa_cfg_src;


void phj_rdx_bc(const datameta_t *datameta, timekeeper_t * const timekeeper) {
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
		datameta->r_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ), 
		&numa_cfg_src
	);
	tuple_t *s_src_tuples = (tuple_t *) alloc_weighted(
		datameta->s_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ), 
		&numa_cfg_src
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
		datameta->r_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ),
		&numa_cfg_dest
	);
	tuple_t *s_dest_tuples = (tuple_t *) alloc_weighted(
		datameta->s_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ),
		&numa_cfg_dest
	);

	tuple_t *aux_r_dest_tuples = (tuple_t *) alloc_weighted(
		datameta->r_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ),
		&numa_cfg_dest
	);
	tuple_t *aux_s_dest_tuples = (tuple_t *) alloc_weighted(
		datameta->s_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) ),
		&numa_cfg_dest
	);

#ifdef PREFAULT
	memset(r_dest_tuples, 0, 
		datameta->r_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	memset(s_dest_tuples, 0, 
		datameta->s_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	memset(aux_r_dest_tuples, 0,
		datameta->r_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	memset(aux_s_dest_tuples, 0,
		datameta->s_tuple_num * TUPLE_T_SIZE + \
			SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
#endif	/* PREFAULT */

	my_cnt_t *r_hist[THREAD_NUM], *s_hist[THREAD_NUM];

	for (int i = 0; i < THREAD_NUM; ++i) {
		r_hist[i] = (my_cnt_t *) alloc_weighted(
			sizeof(size_t) * 2 * FANOUT_PASS_1,
			&numa_cfg_dest
		);
		s_hist[i] = (my_cnt_t *) alloc_weighted(
			sizeof(size_t) * 2 * FANOUT_PASS_1,
			&numa_cfg_dest
		);

		if (!r_hist[i] || !s_hist[i]) {
			fprintf(stderr, "Memory allocation failed at thread %d\n", i);
			exit(EXIT_FAILURE);
		}
	}

	task_queue_t *part_queue = task_queue_init(FANOUT_PASS_1);

	my_cnt_t max_build_side_num_tup = 0;


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

	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->memcpy = diff_sec(time_start, time_end);

	/**************** PARTITION ****************/
	clock_gettime(CLOCK_REALTIME, &time_start);

	#pragma omp parallel num_threads(THREAD_NUM)
	{
		int _tid = omp_get_thread_num();
		// struct timespec local_time_start, local_time_end;
		// double __attribute__((unused)) local_time = 0.0;

#ifdef __linux__
		/**
		 * Assumes each OpenMP thread is pinned to a distinct physical core.
		 * Both numa_cfg_dest and numa_cfg_src use the same set of local cores.
		 */
		openmp_bind_core(_tid, &numa_cfg_dest);
#endif	/* __linux__ */

		my_cnt_t *r_output_offset = (my_cnt_t *) alloc_weighted(
			sizeof(my_cnt_t) * FANOUT_PASS_1,
			&numa_cfg_dest
		);
		my_cnt_t *s_output_offset = (my_cnt_t *) alloc_weighted(
			sizeof(my_cnt_t) * FANOUT_PASS_1,
			&numa_cfg_dest
		);
#ifdef	USE_SWWCB
		swwcb_t *swwcb = (swwcb_t *) alloc_weighted(
			SWWCB_SIZE * FANOUT_PASS_1, &numa_cfg_dest
		);
#endif /* USE_SWWCB */

		#pragma omp barrier

		my_cnt_t hashmask = FANOUT_PASS_1 - 1;
		my_cnt_t bitskip = 0; 

		part_t part = {
			._tid = _tid,
			.hashmask = hashmask,
			.bitskip = bitskip,
			.hist = r_hist,
			.output_offset = r_output_offset,
			.rel = {
				.tuples = r_dest_tuples + _tid * (datameta->r_tuple_num / THREAD_NUM),
				.tuple_num = (_tid == THREAD_NUM-1) ?
					datameta->r_tuple_num - _tid * (datameta->r_tuple_num / THREAD_NUM) :
					(datameta->r_tuple_num / THREAD_NUM)
			},
			.tmp = {
				.tuples = aux_r_dest_tuples,
				.tuple_num = datameta->r_tuple_num
			}
		};

#ifdef USE_SWWCB
		rdx_part_swwcb(&part, swwcb, FANOUT_PASS_2 * PADDING_UNIT_NUM, 0);
#else	/* USE_SWWCB */
		rdx_part(&part, FANOUT_PASS_2 * PADDING_UNIT_NUM);
#endif /* USE_SWWCB */

		part.hist = s_hist;
		part.output_offset = s_output_offset;
		part.rel = (relation_t) {
			.tuples = s_dest_tuples + _tid * (datameta->s_tuple_num / THREAD_NUM),
			.tuple_num = (_tid == THREAD_NUM-1) ?
				datameta->s_tuple_num - _tid * (datameta->s_tuple_num / THREAD_NUM) :
				(datameta->s_tuple_num / THREAD_NUM)
		};
		part.tmp = (relation_t) {
			.tuples = aux_s_dest_tuples,
			.tuple_num = datameta->s_tuple_num
		};

#ifdef USE_SWWCB
		rdx_part_swwcb(&part, swwcb, FANOUT_PASS_2 * PADDING_UNIT_NUM, 0);
#else	/* USE_SWWCB */
		rdx_part(&part, FANOUT_PASS_2 * PADDING_UNIT_NUM);
#endif /* USE_SWWCB */

		#pragma omp barrier

		if (_tid == 0) {
			my_cnt_t r_num_tup, s_num_tup;
			for (my_cnt_t idx = 0; idx < FANOUT_PASS_1; idx ++) {

				r_num_tup = (idx == (FANOUT_PASS_1-1)) ?
					datameta->r_tuple_num - r_output_offset[idx] + idx * FANOUT_PASS_2 * PADDING_UNIT_NUM :
					r_output_offset[idx+1] - r_output_offset[idx] - FANOUT_PASS_2 * PADDING_UNIT_NUM;

				s_num_tup = (idx == (FANOUT_PASS_1-1)) ?
					datameta->s_tuple_num - s_output_offset[idx] + idx * FANOUT_PASS_2 * PADDING_UNIT_NUM :
					s_output_offset[idx+1] - s_output_offset[idx] - FANOUT_PASS_2 * PADDING_UNIT_NUM;

				if (r_num_tup > 0 && s_num_tup > 0) {
					task_t *task = task_queue_get_slot(part_queue);

					task->r_rel.tuples = aux_r_dest_tuples + r_output_offset[idx];
					task->r_rel.tuple_num = r_num_tup;
					// task->r_tmp.tuples = r_dest_tuples + r_output_offset[idx];

					task->s_rel.tuples = aux_s_dest_tuples + s_output_offset[idx];
					task->s_rel.tuple_num = s_num_tup;
					// task->s_tmp.tuples = s_dest_tuples + s_output_offset[idx];

					task_queue_add(part_queue, task);

					max_build_side_num_tup = max_build_side_num_tup >= r_num_tup ? max_build_side_num_tup : r_num_tup;
				}
			}
		}

		munmap(r_output_offset, sizeof(my_cnt_t) * FANOUT_PASS_1);
		munmap(s_output_offset, sizeof(my_cnt_t) * FANOUT_PASS_1);
#ifdef	USE_SWWCB
		munmap(swwcb, SWWCB_SIZE * FANOUT_PASS_1);
#endif /* USE_SWWCB */
	}


	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->buildpart = diff_sec(time_start, time_end);

	/**************** JOIN ****************/
	clock_gettime(CLOCK_REALTIME, &time_start);
	size_t intermediate_size = MY_CNT_T_SIZE * NEXT_POW_2(max_build_side_num_tup) * INTM_SCALE_FACTOR;
	my_cnt_t matched_cnt = 0, checksum = 0;

	#pragma omp parallel num_threads(THREAD_NUM) reduction(+:matched_cnt, checksum)
	{
		int _tid = omp_get_thread_num();
		// struct timespec local_time_start, local_time_end;
		// double __attribute__((unused)) local_time = 0.0;

#ifdef __linux__
		/**
		 * Assumes each OpenMP thread is pinned to a distinct physical core.
		 * Both numa_cfg_dest and numa_cfg_src use the same set of local cores.
		 */
		openmp_bind_core(_tid, &numa_cfg_dest);
#endif	/* __linux__ */

		my_cnt_t local_matched_cnt = 0;
		my_cnt_t local_checksum = 0;

		void *intermediate = alloc_weighted(intermediate_size, &numa_cfg_dest);
		memset(intermediate, 0, intermediate_size);

		task_t *task = NULL;
		#pragma omp critical
		{
			if(part_queue->count > 0) {
				task = part_queue->head;
				part_queue->head = task->next;
				(part_queue->count) --;
			}
		}

		while (task != NULL) {
			bc_join(
				&(task->r_rel), &(task->s_rel), intermediate,
				&local_matched_cnt, &local_checksum
			);
			#pragma omp critical
			{
				task = NULL;
				if(part_queue->count > 0) {
					task = part_queue->head;
					part_queue->head = task->next;
					(part_queue->count) --;
				}
			}
		}

		munmap(intermediate, intermediate_size);

		matched_cnt += local_matched_cnt;
		checksum += local_checksum;
	}

	clock_gettime(CLOCK_REALTIME, &time_end);
	timekeeper->probejoin = diff_sec(time_start, time_end);

	timekeeper->total = timekeeper->memcpy + 
		timekeeper->buildpart + timekeeper->probejoin;

	purple();
	printf("matched_cnt: %zu\tcheck_sum: %zu\n", matched_cnt, checksum);

	task_queue_free(part_queue);

	for (int i = 0; i < THREAD_NUM; ++i) {
		munmap(r_hist[i], sizeof(size_t) * 2 * FANOUT_PASS_1);
		munmap(s_hist[i], sizeof(size_t) * 2 * FANOUT_PASS_1);
		r_hist[i] = NULL;
		s_hist[i] = NULL;
	}

	munmap(r_src_tuples, datameta->r_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	munmap(s_src_tuples, datameta->s_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	munmap(r_dest_tuples, datameta->r_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	munmap(s_dest_tuples, datameta->s_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	munmap(aux_r_dest_tuples, datameta->r_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
	munmap(aux_s_dest_tuples, datameta->s_tuple_num * TUPLE_T_SIZE + \
		SWWCB_SIZE * ( (size_t) (1 << NUM_RADIX_BIT) )
	);
}

