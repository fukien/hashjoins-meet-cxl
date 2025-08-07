#include "common.h"

numa_cfg_t numa_cfg_dest, numa_cfg_src, numa_cfg_aux;
numa_cfg_t numa_cfg_dest_r, numa_cfg_dest_s;
double buildpart_dram_ratio = 0.0, probejoin_dram_ratio = 0.0;


void cfg_print() {
	printf("CONFIGURATION:");
	printf("\tRUN_NUM: %d", RUN_NUM);
	printf("\tTHREAD_NUM: %d", THREAD_NUM);
#ifdef USE_HYPERTHREADING
	printf("\tUSE_HYPERTHREADING");
#endif	/* USE_HYPERTHREADING */
#ifdef USE_WEIGHTED_INTERLEAVING
	printf("\tUSE_WEIGHTED_INTERLEAVING");
#endif	/* USE_WEIGHTED_INTERLEAVING */
#ifdef USE_HUGE
	printf("\tUSE_HUGE");
#endif	/* USE_HUGE */
#ifdef PREFAULT
	printf("\tPREFAULT");
#endif	/* PREFAULT */
#ifdef NTMEMCPY
	printf("\tNTMEMCPY");
#endif	/* NTMEMCPY */
#ifdef PREFETCH_DISTANCE
	printf("\tPREFETCH_DISTANCE: %d", PREFETCH_DISTANCE);
#endif	/* PREFETCH_DISTANCE */
#ifdef OVERFLOW_BUF_SIZE
	printf("\tOVERFLOW_BUF_SIZE: %d", OVERFLOW_BUF_SIZE);
#endif	/* OVERFLOW_BUF_SIZE */
#ifdef USE_SWWCB
	printf("\tSWWCB_SIZE: %d", SWWCB_SIZE);
#endif	/* USE_SWWCB */
#ifdef NUM_RADIX_BIT
	printf("\tNUM_RADIX_BIT: %d", NUM_RADIX_BIT);
#endif	/* NUM_RADIX_BIT */
#ifdef NUM_PASS
	printf("\tNUM_PASS: %d", NUM_PASS);
#endif	/* NUM_PASS */
#ifdef INTM_SCALE_FACTOR
	printf("\tINTM_SCALE_FACTOR: %d", INTM_SCALE_FACTOR);
#endif	/* INTM_SCALE_FACTOR */
#ifdef MEM_MON
	printf("\tMEM_MON");
#endif	/* MEM_MON */
#ifdef USE_PAPI
	printf("\tUSE_PAPI");
#endif	/* USE_PAPI */
	printf("\n");
}

void numa_cfg_mask_update(numa_cfg_t *numa_cfg, unsigned long mask_val) {
	numa_bitmask_clearall(numa_cfg->numa_mask);
	for (int node_idx = 0; node_idx < numa_cfg->nr_nodes; node_idx ++) {
		if ( GETBIT( mask_val, node_idx ) ) {
			numa_bitmask_setbit(numa_cfg->numa_mask, node_idx);
		}
	}
}


void datameta_init(datameta_t * const datameta, const char * cfg_path) {
	const char *tmp_r_path_suffix = NULL;
	const char *tmp_s_path_suffix = NULL;
	const char *tmp_workload_name = NULL;

	long long tmp_r_tuple_num = 0;
	long long tmp_s_tuple_num = 0;
	long long tmp_r_tuple_size = 0;
	long long tmp_s_tuple_size = 0;
	long long tmp_min_key = 0;
	long long tmp_max_key = 0;

	config_t cfg;
	config_init(&cfg);

	if (!config_read_file(&cfg, cfg_path)) {
		fprintf(stderr, "Config error: %s:%d - %s\n",
			config_error_file(&cfg),
			config_error_line(&cfg),
			config_error_text(&cfg));
		config_destroy(&cfg);
		exit(EXIT_FAILURE);
	}

	if (!config_lookup_string(&cfg, "r_path_suffix", &tmp_r_path_suffix) ||
		!config_lookup_string(&cfg, "s_path_suffix", &tmp_s_path_suffix) ||
		!config_lookup_string(&cfg, "workload_name", &tmp_workload_name)) {
		fprintf(stderr, "Missing required string field in config file\n");
		config_destroy(&cfg);
		exit(EXIT_FAILURE);
	}

	// Optional: add return value checks for these as well
	config_lookup_float(&cfg, "theta", &datameta->theta);
	config_lookup_float(&cfg, "selectivity", &datameta->selectivity);
	config_lookup_float(&cfg, "density", &datameta->density);
	config_lookup_float(&cfg, "sparsity", &datameta->sparsity);

	config_lookup_int64(&cfg, "r_tuple_num", &tmp_r_tuple_num);
	config_lookup_int64(&cfg, "s_tuple_num", &tmp_s_tuple_num);
	config_lookup_int64(&cfg, "r_tuple_size", &tmp_r_tuple_size);
	config_lookup_int64(&cfg, "s_tuple_size", &tmp_s_tuple_size);
	config_lookup_int64(&cfg, "min_key", &tmp_min_key);
	config_lookup_int64(&cfg, "max_key", &tmp_max_key);

	datameta->r_tuple_num = tmp_r_tuple_num;
	datameta->s_tuple_num = tmp_s_tuple_num;
	datameta->r_tuple_size = tmp_r_tuple_size;
	datameta->s_tuple_size = tmp_s_tuple_size;
	datameta->min_key = tmp_min_key;
	datameta->max_key = tmp_max_key;

	// Allocate + copy strings safely (include '\0')
	datameta->r_path_suffix = malloc(strlen(tmp_r_path_suffix) + 1);
	datameta->s_path_suffix = malloc(strlen(tmp_s_path_suffix) + 1);
	datameta->workload_name = malloc(strlen(tmp_workload_name) + 1);

	if (!datameta->r_path_suffix || !datameta->s_path_suffix || !datameta->workload_name) {
		fprintf(stderr, "Memory allocation failed for string fields\n");
		config_destroy(&cfg);
		exit(EXIT_FAILURE);
	}

	strcpy(datameta->r_path_suffix, tmp_r_path_suffix);
	strcpy(datameta->s_path_suffix, tmp_s_path_suffix);
	strcpy(datameta->workload_name, tmp_workload_name);

	config_destroy(&cfg);
}

void datameta_free(datameta_t *datameta) {
	if (datameta == NULL) return;

	free(datameta->r_path_suffix);
	datameta->r_path_suffix = NULL;

	free(datameta->s_path_suffix);
	datameta->s_path_suffix = NULL;

	free(datameta->workload_name);
	datameta->workload_name = NULL;
}

#ifdef __linux__
void openmp_bind_core(int _tid, numa_cfg_t *numa_cfg) {
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(get_cpu_id_wrt_numa(_tid, numa_cfg), &cpuset);	
	if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
		perror("pthread_setaffinity_np");
		// Optionally handle the error or exit.
	}
}
#endif /* __linux__ */

double openmp_memcpy(void *dest_mem, void*src_mem, size_t size) {
	struct timespec time_start, time_end;
	clock_gettime(CLOCK_REALTIME, &time_start);
	#pragma omp parallel num_threads(THREAD_NUM)
	{
		int _tid = omp_get_thread_num();
#ifdef __linux__
			cpu_set_t cpuset;
			CPU_ZERO(&cpuset);
			/**
			 * Assumes each OpenMP thread is bound to a distinct physical core.
			 * All numa_cfg_aux, numa_cfg_dest, and numa_cfg_src share the same local core set.
			 */
			CPU_SET(get_cpu_id_wrt_numa(_tid, &numa_cfg_src), &cpuset);	
			if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
				perror("pthread_setaffinity_np");
				// Optionally handle the error or exit.
			}
#endif	/* __linux__ */

			// Partition work among threads
			size_t chunk_size = size / THREAD_NUM;
			size_t offset = _tid * chunk_size;

			// Last thread takes the remainder
			if (_tid == THREAD_NUM - 1) {
				chunk_size = size - offset;
			}

			void *dst_chunk = (unsigned char *)dest_mem + offset;
			void *src_chunk = (unsigned char *)src_mem + offset;
			memcpy(dst_chunk, src_chunk, chunk_size);

	}
	clock_gettime(CLOCK_REALTIME, &time_end);
	return diff_sec(time_start, time_end);
}


void my_free(void **mem, size_t size) {
	if (*mem != NULL) {
		munmap(*mem, size);
	}
	*mem = NULL;
}
