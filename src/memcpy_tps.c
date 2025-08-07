/**
 * Assumes Linux version >= 6.9.0.
 * Currently, only USE_WEIGHTED_INTERLEAVING is supported.
 */


#include <getopt.h>
#include "inc/common.h"
#include "inc/memaccess.h"

#define WORKING_MEM_SIZE_UL  (8UL * 1024UL * 1024UL * 1024UL)  // 4 GB as unsigned long

extern numa_cfg_t numa_cfg_dest, numa_cfg_src;

int main(int argc, char* argv[]) {
	cfg_print();
	srand(32768);
	setvbuf(stdout, NULL, _IOLBF, 0);

	unsigned long dest_numa_mask = 0, src_numa_mask = 0;
	numa_cfg_init(&numa_cfg_dest);
	numa_cfg_init(&numa_cfg_src);

	int command_arg_char;
	int found = 0;
	while (1) {
		int option_index = 0;
		static struct option long_options[] = {
			{"dnm",		required_argument,	NULL,	0},		// dest numa mask
			{"snm",		required_argument,	NULL,	0},		// src numa mask
			{NULL,		0,					NULL,	0}
		};
		command_arg_char = getopt_long(argc, argv, "-:", long_options, &option_index);
		if (command_arg_char == -1) {
			break;
		}
		switch (command_arg_char) {
			case 0:
				/**
				printf("long option %s", long_options[option_index].name);
				if (optarg) {
					printf(" with arg %s, index %d \n", optarg, option_index);
				}
				*/
				switch (option_index) {
					case 0:
						found = 0;
						dest_numa_mask = atol(optarg);
						found = 1;

						if (found == 0) {
							printf("[ERROR] Pattern named `%s' does not exist!\n", optarg);
							exit(EXIT_FAILURE);
						}

						break;
					case 1:
						found = 0;
						src_numa_mask = atol(optarg);
						found = 1;

						if (found == 0) {
							printf("[ERROR] Operation named `%s' does not exist!\n", optarg);
							exit(EXIT_FAILURE);
						}

						break;
					default:
						perror("command line arguments parsing error\n");
						printf("unknow arg %s\tof index %d\nerrono: %d", optarg, option_index, errno);
						exit(EXIT_FAILURE);
						break;
				}
				break;
			case '?':
				printf("unknown option %c\n", optopt);
				break;
		}
	}

	numa_cfg_mask_update(&numa_cfg_dest, dest_numa_mask);
	numa_cfg_mask_update(&numa_cfg_src, src_numa_mask);


#ifdef MEM_MON
	char command[CHAR_BUFFER_LEN] = {'\0'};
	snprintf(
		command, sizeof(command),
		"bash %s/mem_mon.sh %d %lx %lx < /dev/null &",
		PROJECT_PATH, getpid(), dest_numa_mask, src_numa_mask
	);
	system(command);
#endif /* MEM_MON */

	void *src_mem = alloc_weighted(WORKING_MEM_SIZE_UL, &numa_cfg_src);
	memset(src_mem, 0, WORKING_MEM_SIZE_UL);
	void *dest_mem = alloc_weighted(WORKING_MEM_SIZE_UL, &numa_cfg_dest);
#ifdef PREFAULT
	memset(dest_mem, 0, WORKING_MEM_SIZE_UL);
#endif	/* PREFAULT */

	double total_runtime = 0.0;
	for (int idx = 0; idx < RUN_NUM; idx ++) {

		struct timespec time_start, time_end;
		clock_gettime(CLOCK_REALTIME, &time_start);

		#pragma omp parallel num_threads(THREAD_NUM)
		{

			int tid = omp_get_thread_num();
#ifdef __linux__
			cpu_set_t cpuset;
			CPU_ZERO(&cpuset);
			/**
			 * Assumes each OpenMP thread is pinned to a distinct physical core.
			 * Both numa_cfg_dest and numa_cfg_src use the same set of local cores.
			 */
			CPU_SET(get_cpu_id_wrt_numa(tid, &numa_cfg_dest), &cpuset);	
			if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
				perror("pthread_setaffinity_np");
				// Optionally handle the error or exit.
			}
#endif	/* __linux__ */

			// Partition work among threads
			size_t chunk_size = WORKING_MEM_SIZE_UL / THREAD_NUM;
			size_t offset = tid * chunk_size;

			// Last thread takes the remainder
			if (tid == THREAD_NUM - 1) {
				chunk_size = WORKING_MEM_SIZE_UL - offset;
			}

			void *dst_chunk = (unsigned char *)dest_mem + offset;
			void *src_chunk = (unsigned char *)src_mem + offset;
#ifdef NTMEMCPY
			size_t cacheline_num = chunk_size/CACHELINE_SIZE;
			for (size_t jdx = 0; jdx < cacheline_num; jdx ++) {
				write_nt_64(dst_chunk, src_chunk);
				dst_chunk += CACHELINE_SIZE;
				src_chunk += CACHELINE_SIZE;
			}
#else	/* NTMEMCPY */
			memcpy(dst_chunk, src_chunk, chunk_size);
#endif	/* NTMEMCPY */
		}
		clock_gettime(CLOCK_REALTIME, &time_end);
		double current_runtime = diff_sec(time_start, time_end);
		printf("%dth run: %.9f\n", idx, current_runtime);

		if (idx > 0) {
			total_runtime += current_runtime;
		}
	}

#if RUN_NUM > 1
	printf("thread-%d-agg_run:\t%.9fs\t%.9fGB/s\n", 
		THREAD_NUM, total_runtime/(RUN_NUM-1),
		(double) WORKING_MEM_SIZE_UL/(total_runtime/(RUN_NUM-1))/1024/1024/1024
	);
	printf("%d_runs_duration: %.9f\n", RUN_NUM, total_runtime);
#endif /* RUN_NUM > 1 */	



	dealloc_weighted(dest_mem, WORKING_MEM_SIZE_UL);
	dealloc_weighted(src_mem, WORKING_MEM_SIZE_UL);

	numa_cfg_free(&numa_cfg_dest);
	numa_cfg_free(&numa_cfg_src);

	return 0;
}









