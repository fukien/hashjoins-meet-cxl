/**
 * Assumes Linux version >= 6.9.0.
 * Currently, only USE_WEIGHTED_INTERLEAVING is supported.
 */


#include <getopt.h>
#include "inc/common.h"
#include "inc/memaccess.h"

#include "algo/nphj.h"
#include "algo/phj.h"

extern numa_cfg_t numa_cfg_dest, numa_cfg_src;

typedef struct algo_t {
	char name[128];
	void (*joinalgo)(const datameta_t *, timekeeper_t* const);
} algo_t;

algo_t algos [] = {
	{"nphj_sc", nphj_sc},
	{"phj_rdx_bc", phj_rdx_bc},
};



int main(int argc, char** argv) {
	cfg_print();

	srand(32768);
	setvbuf(stdout, NULL, _IOLBF, 0);

	unsigned long dest_numa_mask = 6;
	unsigned long src_numa_mask = 4;
	numa_cfg_init(&numa_cfg_dest);
	numa_cfg_init(&numa_cfg_src);

	algo_t* algo = &algos[0];
	char* workload = NULL;
	char* subtype = NULL;
	char* param = NULL;

	assert( argc >= 5);
	int command_arg_char;
	int i = 0;
	int found = 0;

	while (1) {
		int option_index = 0;
		static struct option long_options[] = {
			{"algo",		required_argument,	NULL,	0},
			{"workload",	required_argument,	NULL,	0},
			{"subtype",		required_argument, 	NULL,	0},
			{"param",		required_argument, 	NULL,	0},
			{"dnm",			required_argument,	NULL,	0},
			{"snm",			required_argument,	NULL,	0},
			{NULL,			0,					NULL,	0}
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
						i = 0;
						found = 0;
						while (algos[i].joinalgo) {
							if (strcmp(optarg, algos[i].name) == 0) {
								algo = &algos[i];
								found = 1;
								break;
							}
							i ++;
						}

						if (found == 0) {
							printf("[ERROR] Join algorithm named `%s' does not exist!\n", optarg);
							exit(EXIT_FAILURE);
						}

						break;

					case 1:
						workload = optarg;
						break;
					case 2:
						subtype = optarg;
						break;
					case 3:
						param = optarg;
						break;
					case 4:
						dest_numa_mask = atol(optarg);
						break;
					case 5:
						src_numa_mask = atol(optarg);
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

	char cfg_path[CHAR_BUFFER_LEN];
	snprintf(cfg_path, CHAR_BUFFER_LEN, 
		"%s/config/data/%s/%s_%s.cfg", 
		PROJECT_PATH, workload, subtype, param
	);
	assert(file_exists(cfg_path));
	datameta_t datameta;
	datameta_init(&datameta, cfg_path);

	numa_cfg_mask_update(&numa_cfg_dest, dest_numa_mask);
	numa_cfg_mask_update(&numa_cfg_src, src_numa_mask);

	timekeeper_t global_time_keeper = {0.0, 0.0, 0.0, 0.0};

	for (int cur_t = 0; cur_t < RUN_NUM; cur_t ++) {
		timekeeper_t tmp_time_keeper = {0.0, 0.0, 0.0, 0.0};
		(*(algo->joinalgo)) (&datameta, &tmp_time_keeper);
		if (cur_t != 0) {
			global_time_keeper.total += tmp_time_keeper.total;
			global_time_keeper.memcpy += tmp_time_keeper.memcpy;
			global_time_keeper.buildpart += tmp_time_keeper.buildpart;
			global_time_keeper.probejoin += tmp_time_keeper.probejoin;
		}
		cyan();
		printf("algo: %s\t",  algo->name);
		printf("workload: %s\t", datameta.workload_name);
		printf("memcpy_time: %.9f\t", tmp_time_keeper.memcpy);
		printf("buildpart_time: %.9f\t", tmp_time_keeper.buildpart);
		printf("probejoin_time: %.9f\t", tmp_time_keeper.probejoin);
		printf("total_time: %.9f\n", tmp_time_keeper.total);
	}

	numa_cfg_free(&numa_cfg_dest);
	numa_cfg_free(&numa_cfg_src);

#if RUN_NUM != 1
	green();
	printf("\n");
	printf("num_trials: %d\t", RUN_NUM-1);
	printf("dest: %ld\t", dest_numa_mask);
	printf("src: %ld\t", src_numa_mask);
	printf("algo: %s\t",  algo->name);
	printf("workload: %s\t", datameta.workload_name);
	printf("memcpy_time: %.9f\t", global_time_keeper.memcpy/(RUN_NUM-1));
	printf("buildpart_time: %.9f\t", global_time_keeper.buildpart/(RUN_NUM-1));
	printf("probejoin_time: %.9f\t", global_time_keeper.probejoin/(RUN_NUM-1));
	printf("total_time: %.9f\n\n", global_time_keeper.total/(RUN_NUM-1));
#endif /* RUN_NUM != 1 */

	datameta_free(&datameta);

	reset();
	printf("\n\n\n\n");

	return 0;
}









