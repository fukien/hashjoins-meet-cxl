#include "inc/utils.h"

void knuth_shuffle(void* const dram_data, const size_t record_num) {
	tuple_t tuple;
	size_t jdx;
	
	for (size_t idx = 0; idx < record_num; idx ++) {
		jdx = rand() % record_num;
		memcpy(&tuple, dram_data + jdx * TUPLE_T_SIZE, TUPLE_T_SIZE);
		memcpy(dram_data + jdx * TUPLE_T_SIZE, dram_data + idx * TUPLE_T_SIZE, TUPLE_T_SIZE);
		memcpy(dram_data + idx * TUPLE_T_SIZE, &tuple, TUPLE_T_SIZE);
	}
}

void gen_fk_from_pk(const void* pk_data, const size_t pk_record_num, const size_t fk_record_num, const char* subdirectory, const char* flag) {
	void* dram_data = malloc(fk_record_num * TUPLE_T_SIZE);
	size_t memcpy_size = pow(2, 12);

	for (size_t idx = 0; idx < (double) fk_record_num / pk_record_num; idx ++) {
		for (size_t jdx = 0; jdx < pk_record_num * TUPLE_T_SIZE / memcpy_size; jdx ++) {
			memcpy(dram_data + idx * pk_record_num * TUPLE_T_SIZE + jdx * memcpy_size, pk_data + jdx * memcpy_size, memcpy_size);
		}
	}

	knuth_shuffle(dram_data, fk_record_num);

	char path[CHAR_BUFFER_LEN];
	snprintf(path, CHAR_BUFFER_LEN, "%s/%s/%s_%s.bin", DATA_PATH_PREFIX, subdirectory, "fk", flag);
	void* ondisk_data = map_disk_file(path, fk_record_num * TUPLE_T_SIZE);
	memcpy(ondisk_data, dram_data, fk_record_num * TUPLE_T_SIZE);

	free(dram_data);
	pmem_unmap(ondisk_data, fk_record_num * TUPLE_T_SIZE);
}

void gen_pkfk_uniform(const size_t pk_record_num, const size_t fk_record_num, const char* subdirectory, const char* pk_flag, const char* fk_flag) {
	void* dram_data = malloc(pk_record_num * TUPLE_T_SIZE);
	tuple_t tuple;
	
	for (size_t idx = 0; idx < pk_record_num; idx ++) {
		tuple.key = idx + 1;
		tuple.row_id = idx + 1;
		memcpy(dram_data + idx * TUPLE_T_SIZE, &tuple, TUPLE_T_SIZE);
	}

	knuth_shuffle(dram_data, pk_record_num);


	char path[CHAR_BUFFER_LEN];
	snprintf(path, CHAR_BUFFER_LEN, "%s/%s", DATA_PATH_PREFIX, subdirectory);
	newdir(path);
	snprintf(path, CHAR_BUFFER_LEN, "%s/%s/%s_%s.bin", DATA_PATH_PREFIX, subdirectory, "pk", pk_flag);
	void* ondisk_data = map_disk_file(path, pk_record_num * TUPLE_T_SIZE);
	memcpy(ondisk_data, dram_data, pk_record_num * TUPLE_T_SIZE);
	
	gen_fk_from_pk(dram_data, pk_record_num, fk_record_num, subdirectory, fk_flag);

	free(dram_data);
	munmap(ondisk_data, pk_record_num * TUPLE_T_SIZE);
}


void generate_workload(int seed) {
	size_t workload_A_R_num = 16 * pow(2, 20);
	size_t workload_B_R_num = 64 * pow(2, 20);
	size_t workload_C_R_num = 256 * pow(2, 20);
	size_t workload_ABC_S_num = 256 * pow(2, 20);

	srand(seed);
	gen_pkfk_uniform(workload_A_R_num, workload_ABC_S_num, "uniform", "AR", "AS");
	printf("gen_pkfk_uniform(workload_A_R_num, workload_ABC_S_num, \"uniform\", \"AR\", \"AS\");\n");
	srand(seed);
	gen_pkfk_uniform(workload_B_R_num, workload_ABC_S_num, "uniform", "BR", "BS");
	printf("gen_pkfk_uniform(workload_B_R_num, workload_ABC_S_num, \"uniform\", \"BR\", \"BS\");\n");
	srand(seed);
	gen_pkfk_uniform(workload_C_R_num, workload_ABC_S_num, "uniform", "CR", "CS");
	printf("gen_pkfk_uniform(workload_C_R_num, workload_ABC_S_num, \"uniform\", \"CR\", \"CS\");\n");
}


int main(int argc __attribute__((unused)), char** argv __attribute__((unused))) {
	setvbuf(stdout, NULL, _IOLBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	newdir(DATA_PATH_PREFIX);

	/* Start measuring time */
	struct timespec begin, end; 
	clock_gettime(CLOCK_REALTIME, &begin);

	/* generate the seed */
	// srand((unsigned) time(NULL));
	// int seed = rand();
	int seed = 12345;

#ifdef RUN_PAYLOAD
	generate_payload(seed);
#else /* RUN_PAYLOAD */
	generate_workload(seed);
#endif /* RUN_PAYLOAD */

	/* Stop measuring time and calculate the elapsed time */
	clock_gettime(CLOCK_REALTIME, &end);
	double elapsed = diff_sec(begin, end);
	printf("Time elapsed: %.9f seconds.\n", elapsed);

	return 0;
}