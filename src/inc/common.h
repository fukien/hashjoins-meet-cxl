#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <omp.h>

#include "utils.h"

#ifndef IDHASH
#define IDHASH(X, MASK, BITSKIP) (((X) & MASK) >> BITSKIP)
#endif /* IDHASH */


typedef struct {
	double memcpy;
	double buildpart;
	double probejoin;
	double total;
} timekeeper_t;


typedef struct {
	char *r_path_suffix;
	char *s_path_suffix;

	double theta;
	double selectivity;
	double density;
	double sparsity;

	size_t r_tuple_num;
	size_t s_tuple_num;
	size_t r_tuple_size;
	size_t s_tuple_size;

	my_cnt_t min_key;
	my_cnt_t max_key;

	char *workload_name;
} datameta_t;


void cfg_print();

void numa_cfg_mask_update(numa_cfg_t *numa_cfg, unsigned long mask_val);

void datameta_init(datameta_t* const datameta, const char* cfg_path);
void datameta_free(datameta_t *datameta);

typedef volatile char lock_t;

inline void unlock(lock_t *_l) __attribute__((always_inline));
inline void lock(lock_t *_l) __attribute__((always_inline));
inline int tas(lock_t *lock) __attribute__((always_inline));

/*
 * Non-recursive spinlock. Using `xchg` and `ldstub` as in PostgresSQL.
 */
/* Call blocks and retunrs only when it has the lock. */
inline void lock(lock_t *_l) {
	while(tas(_l)) {
#if defined(__i386__) || defined(__x86_64__)
		__asm__ __volatile__ ("pause\n");
#endif
	}
}

/** Unlocks the lock object. */
inline void unlock(lock_t *_l) { 
	*_l = 0;
}

inline int tas(lock_t *lock) {
	register char res = 1;
#if defined(__i386__) || defined(__x86_64__)
	__asm__ __volatile__ (
						  "lock xchgb %0, %1\n"
						  : "+q"(res), "+m"(*lock)
						  :
						  : "memory", "cc");
#elif defined(__sparc__)
	__asm__ __volatile__ (
						  "ldstub [%2], %0"
						  : "=r"(res), "+m"(*lock)
						  : "r"(lock)
						  : "memory");
#else
#error TAS not defined for this architecture.
#endif
	return res;
}


#ifdef __linux__
void openmp_bind_core(int _tid, numa_cfg_t *numa_cfg);
#endif /* __linux__ */


double openmp_memcpy(void *dest, void*src, size_t size);


void my_free(void **mem, size_t size);


#ifdef __cplusplus
}
#endif