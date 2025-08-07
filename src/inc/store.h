#pragma once

#define CACHELINE_SIZE 64
#define XPLINE_SIZE 256



#ifdef __cplusplus
extern "C" {
#endif



#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef int_fast64_t my_cnt_t;
typedef double my_rat_t;

#define AVX512_SIZE (512UL / 8)

#ifndef MY_CNT_T_SIZE
#define MY_CNT_T_SIZE sizeof(my_cnt_t)
#endif /* MY_CNT_T_SIZE */

#ifndef MY_RAT_T_SIZE
#define MY_RAT_T_SIZE sizeof(my_rat_t)
#endif /* MY_RAT_T_SIZE */


typedef struct {
	my_cnt_t key;
	my_cnt_t row_id;
} __attribute__( (packed, aligned( MY_CNT_T_SIZE + MY_CNT_T_SIZE ) ) ) keyrid_t;


typedef struct tuple_t {
	my_cnt_t key;
	my_cnt_t row_id;

#ifdef TUPLE_T_SIZE
	char values[TUPLE_T_SIZE-MY_CNT_T_SIZE-MY_CNT_T_SIZE];
} __attribute__( (packed, aligned( TUPLE_T_SIZE ) ) ) tuple_t;
#else /* TUPLE_T_SIZE */
} __attribute__( (packed, aligned( MY_CNT_T_SIZE + MY_CNT_T_SIZE ) ) ) tuple_t;
#endif /* TUPLE_T_SIZE */


typedef struct {
	my_cnt_t r_row_id;
	my_cnt_t s_row_id;
} row_id_pair_t;

typedef struct {
	tuple_t *tuples;
	my_cnt_t tuple_num;
} relation_t;


#ifndef KEY_RID_T_SIZE
#define KEY_RID_T_SIZE sizeof(keyrid_t)
#endif /* KEY_RID_T_SIZE */


#ifndef TUPLE_T_SIZE
#define TUPLE_T_SIZE sizeof(tuple_t)
#endif /* TUPLE_T_SIZE */


#ifndef ROW_ID_PAIR_T_SIZE
#define ROW_ID_PAIR_T_SIZE sizeof(row_id_pair_t)
#endif /* ROW_ID_PAIR_T_SIZE */


#ifdef __cplusplus
}
#endif