cmake_minimum_required(VERSION 3.16.3)
project(hashjoins-meet-cxl)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# C flags
set(CMAKE_C_FLAGS "-O3 -march=native -Wall -Wextra")
# set(CMAKE_C_FLAGS "-O3 -march=native -Wall -Wextra -g")
# set(CMAKE_C_FLAGS "-O3 -march=native -Wall -Wextra -g -fsanitize=address")
# set(CMAKE_C_FLAGS "-O0 -march=native -Wall -Wextra -ggdb3")
# set(CMAKE_C_FLAGS "-O0 -march=native -Wall -Wextra -ggdb3 -fsanitize=address")

# C++ flags
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS}")


# add_definitions(-DIN_DEBUG)
#[[
#ifdef IN_DEBUG
	printf("%s:%d\n", __FILE__, __LINE__);
#endif /* IN_DEBUG */
]]


add_definitions(-DPROJECT_PATH="${CMAKE_CURRENT_SOURCE_DIR}")

set(SRC_PATH "src")
set(BIN_PATH "bin")
set(ALGO_PATH "src/algo")
set(INC_PATH "src/inc")
# set(PAPI_PATH "src/papi")
# set(PAPI_INC_PATH "/home/huang/papi/src/install/include")
# set(PAPI_LIB_PATH "/home/huang/papi/src/install/lib")

set(NUMA_INC_PATH "/usr/local/include")
set(NUMA_LIB_PATH "/usr/local/lib")

add_definitions(-DCHAR_BUFFER_LEN=256)

cmake_host_system_information(RESULT _host_name QUERY HOSTNAME)
message("-- _host_name VARIABLE IS SET TO: " ${_host_name})
if(${_host_name} STREQUAL "quad")
	add_definitions(-DCORE_NUM=72)
	add_definitions(-DSOCKET_CORE_NUM=18)
	add_definitions(-DCFG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/config/mc/quad.cfg")
	# set(IPMWATCH_INC_PATH "/home/huang/intel-pmwatch-3.2.1/src/inc")
	set(MKLROOT_PATH "/opt/intel/oneapi/2024.2")
	set(VTUNE_INC_PATH "/opt/intel/oneapi/vtune/2024.2/include/")
	set(VTUNE_LIB_PATH "/opt/intel/oneapi/vtune/2024.2/lib64/")
	set(LINUX_LIB_PATH "/lib/modules/6.11.0/kernel/lib")
	# set(SUITESPARSE_INC_DIR "/home/huang/SuiteSparse/self_defined_installation/include/suitesparse")
	# set(SUITESPARSE_LIB_DIR "/home/huang/SuiteSparse/build/GraphBLAS")
elseif(${_host_name} STREQUAL "ocna1")
	add_definitions(-DCORE_NUM=40)
	add_definitions(-DSOCKET_CORE_NUM=20)
	add_definitions(-DCFG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/config/mc/ocna1.cfg")
	# set(IPMWATCH_INC_PATH "/work/huang/workspace/intel-pmwatch/src/inc")
	set(MKLROOT_PATH "/work/huang/intel/oneapi/mkl/2022.0.2")
	set(VTUNE_INC_PATH "/work/huang/intel/oneapi/vtune/2022.0.0/include/")
	set(VTUNE_LIB_PATH "/work/huang/intel/oneapi/vtune/2022.0.0/lib64")
	set(LINUX_LIB_PATH "/lib/modules/5.4.0-187-generic/kernel/lib")
elseif(${_host_name} STREQUAL "xfusion-2288HV7")
	add_definitions(-DCORE_NUM=64)
	add_definitions(-DSOCKET_CORE_NUM=32)
	# add_definitions(-DCFG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/config/mc/mxc0.cfg")
	add_definitions(-DCFG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/config/mc/mxc.cfg")
	# add_definitions(-DSOCKET_CORE_NUM=8)
	# add_definitions(-DCFG_PATH="${CMAKE_CURRENT_SOURCE_DIR}/config/mc/mxc-snc.cfg")
	set(IPMWATCH_INC_PATH "/home/huang/intel-pmwatch-3.2.1/src/inc")
	set(MKLROOT_PATH "/opt/intel/oneapi/2024.0")
	set(VTUNE_INC_PATH "/opt/intel/oneapi/vtune/2024.0/include/")
	set(VTUNE_LIB_PATH "/opt/intel/oneapi/vtune/2024.0/lib64/")
	set(LINUX_HEADERS_PATH "/home/huang/workspace/numactl/include")
	set(LINUX_LIB_PATH "/lib/modules/6.11.0/kernel/lib")
	# set(SUITESPARSE_INC_DIR "/home/huang/SuiteSparse/self_defined_installation/include/suitesparse")
	# set(SUITESPARSE_LIB_DIR "/home/huang/SuiteSparse/build/GraphBLAS")
else()
	message( FATAL_ERROR "-- _host_name: " ${_host_name} " NOT FOUND " )
endif()


foreach(HEADER_PATH IN LISTS LINUX_HEADERS_PATH)
	include_directories(${HEADER_PATH})
endforeach()

foreach(LIB_PATH IN LISTS LINUX_LIB_PATH)
	link_directories(${LIB_PATH})
	list(APPEND CMAKE_EXE_LINKER_FLAGS "-Wl,-rpath,${LIB_PATH}")
endforeach()


if(CFG_PATH)
	add_definitions(-DCFG_PATH=\"${CFG_PATH}\")
endif()


add_definitions(-DDATA_PATH_PREFIX="${CMAKE_CURRENT_SOURCE_DIR}/dataset")

if(USE_HUGE)
	add_definitions(-DUSE_HUGE)
endif()

if(PREFAULT)
	add_definitions(-DPREFAULT)
endif()

# Declare options with default values
option(USE_INTERLEAVING "Enable interleaving" OFF)
option(USE_WEIGHTED_INTERLEAVING "Enable weighted interleaving" OFF)
if(USE_INTERLEAVING)
	add_definitions(-DUSE_INTERLEAVING)
elseif(USE_WEIGHTED_INTERLEAVING)
	add_definitions(-DUSE_WEIGHTED_INTERLEAVING)
endif()

if(USE_NUMA)
	add_definitions(-DUSE_NUMA)
endif()

# NUMA_MASK_VAL starts from 1
if(NUMA_MASK_VAL)
	add_definitions(-DNUMA_MASK_VAL=${NUMA_MASK_VAL})
else()
	# use local numa node by default
	add_definitions(-DNUMA_MASK_VAL=1)
endif()

if(NTMEMCPY)
	add_definitions(-DNTMEMCPY)
endif()

if (MEM_MON)
	add_definitions(-DMEM_MON)
endif()

# We force NUMA_NODE_IDX start from 1, i.e., NUMA_NODE_IDX = numa_node_id + 1
if(NUMA_NODE_IDX)
	add_definitions(-DNUMA_NODE_IDX=${NUMA_NODE_IDX})
else()
	# add_definitions(-DNUMA_NODE_IDX=2)
	add_definitions(-DNUMA_NODE_IDX=2)
endif()

if(LOCAL_NUMA_NODE_IDX)
	add_definitions(-DLOCAL_NUMA_NODE_IDX=${LOCAL_NUMA_NODE_IDX})
else()
	add_definitions(-DLOCAL_NUMA_NODE_IDX=2)
	# add_definitions(-DLOCAL_NUMA_NODE_IDX=8)
endif()

add_definitions(-DLAST_NUMA_NODE_IDX=3)
add_definitions(-DCXL_NUMA_NODE_IDX=3)

if(USE_NVM)
	add_definitions(-DUSE_NVM)
	add_definitions(-DNVM_LOCAL_PREFIX="/pmemfs1/huang/hmem-io")
	add_definitions(-DNVM_REMOTE_PREFIX="/pmemfs0/huang/hmem-io")
else()
	add_definitions(-DUSE_CXL)
endif()

if(RUN_NUM)
	add_definitions(-DRUN_NUM=${RUN_NUM})
else()
	add_definitions(-DRUN_NUM=50)
endif()

if(THREAD_NUM)
	add_definitions(-DTHREAD_NUM=${THREAD_NUM})
else()
	add_definitions(-DTHREAD_NUM=32)
endif()

if(USE_HYPERTHREADING)
	add_definitions(-DUSE_HYPERTHREADING)
endif()

# if(USE_PAPI)
# 	add_definitions(-DUSE_PAPI)
# endif()

if(IN_STATS)
	add_definitions(-DIN_STATS)
endif()

if(IN_VERIFY)
	add_definitions(-DIN_VERIFY)
endif()

if(IN_DEBUG)
	add_definitions(-DIN_DEBUG)
endif()



# NPHJ
if(PREFETCH_DISTANCE)
	add_definitions(-DPREFETCH_DISTANCE=${PREFETCH_DISTANCE})
endif()

if(OVERFLOW_BUF_SIZE)
	add_definitions(-DOVERFLOW_BUF_SIZE=${OVERFLOW_BUF_SIZE})
else()
	add_definitions(-DOVERFLOW_BUF_SIZE=1024)
endif()


# PHJ
if(USE_SWWCB)
	add_definitions(-DUSE_SWWCB)
endif()
if(SWWCB_SIZE)
	add_definitions(-DSWWCB_SIZE=${SWWCB_SIZE})
endif()
if(NUM_RADIX_BIT)
	add_definitions(-DNUM_RADIX_BIT=${NUM_RADIX_BIT})
else()
	add_definitions(-DNUM_RADIX_BIT=9)
endif()
if(NUM_PASS)
	add_definitions(-DNUM_PASS=${NUM_PASS})
else()
	add_definitions(-DNUM_PASS=1)
endif()
if(INTM_SCALE_FACTOR)
	add_definitions(-DINTM_SCALE_FACTOR=${INTM_SCALE_FACTOR})
else()
	add_definitions(-DINTM_SCALE_FACTOR=2)
endif()



add_subdirectory(${INC_PATH})
link_libraries(Inc)

include_directories(${VTUNE_INC_PATH})
link_directories(${VTUNE_LIB_PATH})

include_directories(${NUMA_INC_PATH})
link_directories(${NUMA_LIB_PATH})

# include_directories(${PAPI_INC_PATH})
# add_subdirectory(${PAPI_PATH})
# link_directories(${PAPI_LIB_PATH})
# link_libraries(Papi -lpapi)

add_subdirectory(${ALGO_PATH})
link_libraries(Algo)


find_package(OpenMP REQUIRED)
add_executable(memcpy_tps ${SRC_PATH}/memcpy_tps.c)
set_target_properties(memcpy_tps PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/${BIN_PATH})
target_link_libraries(memcpy_tps PRIVATE OpenMP::OpenMP_C)

add_executable(gen ${SRC_PATH}/gen.c)
set_target_properties(gen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/${BIN_PATH})
target_link_libraries(gen PRIVATE OpenMP::OpenMP_C)

add_executable(join ${SRC_PATH}/join.c)
set_target_properties(join PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/${BIN_PATH})
target_link_libraries(join PRIVATE OpenMP::OpenMP_C)