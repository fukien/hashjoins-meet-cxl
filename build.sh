date


RUN_NUM=3
THREAD_NUM=$(numactl --hardware | awk '/node 0 cpus:/ {print (NF-3)/2}')
# THREAD_NUM=$((2 * THREAD_NUM))
THREAD_NUM=32


mkdir build
cd build

cmake .. -DRUN_NUM=$RUN_NUM \
	-DTHREAD_NUM=$THREAD_NUM -DUSE_HYPERTHREADING=true  \
	-DNUMA_MASK_VAL=6 -DUSE_WEIGHTED_INTERLEAVING=true \
	-DUSE_HUGE=false -DPREFAULT=true -DUSE_NUMA=false \
	-DNTMEMCPY=false \
	-DPREFETCH_DISTANCE=8 -DOVERFLOW_BUF_SIZE=1024 \
	-DUSE_SWWCB=true -DSWWCB_SIZE=64 -DNUM_RADIX_BIT=12 -DNUM_PASS=1 \
	-DINTM_SCALE_FACTOR=2 \
	-DMEM_MON=false \
	-DUSE_PAPI=false -DIN_STATS=false -DIN_VERIFY=false \
	-DIN_DEBUG=false # \ 
	# -DCFG_PATH=config/mc/mxc0.cfg


make -j $(( $(nproc) / 16 ))
cd ..


date


