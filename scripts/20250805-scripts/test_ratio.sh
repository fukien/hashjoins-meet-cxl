# mxc
## weighted interleaving ratio: 10:7


date
start_time=$(date +%s)

CUR_DIR=$(pwd)


DIR_PATH=${CUR_DIR}/../../logs/20250805-logs
mkdir -p $DIR_PATH

HC_PHJ_RDX_BC_LOG=${DIR_PATH}/hc_phj_rdx_bc.log
rm $HC_PHJ_RDX_BC_LOG
HC_NPHJ_SC_BUILD_LOG=${DIR_PATH}/hc_nphj_sc_build.log
rm $HC_NPHJ_SC_BUILD_LOG
HC_NPHJ_SC_PROBE_LOG=${DIR_PATH}/hc_nphj_sc_probe.log
rm $HC_NPHJ_SC_PROBE_LOG


RUN_NUM=3
# NUMACTL="/home/huang/workspace/numactl/numactl"
# CORE_RANGE="32-63,96-127"
THREAD_NUM=$(numactl --hardware | awk '/node 0 cpus:/ {print (NF-3)/2}')
cd ../..
bash clean.sh
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
	-DIN_DEBUG=true # \ 
	# -DCFG_PATH=config/mc/mxc0.cfg
make -j $(( $(nproc) / 16 ))
cd ..

for param in A B C; do
	for x_ratio in 0.05 0.10 0.15 0.20 0.25 0.29 0.30 0.35 0.40 0.45 \
		0.50 0.51 0.59 0.60 0.65 0.70 0.71 0.72 0.75 0.80 \
		0.85 0.90 0.95; do
		./bin/hc_join --algo=hc_phj_rdx_bc --workload=uniform --subtype=pkfk --param=$param --bpdr=$x_ratio >> $HC_PHJ_RDX_BC_LOG
		./bin/hc_join --algo=hc_nphj_sc --workload=uniform --subtype=pkfk --param=$param --bpdr=$x_ratio >> $HC_NPHJ_SC_BUILD_LOG
		./bin/hc_join --algo=hc_nphj_sc --workload=uniform --subtype=pkfk --param=$param --pjdr=$x_ratio >> $HC_NPHJ_SC_PROBE_LOG
	done
done


bash clean.sh
cd ${CUR_DIR}


end_time=$(date +%s)
duration=$((end_time - start_time))
echo "Duration: $duration seconds"
date