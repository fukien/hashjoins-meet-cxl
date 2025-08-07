# Hash Joins Meet CXL: A Fresh Look

This repository accompanies our CIDR'26 submission:  **“Hash Joins Meet CXL: A Fresh Look”**.


## Prerequisites
1. A genuine CXL type-3 memory device

## Dependencies
```
sudo apt-installl libnuma-dev
```

## Quick Start
```
./build.sh
./bin/gen
./bin/join --algo=phj_rdx_bc --workload=uniform --subtype=pkfk --param=A --dnm=2 --snm=4
./bin/join --algo=nphj_sc --workload=uniform --subtype=pkfk --param=A --dnm=4 --snm=4
```

## Reproducing Experiments 
```
cd scripts/20250806-scripts
bash test_hc_join.sh
bash test_ratio.sh
python plot_pkfk.py
```

## Further Support
If you have any enquiries, please contact huangwentao@u.nus.edu (Huang Wentao) for the further support.
