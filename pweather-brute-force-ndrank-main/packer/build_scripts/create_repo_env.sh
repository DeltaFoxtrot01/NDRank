#!/bin/bash
#download code for worker node
echo ${GIT_TOKEN}
git clone -b main https://${GIT_TOKEN}@github.com/penedocapital/pweather-brute-force-ndrank.git
cd /home/pweather/pweather-brute-force-ndrank

export PATH="/home/pweather/miniconda3/bin:$PATH"
export PYTHONPATH=/home/pweather/pweather-brute-force-ndrank/worker_node
#/home/pweather/miniconda3/etc/profile.d/conda.sh

#create conda environment
conda env create -f conda_env.yaml
conda init bash
cd /home/pweather/pweather-brute-force-ndrank
source activate pweather-brute-force-ndrank

#compile protobuf
chmod +x compile-protobuf.sh
./compile-protobuf.sh