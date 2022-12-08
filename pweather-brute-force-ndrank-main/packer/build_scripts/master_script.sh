#!/bin/bash
#download code for data visualizer
echo ${GIT_TOKEN}
git clone -b main https://${GIT_TOKEN}@github.com/penedocapital/pweather-data-visualizer.git
cd /home/pweather/pweather-data-visualizer

export PATH="/home/pweather/miniconda3/bin:$PATH"
export PYTHONPATH=/home/pweather/pweather-data-visualizer
#/home/pweather/miniconda3/etc/profile.d/conda.sh

#create conda environment
conda env create -f conda_env.yaml
conda clean --all 