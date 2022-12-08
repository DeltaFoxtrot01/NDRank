#!/bin/bash
#make gcp run the scripts at launch of the instance, because, apparently, there is a portuguese culture inside
# google's SRE teams and things only start more than 5 minutes latter than they were scheduled to
sleep 30
sudo google_metadata_script_runner startup
export PATH="/home/pweather/miniconda3/bin:$PATH"
export PYTHONPATH=/home/pweather/pweather-brute-force-ndrank/worker_node
conda init bash
source activate pweather-brute-force-ndrank
cp /home/pweather/pweather-brute-force-ndrank/packer/properties_files/kafka_properties.yaml /home/pweather/properties.yaml

hostname -I | sed 's/ //g' | xargs -I % sudo sed -i 's/IP_HOST/%/g' /home/pweather/properties.yaml
cat /home/pweather/ids.yaml >> /home/pweather/properties.yaml

python3 /home/pweather/pweather-brute-force-ndrank/kafka/__main__.py -p /home/pweather/properties.yaml