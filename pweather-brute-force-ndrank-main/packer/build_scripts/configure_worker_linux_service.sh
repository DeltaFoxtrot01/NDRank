#!/bin/bash
cp /home/pweather/pweather-brute-force-ndrank/packer/startup_scripts/worker_script.sh /home/pweather/execute_server.sh
cp /home/pweather/pweather-brute-force-ndrank/packer/startup_scripts/worker.service /home/pweather/worker.service

chmod +x /home/pweather/execute_server.sh
#configure systemctl service
sudo cp /home/pweather/worker.service /etc/systemd/system/
sudo systemctl enable worker

cp /home/pweather/pweather-brute-force-ndrank/packer/properties_files/brute_worker.yaml /home/pweather/brute_force_properties.yaml.template
cp /home/pweather/pweather-brute-force-ndrank/packer/properties_files/ndrank_worker.yaml /home/pweather/ndrank_properties.yaml.template

echo "templates created"