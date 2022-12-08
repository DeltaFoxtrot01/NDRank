#!/bin/bash
#make gcp run the scripts at launch of the instance, because, apparently, there is a portuguese culture inside
# google's SRE teams and things only start more than 5 minutes latter than they were scheduled to
sleep 10
sudo google_metadata_script_runner startup
#configure conda and python 3
export PATH="/home/pweather/miniconda3/bin:$PATH"
export PYTHONPATH=/home/pweather/pweather-brute-force-ndrank/worker_node

conda init bash
source activate pweather-brute-force-ndrank

source /home/pweather/settings.sh

LOW_RES_PATH="/home/pweather/low_dataset/"
FULL_RES_PATH="/home/pweather/dataset"

function list_folders_in_yaml_format () {
    #reads the folders from a specific directory
    # used to map all existing repositories, as it
    # is assumed that they are in a single folder
    text=$1

    if [ "${text: -1}" != "/" ]
    then
        text+="/"
    fi
    res=""

    for folder in $(ls -d "${text}"*/); do
        if [[ $folder != *lost+found* ]]
        then
            res+="\ \ -\ ${folder}'\n'"
        fi
    done
    echo $res
}

if [ $NDRANK != 0 ]
then
    #generate properties file
    echo "NDRANK mode active"
    sudo cp /home/pweather/ndrank_properties.yaml.template /home/pweather/properties.yaml
    hostname -I | sed 's/ //g' | xargs -I % sudo sed -i 's/IP_HOST/%/g' /home/pweather/properties.yaml
    echo "Configuring Kafka Host and Node id"
    echo $KAFKA_HOST | xargs -I % sudo sed -i 's/KAFKA_HOST/%/g' /home/pweather/properties.yaml
    echo $NODE_ID | xargs -I % sudo sed -i 's/NODE_ID/%/g' /home/pweather/properties.yaml

    echo "Configuring low resolution service"
    if [[ $LOW_SERVICE == "" ]]
    then
        echo "Configuring low resolustion service as empty"
        sudo sed -i 's/LOW_SERVICE//g' /home/pweather/properties.yaml
        sudo sed -i 's/LOW_REPOSITORY//g' /home/pweather/properties.yaml
    else
        echo "Configuring low resolution service"
        echo $LOW_SERVICE | xargs -I % sudo sed -i 's/LOW_SERVICE/%/g' /home/pweather/properties.yaml
        echo $LOW_REPOSITORY | xargs -I % sudo sed -i 's/LOW_REPOSITORY/%/g' /home/pweather/properties.yaml
    fi

    echo "Configuring full resolution service"
    echo $SERVICE | xargs -I % sudo sed -i 's/SERVICE/%/g' /home/pweather/properties.yaml
    echo $REPOSITORY | xargs -I % sudo sed -i 's/REPOSITORY/%/g' /home/pweather/properties.yaml

    #mount additional dataset
    echo "Mounting low resolution disk"
    sudo mount -o discard,defaults /dev/disk/by-id/google-lowres /home/pweather/low_dataset
    sudo chown -R pweather:pweather /home/pweather/low_dataset
    list_folders_in_yaml_format $LOW_RES_PATH | xargs -I % sudo sed -i 's|LOW_PATHS|%|g' /home/pweather/properties.yaml

else
    #generate properties file
    echo "Brute-force mode active"
    sudo cp /home/pweather/brute_force_properties.yaml.template /home/pweather/properties.yaml
    hostname -I | sed 's/ //g' | xargs -I % sudo sed -i 's/IP_HOST/%/g' /home/pweather/properties.yaml

    echo $SERVICE | xargs -I % sudo sed -i 's/SERVICE/%/g' /home/pweather/properties.yaml
    echo $REPOSITORY | xargs -I % sudo sed -i 's/REPOSITORY/%/g' /home/pweather/properties.yaml
fi

if [ $USE_PARAMETERS != 0 ]
then
    echo "Mounting parameter disk"
    sudo mount -o discard,defaults /dev/disk/by-id/google-params /home/pweather/parameters
    sudo chown -R pweather:pweather /home/pweather/parameters
fi

#mounts folder containing the dataset
echo "Mounting full resolution disk"
sudo mount -o discard,defaults /dev/disk/by-id/google-fullres $FULL_RES_PATH
sudo chown -R pweather:pweather $FULL_RES_PATH
list_folders_in_yaml_format $FULL_RES_PATH | xargs -I % sudo sed -i 's|FULL_PATHS|%|g' /home/pweather/properties.yaml


python3 /home/pweather/pweather-brute-force-ndrank/worker_node/__main__.py -p /home/pweather/properties.yaml