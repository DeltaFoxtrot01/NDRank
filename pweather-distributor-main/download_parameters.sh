#!/usr/bin/env bash

# script arguments: ./script.sh <MOUNT|UMOUNT> <NUMBER OF DISKS> <BUCKET PATHS INCLUDING NAME OF THE BUCKET TO DOWNLOAD>

SMALL_A_ASCII_VALUE=97
DISK_PATH_TEMPLATE="/dev/sd"
MOUNT_FOLDER_TEMPLATE="/disk"

chr() {
  [ "$1" -lt 256 ] || return 1
  printf "\\$(printf '%03o' "$1")"
}

function get_file_path() {
    echo $HOME$MOUNT_FOLDER_TEMPLATE$1
}

function mount_disk() {
    folder=$(get_file_path $1)
    disk=$DISK_PATH_TEMPLATE$(chr $(($SMALL_A_ASCII_VALUE + $1)))
    echo $folder

    mkdir $folder
    echo "Mounting disk ${disk}"

    sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard $disk
    sudo mount -o discard,defaults $disk $folder
    sudo chown ddmdavid:ddmdavid $folder
    sudo chmod 777 $folder
}

function umount_disk() {
    folder=$(get_file_path $1)
    disk=$DISK_PATH_TEMPLATE$(chr $(($SMALL_A_ASCII_VALUE + $1)))
    echo $folder

    sudo umount $folder
    rm -r $folder
}

function download_files() {
    folder=$(get_file_path $1)"/"
    gsutil -m cp -r gs://$2 $folder
}

function copy_files() {
    cd $(get_file_path $1)
    rsync -r --exclude="lost+found" * $(get_file_path $2)
    cd $HOME
}

NUM_FIXED_ARGS=2
IFS=' '
read -ra ARGS_ARRAY <<< $@

if [[ "MOUNT" == $1 ]]; then
    for ((i = 1 ; i <= $2 ; i++)); do
        mount_disk $i
        if [[ $i == 1 ]]; then
            echo "Downloading files..."
            for ((j = $NUM_FIXED_ARGS; j < $#; j++)); do
                download_files $i ${ARGS_ARRAY[$j]}
            done
        else
            echo "Copying files to disk ${i}"
            copy_files 1 $i
        fi
    done
elif [[ "UMOUNT" == $1 ]]; then
    for ((i = 1 ; i <= $2 ; i++)); do
        umount_disk $i
    done
else
    echo "Mode not specified in first argument"
    exit 1
fi
