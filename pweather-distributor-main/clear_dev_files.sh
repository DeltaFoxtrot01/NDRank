#!/bin/bash

for FILE in ./dev_dest_file_0 ./dev_dest_file_1 ./dev_dest_file_2 ./temp_download_folder
do
    cd $FILE
    rm -r *
    cd ..
done