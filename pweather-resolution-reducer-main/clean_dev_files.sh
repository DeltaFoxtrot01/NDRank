#!/bin/bash

for FILE in ./dev_dest ./temp_download_folder ./dev_intermediate_1 ./dev_intermediate_2
do
    cd $FILE
    rm *
    cd ..
done

for FILE in ./dev_source ./dev_source_1 ./dev_source_2 ./dev_source_3 ./dev_source_4 ./dev_source_5 ./dev_source_6
do
    cd $FILE
    rm *.idx
    cd ..
done

for FILE in ./log_files
do
    cd $FILE
    rm *.log
    cd ..
done