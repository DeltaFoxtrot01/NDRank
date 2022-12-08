#!/bin/bash
mypy master_node
code1=$?
mypy worker_node
code2=$?
mypy extractor
code3=$?

if [ $code1 == 0 ] && [ $code2 == 0 ] && [ $code3 == 0 ]
then
    exit 0
else
    exit 1
fi
