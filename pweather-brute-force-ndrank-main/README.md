# pweather-brute-force

This repository has the code base for both the brute force architecture and the NDRank architecture.

## Repository structure
This repository has 4 main components:
- the extractor
- the master_node
- the worker_node
- the kafka
- the packer

The ```extractor``` is a tool mostly used for development to extract sections in time out of dataset files. Not intended to be used as a finished tool, therefore there may be some minor bugs.

The ```master_node``` has all the code for the master node.

The ```worker_node``` has all the code for the worker node.

The ```kafka``` has all the code for the kafka merger process.

The ```packer``` has all the configuration to build the disk images for the master, worker and kafka merger processes.

## Configure Conda environemnt
To create an environment
- conda env create -f conda_env.yaml 

To activate the conda environment run:
- conda activate pweather-brute-force-ndrank

## Packer images
To create the production environment, compute instances were used in GCP.
These instances were configured using packer. The packer images are built 
using cloud build. The cloud build is configured with the ```cloudbuild.yaml``` 
file.

## Run the tests:
Run the following command:
- py.test -s

The tests do not have a full coverage, because it would require to deploy a kafka queue in order to tests this (which it was a bit complicated). The existing tests test parts of the protocol, the existing services and other components of the system.

## Type check:
Run the ```type_check.sh``` script. This is the only repository that has type checking configured, because it was only decided to try at latter stage of development. It increases productivity significantly as the type checking process catches a lot of bugs and it improves the intelisense from Visual Studio. I did not feel that it added much more complexity to the project.