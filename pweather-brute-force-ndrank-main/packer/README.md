# Packer

The system is deployed in gcp compute instances. To configure these compute instances, packer is used.

The packer code builds images for the following components:
- master node
- worker node
- kafka

The kafka image not only configures the kafka_merger and the a kafka cluster with a single broker.

To build the packer images, run the following command:
```
packer build -var-file variables.pkrvars.hcl .
```

## Variables file
To configure packer instances, a variable files is used. The file has the following structure:
```
git_token="PUT TOKEN HERE"
project_id="PUT PROJECT ID HERE"
region="PUT REGION HERE"
```

The ```git_token``` is a valid github token that can be used to pull repositories.
The ```project_id``` is the Id of the google cloud project.
The ```region``` is the gcp region where to create the AMIs.

To create a specific variables file create a copy of the ```variables.pkvars.hcl.template``` and remove the "template" part of the file name.