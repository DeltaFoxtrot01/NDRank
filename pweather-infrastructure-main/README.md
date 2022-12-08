# pweather-infrastructure

This project stores all the code that manages the infrastructure required for NDRank.

This repository that 4 different disk images exist:
 - pweather-master-node
 - pweather-worker-node
 - pweather-kafka
 - distributor2 (this one was manually built, not built in packer)

To apply any changes to the code or to build it, run the following command:
```
terraform apply
```

## Variables file
To configure the terraform code with some general variables, a variables file was defined. It has the following structure:
```
region                   = "<region>"
zone                     = "<zone>"
project_id               = "<project id>"
credentials_file         = "<credentials file>"
tag                      = "<tag>"
bucket_location          = "<location>"
github_token             = "<github token>"
```

To create a custom variables file, create a copy of the file "terraform.tfvars.example" and remove the remove the ".example" part of the code. This file will be ignored by the ".gitignore" file.

The ```region``` is the google cloud region and the ```zone``` is the zone inside the region (a, b, c, etc.)

The ```project_id``` is the Id of the google cloud project.

The ```credentials_file``` is a key file from gcp.

The ```tag``` is a tag added accross most objects created by terraform so it is possible to distinguish where the objects originate from. This can be any name.

The ```bucket_location``` is the location specific for the bucket (see here -> https://cloud.google.com/storage/docs/locations).

The ```github_token``` is a github token that has access to the repositories from the organization, so it can be passed as a secret.