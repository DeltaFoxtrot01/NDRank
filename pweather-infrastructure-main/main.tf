#all user defined variables
variable "region" {
  description = "Region where the resources are going to be created"
}

variable "zone" {
  description = "Availability zone where the resources are going to be deployed"
}

variable "project_id" {
  description = "Id of the project"
}

variable "credentials_file" {
  description = "Name of the credentials file"
}

variable "tag" {
  description = "General tag for easy identification of this specific project"
}

variable "bucket_location" {
  description = "Bucket location."
  type = string
}

variable "github_token" {
  description = "github token"
  type = string
}

# This is the structure for a cluster description:
#   {
#     cluster_name = "<CLUSTER NAME (THE DEFAULT THAT IS BEING USED IS TO ASSIGN EUROPEAN CAPITALS)>
#     number_nodes = <NUMBER OF WORKER NODES>
#     vm_model = <VM MODEL FOR THE WORKER NODE>
#     low_res_disk_size = <SIZE THE LOW RESOLUTION DISK SHOULD HAVE>
#     full_res_disk_size = <SIZE THE FULL RESOLUTION DISK SHOULD HAVE>
#     parameter_disk_size = <SIZE THE PARAMETER DISK SHOULD HAVE 0 TO NOT CREATE ANY DISK OR THE PARAMETER DISTRIBUTOR>
#     disk_type = <TYPE OF THE DISK FOR BOTH THE FULL RESOLUTION AND LOW RESOLUTION DATASET>
#     ndrank_mode = <IF THE CLUSTER SHOULD BE EXECUTED AS NDRANK (true) OR BRUTE FORCE (false)>
#     mount = <IF DISKS SHOULD BE MOUNTED IN THE WORKER NODES (true) OR IN THE DISTRIBUTOR NODES (false)>
#     service = <NAME OF THE SERVICE USED IN THE FULL RESOLUTION DATASET>
#     repository = <NAME OF THE REPOSITORY USED IN THE FULL RESOLUTION DATASET>
#     low_service = <NAME OF THE SERVICE USED IN THE LOW RESOLUTION DATASET>
#     low_repository = <NAME OF THE REPOSITORY USED IN THE LOW RESOLUTION DATASET>
#   }
# The number of possible clusters is limited by the number of subnets that can be created and 
# the number of nodes is limited by the number of available IPs in the subnet

locals {
  clusters = [
    {
      cluster_name = "Berlin"
      number_nodes = 3
      vm_model = "n2-standard-2"
      low_res_disk_size = 20
      full_res_disk_size = 40
      parameter_disk_size = 200
      disk_type = "pd-ssd"
      ndrank_mode = false,
      mount = false,
      service = "simple-service",
      #service = "parameter-candidate-list-service"
      #service = "candidate-list-service",
      repository = "month-year-repository"
      #low_service = "simple-top-n-service",
      low_service = "\"\""
      #low_repository = "month-year-repository"
      low_repository = "\"\""
    }
    ,
    {
      cluster_name = "Dublin"
      number_nodes = 2
      vm_model = "n2-standard-2"
      low_res_disk_size = 10
      full_res_disk_size = 10
      parameter_disk_size = 0
      disk_type = "pd-ssd"
      ndrank_mode = false,
      mount = false,
      service = "simple-service",
      #service = "parameter-candidate-list-service"
      repository = "month-year-repository"
      #low_service = "simple-top-n-service",
      low_service = "\"\""
      #low_repository = "month-year-repository"
      low_repository = "\"\""
    }
    #,
    #{
    #  cluster_name = "Madrid"
    #  number_nodes = 2
    #  vm_model = "n1-standard-1"
    #  low_res_disk_size = 20
    #  full_res_disk_size = 20
    #  disk_type = "pd-ssd"
    #  ndrank_mode = true,
    #  mount = true,
    #  service = "simple-service",
    #  repository = "month-year-repository",
    #  low_service = "candidate-list-service",
    #  low_repository = "month-year-repository"
    #}
    #,
    #{
    #  cluster_name = "Rome"
    #  number_nodes = 2
    #  vm_model = "n1-standard-1"
    #  low_res_disk_size = 20
    #  full_res_disk_size = 20
    #  disk_type = "pd-ssd"
    #  ndrank_mode = false,
    #  mount = false,
    #  service = "simple-service",
    #  repository = "hour-day-month-year-repository",
    #  low_service = "simple-top-n-service",
    #  low_repository = "hour-day-month-year-repository"
    #}
  ]
}


#provider configurations
provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

provider "google-beta" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}


#local modules definition
module "vpc_configurations" {
  #general configurations for the vpcs and subnets
  source = "./vpc_configurations"

  clusters = local.clusters
  zone = var.zone
  region = var.region
  tag = var.tag
}

module "bucket" {
  #configurations for the bucket that stores the datasets
  source = "./bucket"

  tag = var.tag
  location = var.bucket_location
}

module "build_tools" {
  #configuration of cloud build pipelines
  source = "./build_tools"
  zone = var.zone
  region = var.region
  tag = var.tag
  github_token = var.github_token
}

module "dev_environment" {
  #some objects that are usefull for development like some firewalls 
  # that are quite permissive and definitions of instances
  source = "./dev_environment"

  vpc= module.vpc_configurations.main_vpc
  subnet = module.vpc_configurations.main_subnet
  zone = var.zone
  tag = var.tag
}

module "cluster" {
  #configurations of the clusters
  count = length(local.clusters)
  depends_on = [
    module.vpc_configurations
  ]
  source = "./cluster"

  vpc = module.vpc_configurations.main_vpc

  cluster_name = local.clusters[count.index].cluster_name
  nr_nodes = local.clusters[count.index].number_nodes
  vm_model = local.clusters[count.index].vm_model
  
  low_res_disk_size = local.clusters[count.index].low_res_disk_size
  full_res_disk_size = local.clusters[count.index].full_res_disk_size
  parameter_disk_size = local.clusters[count.index].parameter_disk_size

  disk_type = local.clusters[count.index].disk_type
  ndrank_mode = local.clusters[count.index].ndrank_mode
  mount_disks_to_nodes = local.clusters[count.index].mount

  cluster_subnet = module.vpc_configurations.cluster_subnets[count.index]
  subnet_ip_prefix = module.vpc_configurations.subnet_ip_prefix[count.index]

  master_image      = "pweather-master-node"
  worker_image      = "pweather-worker-node"
  kafka_image       = "pweather-kafka"
  distributor_image = "distributor2" #NOTE: this is a privately created image, not a public one or created one by packer

  tag = var.tag

  service = local.clusters[count.index].service
  low_service = local.clusters[count.index].low_service
  repository = local.clusters[count.index].repository
  low_repository = local.clusters[count.index].low_repository
}
