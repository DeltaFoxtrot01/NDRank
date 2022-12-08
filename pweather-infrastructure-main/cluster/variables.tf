variable "vpc" {
  description = "used vpc"
}

variable "cluster_name" {
  description = "Name of the cluster"
  type = string
}

variable "nr_nodes" {
  description = "number of worker nodes"
  type = number
}

variable "vm_model" {
  description = "Type of VM for worker nodes"
  type = string
}

variable "low_res_disk_size" {
  description = "size that the low resolution dataset disks should have"
  type = number
}

variable "full_res_disk_size" {
  description = "size that the full resolution dataset disks should have"
  type = number
}

variable "parameter_disk_size" {
  description = "size that the parameter disks should have"
  type = number
}

variable "disk_type" {
  description = "Type of disk that should be used for the datasets"
  type = string
}

variable "ndrank_mode" {
  description = "True if the cluster should act as a ndrank cluster or false if it should act as a brute force cluster"
  type = bool
}

variable "mount_disks_to_nodes" {
  description = "If the disks with the dataset should be mounted to the nodes. Otherwise they will be mounted to the distributor instance"
  type = bool
}


variable "cluster_subnet" {
  description = "subnet to deploy the cluster's resources"
}

variable "subnet_ip_prefix" {
  description = "ip prefix used by the subnet (it is assumed what already has the first 24 bits and the last dot, like '10.0.1.')"
  type = string
}


variable "master_image" {
  description = "disk image name for master node"
  type = string
}

variable "worker_image" {
  description = "disk image name for the worker node"
  type = string
}

variable "kafka_image" {
  description = "disk image name for the kafka instance"
  type = string
}

variable "distributor_image" {
  description = "disk image name for the distributor instance"
  type = string
}

variable "tag" {
  description = "general project tag"
  type = string
}


variable "service" {
  description = "Service used to search in the full resolution service"
  type = string
}

variable "low_service" {
  description = "Service used to search in the low resolution service"
  type = string
}

variable "repository" {
  description = "Repository used to search in the full resolution service"
  type = string
}

variable "low_repository" {
  description = "Repository used to search in the low resolution service"
  type = string
}
