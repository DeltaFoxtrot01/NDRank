variable "tag" {
  description = "Tag for the objects"
  type = string
}

variable "zone" {
  description = "Zone of the VPC's subnets"
  type = string
}

variable "region" {
  description = "Region of the VPC"
  type = string
}

variable "clusters" {
  description = "Set with all clusters to be created"
  type = list
}