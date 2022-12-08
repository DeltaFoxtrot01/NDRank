
variable "zone" {
  description = "Availability zone where the resources are going to be deployed"
  type = string
}

variable "region" {
  description = "Region where the resources are deployed"
  type = string
}

variable "tag" {
  description = "general project tag"
}

variable "github_token" {
  description = "Github token used to pull from repos"
  type = string
}