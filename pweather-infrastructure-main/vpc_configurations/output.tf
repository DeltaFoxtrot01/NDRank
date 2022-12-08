output "main_vpc" {
  value = google_compute_network.main_vpc
}

output "main_subnet" {
  value = google_compute_subnetwork.main_subnet
}

output "cluster_subnets" {
  value = google_compute_subnetwork.cluster_subnets
}

output "subnet_ip_prefix"{
  value = [for num in range(length(var.clusters)): "10.0.${num+1}."]
}