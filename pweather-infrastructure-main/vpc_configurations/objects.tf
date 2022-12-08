resource "google_compute_network" "main_vpc" {
  name = "${var.tag}-vpc"
  auto_create_subnetworks = false
}
resource "google_compute_subnetwork" "main_subnet" {
  name = "${var.tag}-subnet"
  region = var.region
  network = google_compute_network.main_vpc.name
  ip_cidr_range = "10.0.0.0/24"
}

resource "google_compute_subnetwork" "cluster_subnets" {
  count = length(var.clusters)
  name = "${lower(var.clusters[count.index].cluster_name)}-${var.tag}-subnet"
  region = var.region
  network = google_compute_network.main_vpc.name
  ip_cidr_range = "10.0.${count.index+1}.0/24"
}