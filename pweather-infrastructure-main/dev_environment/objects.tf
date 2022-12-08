
resource "google_compute_instance_from_machine_image" "dev_machine-cds-downloader" {
  count = 0
  provider = google-beta
  name = "dev-machine-cds-downloader-${var.tag}-${count.index}"

  source_machine_image = "downloader"
  zone = var.zone

  tags = ["dev-vm-${var.tag}"]
}

resource "google_compute_firewall" "ssh_access" {
  name = "allow-ssh-access-${var.tag}"
  network = var.vpc.name

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

  allow {
    protocol = "tcp"
    ports = ["8000"]
  }

  allow {
    protocol = "tcp"
    ports = ["9000-10000"]
  }

  source_ranges = [ "0.0.0.0/0" ]
  target_tags = ["dev-vm-${var.tag}"]
}


resource "google_compute_firewall" "allow_all" {
  name = "allow-all-access-${var.tag}"
  network = var.vpc.name

  allow {
    protocol = "tcp"
    ports = ["1-65535"]
  }

  allow {
    protocol = "udp"
    ports = ["1-65535"]
  }

  source_ranges = [ "0.0.0.0/0" ]
  target_tags = ["dev-all-${var.tag}"]
}