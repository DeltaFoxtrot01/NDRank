locals {
  cluster_name = lower(var.cluster_name) #used so it can have a formating acceptable by gcp standards

  kafka_host = "${var.subnet_ip_prefix}200"
  low_res_distributor_host = "${var.subnet_ip_prefix}201"
  full_res_distributor_host = "${var.subnet_ip_prefix}202"
  master_host = "${var.subnet_ip_prefix}203"
  parameter_host = "${var.subnet_ip_prefix}204"

  num_nodes = var.mount_disks_to_nodes ? var.nr_nodes : 0
  
  low_res_disk_device_name = "lowres"
  full_res_disk_device_name = "fullres"
  parameter_disk_device_name = "params"

  vm_type_distributor = "c2d-highmem-2"
  vm_type_master = "c2d-highcpu-2"
  vm_type_kafka  = "c2d-highmem-2"
}

#--------------------------USED DISKS--------------------------
resource "google_compute_disk" "low_resolution_dataset_disk" {
  count = var.nr_nodes
  name = "${local.cluster_name}-${var.tag}-low-res-disk-dataset-${count.index}"
  size = var.low_res_disk_size
  type = var.disk_type
  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_compute_disk" "full_resolution_dataset_disk" {
  count = var.nr_nodes
  name = "${local.cluster_name}-${var.tag}-full-res-disk-dataset-${count.index}"
  size = var.full_res_disk_size
  type = var.disk_type
  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_compute_disk" "parameter_disk"{
  count = var.parameter_disk_size == 0 ? 0 : var.nr_nodes + 1
  name = "${local.cluster_name}-${var.tag}-parameter-disk-${count.index}"
  size = var.parameter_disk_size
  type = var.disk_type
  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }

  lifecycle {
    prevent_destroy = false
  }
}

resource "google_compute_attached_disk" "mount_low_res_disk" {
  count = var.nr_nodes
  disk  = google_compute_disk.low_resolution_dataset_disk[count.index].id

  instance = var.mount_disks_to_nodes ? google_compute_instance.worker_node[count.index].id : google_compute_instance.low_res_distributor_instance.id
  device_name = var.mount_disks_to_nodes ? local.low_res_disk_device_name : null
}

resource "google_compute_attached_disk" "mount_full_res_disk" {
  count = var.nr_nodes
  disk  = google_compute_disk.full_resolution_dataset_disk[count.index].id

  instance = var.mount_disks_to_nodes ? google_compute_instance.worker_node[count.index].id : google_compute_instance.full_res_distributor_instance.id
  device_name = var.mount_disks_to_nodes ? local.full_res_disk_device_name : null
}

resource "google_compute_attached_disk" "mount_parameters_disk" {
  count = var.parameter_disk_size == 0 ? 0 : var.nr_nodes
  disk  = google_compute_disk.parameter_disk[count.index].id

  instance = var.mount_disks_to_nodes ? google_compute_instance.worker_node[count.index].id : google_compute_instance.parameter_distributor_instance.0.id
  device_name = var.mount_disks_to_nodes ? local.parameter_disk_device_name : null
}

resource "google_compute_attached_disk" "mount_parameters_disk_to_master" {
  count = var.parameter_disk_size == 0 ? 0 : 1
  disk  = google_compute_disk.parameter_disk[var.nr_nodes].id

  instance = var.mount_disks_to_nodes ? google_compute_instance.master_node.0.id : google_compute_instance.parameter_distributor_instance.0.id
  device_name = var.mount_disks_to_nodes ? local.parameter_disk_device_name : null
}

#--------------------------USED INSTANCES--------------------------

resource "google_compute_instance" "low_res_distributor_instance" {
  name = "${local.cluster_name}-${var.tag}-dataset-low-res-distributor"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = local.vm_type_distributor

  boot_disk {
      initialize_params {
        image = var.distributor_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = local.low_res_distributor_host
    access_config {
      
    }
  }

  tags = ["${local.cluster_name}-${var.tag}-distributor"]

  lifecycle {
    ignore_changes = [attached_disk]
  }
  
  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

resource "google_compute_instance" "full_res_distributor_instance" {
  name = "${local.cluster_name}-${var.tag}-dataset-full-res-distributor"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = local.vm_type_distributor

  boot_disk {
      initialize_params {
        image = var.distributor_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = local.full_res_distributor_host
    access_config {
      
    }
  }

  tags = ["${local.cluster_name}-${var.tag}-distributor"]

  lifecycle {
    ignore_changes = [attached_disk]
  }

  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

resource "google_compute_instance" "parameter_distributor_instance" {
  count = var.parameter_disk_size == 0 ? 0 : 1
  name = "${local.cluster_name}-${var.tag}-parameter-distributor"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = local.vm_type_distributor

  boot_disk {
      initialize_params {
        image = var.distributor_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = local.parameter_host
    access_config {
      
    }
  }

  tags = ["${local.cluster_name}-${var.tag}-distributor"]

  lifecycle {
    ignore_changes = [attached_disk]
  }

  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

#--------------WORKER NODE CONFIGURATION--------------
data "template_file" "worker_init_script" {
  count = local.num_nodes
  template = file("${path.module}/settings.sh.tftpl")
  vars = {
    kafka_host = local.kafka_host
    node_id = format("node-%d",count.index)
    ndrank = var.ndrank_mode ? 1 : 0
    use_parameters = var.parameter_disk_size == 0 ? 0 : 1

    service = var.service
    repository = var.repository
    low_service = var.low_service
    low_repository = var.low_repository
  }
}

resource "google_compute_instance" "worker_node" {
  count = local.num_nodes

  name = "${local.cluster_name}-${var.tag}-worker-node-${count.index}-${var.ndrank_mode ? "ndrank-mode": "brute-force-mode"}"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = var.vm_model

  boot_disk {
      initialize_params {
        image = var.worker_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = join("",["${var.subnet_ip_prefix}", "${count.index+2}"])
  }

  tags = ["${local.cluster_name}-${var.tag}-vm"]

  lifecycle {
    ignore_changes = [attached_disk]
  }

  metadata = {
    startup-script = <<-EOF
    sleep 30
    echo "STARTUP SCRIPT STARTED EXECUTION"
    echo '${data.template_file.worker_init_script[count.index].rendered}' > /home/pweather/settings.sh 
    echo "STARTUP SCRIPT FINISHED SUCCESSFULLY"
    EOF
  }

  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

#--------------MASTER NODE CONFIGURATION--------------
resource "google_compute_instance" "master_node" {
  count =  var.mount_disks_to_nodes ? 1 : 0

  name = "${local.cluster_name}-${var.tag}-master-node"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = local.vm_type_master

  boot_disk {
      initialize_params {
        image = var.master_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = local.master_host
    #this field is here to allow the master node to have
    #an IP address
    access_config {
      
    }
  }

  metadata_startup_script = "echo \"${
        templatefile("${path.module}/master_node_properties.yaml.tftpl", {
            ips = google_compute_instance.worker_node.*.network_interface.0.network_ip
        })}\" > /home/pweather/properties.yaml"

  tags = ["${local.cluster_name}-${var.tag}-master-vm","allow-all-access-pweather"]

  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

#--------------KAFKA NODE CONFIGURATION--------------
resource "google_compute_instance" "kafka_instance" {
  count = var.mount_disks_to_nodes ? 1 : 0
  name = "${local.cluster_name}-${var.tag}-kafka-node"
  deletion_protection = false
  allow_stopping_for_update = true
  machine_type = local.vm_type_kafka

  boot_disk {
      initialize_params {
        image = var.kafka_image
        size = 20
      }
  }

  network_interface {
    network = var.vpc.name
    subnetwork = var.cluster_subnet.name
    network_ip = local.kafka_host

  }

  metadata_startup_script = "echo \"${
      templatefile("${path.module}/node_ids.yaml.tftpl", {
          node_ids = data.template_file.worker_init_script.*.vars.node_id
        })}\" > /home/pweather/ids.yaml"

  tags = ["${local.cluster_name}-${var.tag}-kafka"]

  labels = {
    tag = var.tag
    cluster_name = local.cluster_name
  }
}

#--------------------------FIREWALL CONFIG--------------------------

resource "google_compute_firewall" "local_firewall" {
  name = "${local.cluster_name}-${var.tag}-local-firewall"
  network = var.vpc.name

  allow {
    protocol = "tcp"
    ports = ["1-65535"]
  }

  source_ranges = ["10.0.2.0/24"]
  target_tags = [
      "${local.cluster_name}-${var.tag}-vm", 
      "${local.cluster_name}-${var.tag}-master-vm",
      "${local.cluster_name}-${var.tag}-kafka",
      "${local.cluster_name}-${var.tag}-distributor"
    ]
}

resource "google_compute_firewall" "external_connections_firewall" {
  name = "${local.cluster_name}-${var.tag}-external-firewall"
  network = var.vpc.name

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

  allow {
    protocol = "tcp"
    ports = ["1-65535"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = [
      "${local.cluster_name}-${var.tag}-vm", 
      "${local.cluster_name}-${var.tag}-master-vm",
      "${local.cluster_name}-${var.tag}-kafka",
      "${local.cluster_name}-${var.tag}-distributor"
    ]
}

resource "google_compute_firewall" "distributor_external_connections_firewall" {
  name = "${local.cluster_name}-${var.tag}-distributor"
  direction = "EGRESS"
  network = var.vpc.name

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

  allow {
    protocol = "tcp"
    ports = ["1-65535"]
  }

  destination_ranges = ["0.0.0.0/0"]
  target_tags = [
      "${local.cluster_name}-${var.tag}-distributor"
    ]
}

resource "google_compute_firewall" "allow_all_dev" {
  name = "${local.cluster_name}-${var.tag}-allow-all-for-dev"
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
  target_tags = ["${local.cluster_name}-${var.tag}-dev-all"]
}