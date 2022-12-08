/* Builds the AMIs for every component in the system
 * The following AMIs are built:
 *    - pweather-brute-force-node
 *    - pweather-master-node
 */

variable "git_token" {
    type = string
    sensitive = true
}

variable "project_id" {
    type = string
}

variable "region" {
    type = string
}

#------------------------REQUIRED SOURCES------------------------

source "googlecompute" "worker-node" {
    project_id = "${var.project_id}"
    source_image_project_id = [
        "ubuntu-os-pro-cloud"
    ]
    source_image = "ubuntu-pro-2004-focal-v20220204"
    zone = "${var.region}"

    communicator = "ssh"
    ssh_username = "pweather"

    image_name = "pweather-worker-node"
}

source "googlecompute" "master-node" {
    project_id = "${var.project_id}"
    source_image_project_id = [
        "ubuntu-os-pro-cloud"
    ]
    source_image = "ubuntu-pro-2004-focal-v20220204"
    zone = "${var.region}"

    communicator = "ssh"
    ssh_username = "pweather"

    image_name = "pweather-master-node"
}

source "googlecompute" "kafka-node" {
    project_id = "${var.project_id}"
    source_image_project_id = [
        "ubuntu-os-pro-cloud"
    ]
    source_image = "ubuntu-pro-2004-focal-v20220204"
    zone = "${var.region}"

    communicator = "ssh"
    ssh_username = "pweather"

    image_name = "pweather-kafka"
}

#------------------------BUILD INSTRUCTIONS------------------------

build {
    name = "worker-node"

    sources = ["source.googlecompute.worker-node"]

    provisioner "shell" {
        environment_vars = [
            "GIT_TOKEN=${var.git_token}"
        ]
        scripts = [
            "build_scripts/update_ubuntu.sh",
            "build_scripts/install_miniconda.sh",
            "build_scripts/create_repo_env.sh",
            "build_scripts/dataset_path_config.sh",
            "build_scripts/configure_worker_linux_service.sh"
        ]  
    }
}


build {
    name = "master-node"

    sources = ["source.googlecompute.master-node"]

    provisioner "shell" {
        environment_vars = [
            "GIT_TOKEN=${var.git_token}"
        ]
        scripts = [
            "build_scripts/update_ubuntu.sh",
            "build_scripts/install_miniconda.sh",
            "build_scripts/create_repo_env.sh",
            "build_scripts/master_script.sh",
            "build_scripts/auto_mount_properties_disk.sh"
        ]  
    }
}

build {
    name = "kafka-node"

    sources = ["source.googlecompute.kafka-node"]

    provisioner "shell" {
        environment_vars = [
            "GIT_TOKEN=${var.git_token}"
        ]
        scripts = [
            "build_scripts/update_ubuntu.sh",
            "build_scripts/install_miniconda.sh",
            "build_scripts/create_repo_env.sh",
            "build_scripts/kafka_install.sh"
        ]
    }
}
