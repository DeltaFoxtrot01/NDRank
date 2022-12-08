#simple storage bucket where datasets are stored

#parameters:
#  - grib2 -> 32 time instances

#the anomaly_changed are the files that require a specific processing for that specific kind of request
locals {
  requests = ["grib1", "grib2", "grib3", "grib4", "grib5", "grib6"]
  folders = ["original_results", "regular_gaussian", "anomaly","anomaly_reduced_resolution", "anomaly_changed"]
  resolution_folders = ["anomaly_reduced_resolution"]
  resolutions = ["lat_lon_4", "lat_lon_8"]

  parameter_folders = ["parameters","parameters_0"]
  parameters = ["average", "standard_deviation"]

  final_folders = flatten([for folder in local.folders : [
                    for request in local.requests :
                      join("",[folder,"/",request,"/"])
                  ]])
  resolution_subfolders = flatten([for resolution_folder in local.resolution_folders: [
                            for request in local.requests: [
                              for resolution in local.resolutions:
                                join("",[resolution_folder,"/",request,"/",resolution,"/"])
                          ]]])
  final_parameter_folders = flatten([for parameter_folder in local.parameter_folders :[
                              for request in local.requests: [
                                for parameter in local.parameters:
                                  join("",[parameter_folder,"/",request,"/",parameter,"/"])
                              ]
                            ]])
}


resource "google_storage_bucket" "dataset_bucket_storage" {
  name = "${var.tag}-penedocapital"
  location = var.location
  force_destroy = false
}

#folders for the bucket
#main folders
resource "google_storage_bucket_object" "dataset_folders" {
  count = length(local.final_folders) 
  
  name = local.final_folders[count.index]
  content = "Empty folder"
  bucket = google_storage_bucket.dataset_bucket_storage.name
}

resource "google_storage_bucket_object" "dataset_resolution_folders" {
  count = length(local.resolution_subfolders) 
  
  name = local.resolution_subfolders[count.index]
  content = "Empty folder"
  bucket = google_storage_bucket.dataset_bucket_storage.name

  depends_on = [
    google_storage_bucket_object.dataset_folders
  ]
}

resource "google_storage_bucket_object" "dataset_parameters" {
  count = length(local.final_parameter_folders)
  
  name = local.final_parameter_folders[count.index]
  content = "Empty folder"
  bucket = google_storage_bucket.dataset_bucket_storage.name

  depends_on = [
    google_storage_bucket_object.dataset_folders
  ]
}