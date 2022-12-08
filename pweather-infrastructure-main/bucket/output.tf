output "bucket_url" {
  value = google_storage_bucket.dataset_bucket_storage.url
  description = "URL of the bucket"
}

output "bucket_name" {
  value = google_storage_bucket.dataset_bucket_storage.name
  description = "bucket's name"
}