# That is constanta
locals {
  data_lake_bucket = "dtc_data_lake"
}

# var will be fill when we runtime our terraform task
variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-east2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}

variable "BQ_TABLE" {
  description = "BigQuery table"
  type = string
  default = "ny_taxi"
}