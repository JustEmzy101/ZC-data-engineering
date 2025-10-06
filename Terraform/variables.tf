   variable "credentials" {
   default = "C:\\Users\\mmzid\\data_engineering_zoomcamp\\Terraform\\keys\\my-creds.json"
   description = " My Credentials"
 }
  
  variable "porject" {
   default = "strong-kit-471914-j0"
   description = " The project name"
 }
 variable "location" {
   default = "me-central1"
   description = " The location where the project will be"
 }


variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "My Bucket Storage Class"
  default = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "strong-kit-471914-j0-terra-bucket"
}