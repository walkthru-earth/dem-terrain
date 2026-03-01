# --- Verda credentials ---

variable "verda_client_id" {
  type        = string
  description = "Verda API client ID"
}

variable "verda_client_secret" {
  type        = string
  sensitive   = true
  description = "Verda API client secret"
}

# --- SSH ---

variable "ssh_public_key" {
  type        = string
  description = "SSH public key for instance access"
}

# --- Instance config ---

variable "location" {
  type        = string
  default     = "FIN-01"
  description = "Verda datacenter location"
}

variable "instance_type" {
  type        = string
  default     = "CPU.360V.1440G"
  description = "Verda instance type (CPU.360V.1440G = 360 vCPUs, 1440 GB RAM, AMD EPYC)"
}

# --- S3 output ---

variable "s3_bucket" {
  type        = string
  description = "S3 bucket for Parquet output"
}

variable "s3_prefix" {
  type        = string
  default     = ""
  description = "S3 key prefix inside the bucket"
}

variable "aws_access_key_id" {
  type        = string
  description = "AWS access key for S3 writes"
}

variable "aws_secret_access_key" {
  type        = string
  sensitive   = true
  description = "AWS secret key for S3 writes"
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for S3 bucket"
}
