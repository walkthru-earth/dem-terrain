terraform {
  required_providers {
    verda = {
      source  = "verda-cloud/verda"
      version = "~> 1.0"
    }
  }
}

provider "verda" {
  client_id     = var.verda_client_id
  client_secret = var.verda_client_secret
}

# --- SSH Key ---

resource "verda_ssh_key" "dem" {
  name       = "dem-processing-key"
  public_key = var.ssh_public_key
}

# --- Startup Script ---

locals {
  startup_script = replace(
    replace(
      replace(
        replace(
          replace(
            file("${path.module}/startup.sh"),
            "__S3_BUCKET__", var.s3_bucket
          ),
          "__S3_PREFIX__", var.s3_prefix
        ),
        "__AWS_ACCESS_KEY_ID__", var.aws_access_key_id
      ),
      "__AWS_SECRET_ACCESS_KEY__", var.aws_secret_access_key
    ),
    "__AWS_REGION__", var.aws_region
  )
}

resource "verda_startup_script" "dem" {
  name   = "dem-processing-setup"
  script = local.startup_script
}

# --- 2TB NVMe Volume ---

resource "verda_volume" "data" {
  name     = "dem-processing-data"
  size     = 2000 # GB
  type     = "NVMe"
  location = var.location
}

# --- CPU Node (360 vCPUs, 1440 GB RAM) ---

resource "verda_instance" "dem" {
  instance_type     = var.instance_type
  image             = "ubuntu-24.04"
  hostname          = "dem-processing"
  description       = "One-time global DEM to Parquet conversion"
  location          = var.location
  ssh_key_ids       = [verda_ssh_key.dem.id]
  startup_script_id = verda_startup_script.dem.id
  existing_volumes  = [verda_volume.data.id]
}
