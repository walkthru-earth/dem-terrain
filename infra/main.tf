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

resource "verda_startup_script" "dem" {
  name   = "dem-processing-setup"
  script = file("${path.module}/startup.sh")
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

  environment = {
    S3_BUCKET             = var.s3_bucket
    S3_PREFIX             = var.s3_prefix
    AWS_ACCESS_KEY_ID     = var.aws_access_key_id
    AWS_SECRET_ACCESS_KEY = var.aws_secret_access_key
    AWS_REGION            = var.aws_region
    SCRATCH_DIR           = "/data/scratch"
  }
}
