output "instance_ip" {
  value       = verda_instance.dem.ip_address
  description = "Public IP of the DEM processing instance"
}

output "ssh_command" {
  value       = "ssh root@${verda_instance.dem.ip_address}"
  description = "SSH command to connect to the instance"
}

output "instance_id" {
  value       = verda_instance.dem.id
  description = "Verda instance ID (for API management)"
}

output "volume_id" {
  value       = verda_volume.data.id
  description = "NVMe volume ID"
}

output "parquet_output_path" {
  value       = var.s3_bucket != "" ? "s3://${var.s3_bucket}/${var.s3_prefix}/dem-terrain/" : "/data/scratch/output/dem-terrain/"
  description = "Location of output Parquet files"
}
