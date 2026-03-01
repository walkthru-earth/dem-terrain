output "instance_ip" {
  value       = verda_instance.dem.ip
  description = "Public IP of the DEM processing instance"
}

output "ssh_command" {
  value       = verda_instance.dem.ip != null ? "ssh root@${verda_instance.dem.ip}" : "IP not yet assigned"
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
  value       = var.s3_bucket != "" ? "s3://${var.s3_bucket}/${var.s3_prefix}/" : "/data/scratch/output/dem-terrain/"
  description = "Location of output Parquet files"
}
