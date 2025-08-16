# BlueWave Insurance Platform - Terraform Variables

# Core Configuration
prefix              = "bw"
environment         = "dev"
location            = "uksouth"
location_short      = "uks"
resource_group_name = "bluewave-rg"

# Storage Configuration
storage_account_name     = "bluewaveplatformsa"
storage_replication_type = "LRS"

# Networking Configuration
vnet_address_space = "10.0.0.0/16"
allowed_ip_ranges  = []

# Security Configuration
enable_customer_managed_keys = false

# Monitoring Configuration
log_retention_days = 30
alert_email        = "devops@bluewave.com"
monthly_budget     = 5000

# Tags
default_tags = {
  ManagedBy  = "Terraform"
  CostCenter = "Engineering"
  DataClass  = "Confidential"
  Compliance = "GDPR"
}