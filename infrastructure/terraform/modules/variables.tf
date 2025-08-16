# Core Variables
variable "prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "bw"
}

variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "uksouth"
}

variable "location_short" {
  description = "Short code for Azure region"
  type        = string
  default     = "uks"
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "bluewave-rg"
}

# Storage Variables
variable "storage_account_name" {
  description = "Name of the storage account (must be globally unique)"
  type        = string
  default     = "bluewaveplatformsa"
}

variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
}

# Networking Variables
variable "vnet_address_space" {
  description = "Address space for the virtual network"
  type        = string
  default     = "10.0.0.0/16"
}

variable "allowed_ip_ranges" {
  description = "List of allowed IP ranges for Key Vault access"
  type        = list(string)
  default     = []
}

# Security Variables
variable "enable_customer_managed_keys" {
  description = "Enable customer-managed encryption keys"
  type        = bool
  default     = false
}

# Monitoring Variables
variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
}

variable "alert_email" {
  description = "Email address for alerts"
  type        = string
  default     = "devops@bluewave.com"
}

variable "monthly_budget" {
  description = "Monthly budget amount in GBP"
  type        = number
  default     = 5000
}

# Tags
variable "default_tags" {
  description = "Default tags to apply to all resources"
  type        = map(string)
  default = {
    ManagedBy   = "Terraform"
    CostCenter  = "Engineering"
    DataClass   = "Confidential"
    Compliance  = "GDPR"
  }
}