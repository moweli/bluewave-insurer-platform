# Demo Environment Variables
# Inherits from module variables with demo-specific defaults

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "bluewave-demo-rg"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "UK South"
}

variable "prefix" {
  description = "Resource prefix"
  type        = string
  default     = "bwdemo"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "demo"
}

variable "virtual_network_config" {
  description = "Virtual network configuration"
  type = object({
    address_space     = list(string)
    subnets          = map(string)
    enable_ddos      = bool
  })
  default = {
    address_space     = ["10.0.0.0/16"]
    subnets          = {
      default      = "10.0.1.0/24"
      databricks   = "10.0.2.0/24"
    }
    enable_ddos      = false
  }
}

variable "databricks_config" {
  description = "Databricks configuration"
  type = object({
    sku                           = string
    enable_no_public_ip          = bool
    enable_unity_catalog        = bool
    cluster_auto_termination_min = number
  })
  default = {
    sku                           = "standard"  # Use standard for demo
    enable_no_public_ip          = false
    enable_unity_catalog        = false        # Disabled for cost
    cluster_auto_termination_min = 10          # Quick termination
  }
}

variable "storage_config" {
  description = "Storage configuration"
  type = object({
    account_tier             = string
    account_replication_type = string
    enable_hns              = bool
    containers              = list(string)
    enable_private_endpoint = bool
  })
  default = {
    account_tier             = "Standard"
    account_replication_type = "LRS"      # Local redundancy only
    enable_hns              = true
    containers              = ["raw", "bronze", "silver", "gold", "audit"]
    enable_private_endpoint = false       # No private endpoints for demo
  }
}

variable "eventhub_config" {
  description = "Event Hub configuration"
  type = object({
    sku                     = string
    capacity               = number
    enable_auto_inflate    = bool
    maximum_throughput_units = number
    eventhubs              = map(object({
      partition_count    = number
      message_retention  = number
    }))
  })
  default = {
    sku                     = "Basic"      # Basic tier for demo
    capacity               = 1             # Minimum capacity
    enable_auto_inflate    = false        # No auto-scaling
    maximum_throughput_units = 1
    eventhubs              = {
      claims-realtime = {
        partition_count    = 2           # Minimum partitions
        message_retention  = 1           # 1 day retention
      }
    }
  }
}

variable "keyvault_config" {
  description = "Key Vault configuration"
  type = object({
    sku_name                    = string
    enabled_for_disk_encryption = bool
    soft_delete_retention_days  = number
    purge_protection_enabled    = bool
  })
  default = {
    sku_name                    = "standard"
    enabled_for_disk_encryption = false
    soft_delete_retention_days  = 7       # Minimum retention
    purge_protection_enabled    = false   # Allow immediate deletion
  }
}

variable "log_analytics_config" {
  description = "Log Analytics configuration"
  type = object({
    sku                = string
    retention_in_days  = number
    daily_quota_gb     = number
    enable_app_insights = bool
  })
  default = {
    sku                = "PerGB2018"
    retention_in_days  = 30              # Minimum retention
    daily_quota_gb     = 1                # 1GB daily cap
    enable_app_insights = false          # Disabled for cost
  }
}

variable "service_bus_config" {
  description = "Service Bus configuration"
  type = object({
    sku      = string
    capacity = number
    enable   = bool
  })
  default = {
    sku      = "Basic"
    capacity = 0
    enable   = false                     # Disabled for demo
  }
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default = {
    project     = "BlueWave"
    environment = "demo"
    cost_center = "demo"
    auto_delete = "true"
    purpose     = "cost-optimized-demo"
  }
}