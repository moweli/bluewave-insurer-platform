terraform {
  required_version = ">= 1.3.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

# Data Sources
data "azurerm_client_config" "current" {}

# Local variables
locals {
  tags = merge(
    var.default_tags,
    {
      Environment = var.environment
      Project     = "BlueWave-Insurance"
      ManagedBy   = "Terraform"
    }
  )
}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  
  tags = local.tags
}

# Storage Account for Data Lake
resource "azurerm_storage_account" "sa" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"

  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  enable_https_traffic_only       = true

  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 30
    }
    
    container_delete_retention_policy {
      days = 7
    }
  }

  lifecycle_rule {
    enabled = true
    name    = "archive-old-data"
    
    blob {
      tier_to_cool_after_days_since_modification_greater_than    = 30
      tier_to_archive_after_days_since_modification_greater_than = 90
      delete_after_days_since_modification_greater_than          = 2555
    }
  }
  
  tags = local.tags
}

# Medallion Architecture Containers
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# Additional Containers
resource "azurerm_storage_container" "features" {
  name                  = "features"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "audit" {
  name                  = "audit"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "archive" {
  name                  = "archive"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# Event Hub Namespace
resource "azurerm_eventhub_namespace" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-eventhubs"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 2
  
  auto_inflate_enabled     = true
  maximum_throughput_units = 10
  
  tags = local.tags
}

# Event Hub for Real-time Claims
resource "azurerm_eventhub" "claims_realtime" {
  name                = "claims-realtime"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 32
  message_retention   = 7
}

# Event Hub for Audit Events
resource "azurerm_eventhub" "audit_events" {
  name                = "audit-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 4
  message_retention   = 3
}

# Consumer Groups for Claims Event Hub
resource "azurerm_eventhub_consumer_group" "fraud_detection" {
  name                = "fraud-detection"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.claims_realtime.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_eventhub_consumer_group" "analytics" {
  name                = "analytics"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.claims_realtime.name
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_eventhub_consumer_group" "archival" {
  name                = "archival"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.claims_realtime.name
  resource_group_name = azurerm_resource_group.rg.name
}
