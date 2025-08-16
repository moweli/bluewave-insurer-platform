# Demo Environment Main Configuration
# Cost-optimized for <$50/month operation

terraform {
  required_version = ">= 1.3.0"
  
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults  = false
    }
  }
}

# Load all modules
module "networking" {
  source = "../../modules"
  
  # Pass all variables through
  resource_group_name     = var.resource_group_name
  location               = var.location
  prefix                 = var.prefix
  environment            = var.environment
  virtual_network_config = var.virtual_network_config
  databricks_config      = var.databricks_config
  storage_config         = var.storage_config
  eventhub_config        = var.eventhub_config
  keyvault_config        = var.keyvault_config
  log_analytics_config   = var.log_analytics_config
  service_bus_config     = var.service_bus_config
  tags                   = var.tags
}