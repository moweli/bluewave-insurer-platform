# Backend configuration for demo environment
# Using local backend for demo to avoid storage costs

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

# For production, use Azure Storage backend:
# terraform {
#   backend "azurerm" {
#     resource_group_name  = "terraform-state-rg"
#     storage_account_name = "tfstatestore"
#     container_name       = "tfstate"
#     key                  = "demo.terraform.tfstate"
#   }
# }