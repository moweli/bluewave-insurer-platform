# Resource Group Outputs
output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.rg.name
}

output "resource_group_id" {
  description = "ID of the resource group"
  value       = azurerm_resource_group.rg.id
}

# Storage Account Outputs
output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.sa.name
}

output "storage_account_id" {
  description = "ID of the storage account"
  value       = azurerm_storage_account.sa.id
}

output "storage_account_primary_endpoint" {
  description = "Primary endpoint for the storage account"
  value       = azurerm_storage_account.sa.primary_dfs_endpoint
  sensitive   = true
}

# Event Hub Outputs
output "eventhub_namespace_name" {
  description = "Name of the Event Hub namespace"
  value       = azurerm_eventhub_namespace.main.name
}

output "eventhub_namespace_id" {
  description = "ID of the Event Hub namespace"
  value       = azurerm_eventhub_namespace.main.id
}

output "eventhub_claims_name" {
  description = "Name of the claims Event Hub"
  value       = azurerm_eventhub.claims_realtime.name
}

# Key Vault Outputs
output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.main.name
}

output "key_vault_id" {
  description = "ID of the Key Vault"
  value       = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.main.vault_uri
}

# Databricks Outputs
output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.name
}

output "databricks_workspace_id" {
  description = "ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.id
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.workspace_url
}

# Monitoring Outputs
output "log_analytics_workspace_name" {
  description = "Name of the Log Analytics workspace"
  value       = azurerm_log_analytics_workspace.main.name
}

output "log_analytics_workspace_id" {
  description = "ID of the Log Analytics workspace"
  value       = azurerm_log_analytics_workspace.main.id
}

output "application_insights_name" {
  description = "Name of Application Insights"
  value       = azurerm_application_insights.main.name
}

output "application_insights_instrumentation_key" {
  description = "Instrumentation key for Application Insights"
  value       = azurerm_application_insights.main.instrumentation_key
  sensitive   = true
}

output "application_insights_connection_string" {
  description = "Connection string for Application Insights"
  value       = azurerm_application_insights.main.connection_string
  sensitive   = true
}

# Networking Outputs
output "vnet_name" {
  description = "Name of the virtual network"
  value       = azurerm_virtual_network.main.name
}

output "vnet_id" {
  description = "ID of the virtual network"
  value       = azurerm_virtual_network.main.id
}

# Identity Outputs
output "managed_identity_id" {
  description = "ID of the managed identity"
  value       = azurerm_user_assigned_identity.main.id
}

output "managed_identity_client_id" {
  description = "Client ID of the managed identity"
  value       = azurerm_user_assigned_identity.main.client_id
}

output "managed_identity_principal_id" {
  description = "Principal ID of the managed identity"
  value       = azurerm_user_assigned_identity.main.principal_id
}