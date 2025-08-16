# Databricks Workspace Configuration

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-databricks-001"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"  # Changed from premium to reduce costs

  managed_resource_group_name = "${var.resource_group_name}-databricks-managed"

  # Network configuration - Enable public access for development
  # TODO: Switch to private endpoints for production
  public_network_access_enabled         = true
  network_security_group_rules_required = "AllRules"

  custom_parameters {
    no_public_ip                                         = false
    virtual_network_id                                   = azurerm_virtual_network.main.id
    public_subnet_name                                   = azurerm_subnet.databricks_public.name
    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_private.id
  }

  tags = local.tags
}

# Unity Catalog Metastore (Note: This requires additional setup post-deployment)
resource "azurerm_storage_account" "unity_catalog" {
  name                     = "${var.prefix}${var.environment}unitycatalog"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  
  # Note: versioning_enabled cannot be true when is_hns_enabled is true

  tags = local.tags
}

resource "azurerm_storage_container" "unity_catalog_metastore" {
  name                  = "metastore"
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}

# Outputs for Databricks configuration
output "databricks_workspace_url" {
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
  description = "The URL to access Databricks workspace"
}

output "databricks_workspace_id" {
  value       = azurerm_databricks_workspace.main.workspace_id
  description = "The workspace ID for Databricks"
}

output "databricks_resource_id" {
  value       = azurerm_databricks_workspace.main.id
  description = "The Azure resource ID of the Databricks workspace"
}