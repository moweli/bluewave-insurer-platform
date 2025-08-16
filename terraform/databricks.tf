# Databricks Workspace Configuration

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-databricks"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  managed_resource_group_name = "${var.prefix}-${var.environment}-databricks-managed-rg"

  # Network configuration for private endpoint support
  public_network_access_enabled         = false
  network_security_group_rules_required = "NoAzureDatabricksRules"

  custom_parameters {
    no_public_ip                                         = true
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
  
  blob_properties {
    versioning_enabled = true
  }

  tags = local.tags
}

resource "azurerm_storage_container" "unity_catalog_metastore" {
  name                  = "metastore"
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}