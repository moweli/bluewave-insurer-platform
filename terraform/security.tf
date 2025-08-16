# Security Resources

# Random suffix for globally unique names
resource "random_string" "unique" {
  length  = 6
  special = false
  upper   = false
}

# Key Vault
resource "azurerm_key_vault" "main" {
  name                = "${var.prefix}-${var.environment}-kv-${random_string.unique.result}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "premium"

  enabled_for_deployment          = false
  enabled_for_disk_encryption     = true
  enabled_for_template_deployment = false
  enable_rbac_authorization       = true
  purge_protection_enabled        = true
  soft_delete_retention_days      = 90

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ip_ranges
  }

  tags = local.tags
}

# Key Vault Access Policy for Current User/Service Principal
resource "azurerm_role_assignment" "keyvault_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# User Assigned Managed Identity for Services
resource "azurerm_user_assigned_identity" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-identity"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Grant Managed Identity access to Key Vault
resource "azurerm_role_assignment" "identity_keyvault_secrets" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.main.principal_id
}

# Grant Managed Identity access to Storage Account
resource "azurerm_role_assignment" "identity_storage_blob_contributor" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.main.principal_id
}

# Grant Managed Identity access to Event Hubs
resource "azurerm_role_assignment" "identity_eventhub_data_owner" {
  scope                = azurerm_eventhub_namespace.main.id
  role_definition_name = "Azure Event Hubs Data Owner"
  principal_id         = azurerm_user_assigned_identity.main.principal_id
}

# Storage Account Encryption with Customer Managed Key (optional, requires additional setup)
resource "azurerm_key_vault_key" "storage_encryption" {
  count = var.enable_customer_managed_keys ? 1 : 0

  name         = "storage-encryption-key"
  key_vault_id = azurerm_key_vault.main.id
  key_type     = "RSA"
  key_size     = 2048

  key_opts = [
    "decrypt",
    "encrypt",
    "sign",
    "unwrapKey",
    "verify",
    "wrapKey",
  ]

  rotation_policy {
    automatic {
      time_before_expiry = "P30D"
    }

    expire_after         = "P365D"
    notify_before_expiry = "P29D"
  }

  tags = local.tags
}

# Azure Policy Assignment for Compliance
resource "azurerm_resource_group_policy_assignment" "require_encryption" {
  name                 = "require-encryption"
  resource_group_id    = azurerm_resource_group.rg.id
  policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/7433c107-6db4-4ad1-b57a-a76dce0154a1"
  
  parameters = jsonencode({
    listOfAllowedSKUs = {
      value = ["Premium_LRS", "Premium_ZRS", "Standard_GRS", "Standard_LRS", "Standard_ZRS"]
    }
  })
}

# Azure Policy Assignment for Tag Compliance
resource "azurerm_resource_group_policy_assignment" "require_tags" {
  name                 = "require-tags"
  resource_group_id    = azurerm_resource_group.rg.id
  policy_definition_id = "/providers/Microsoft.Authorization/policyDefinitions/96670d01-0a4d-4649-9c89-2d3abc0a5025"
  
  parameters = jsonencode({
    tagName = {
      value = "Environment"
    }
  })
}

# Store important secrets in Key Vault
resource "azurerm_key_vault_secret" "storage_connection_string" {
  name         = "storage-connection-string"
  value        = azurerm_storage_account.sa.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.keyvault_admin]
}

resource "azurerm_key_vault_secret" "eventhub_connection_string" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace.main.default_primary_connection_string
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.keyvault_admin]
}

resource "azurerm_key_vault_secret" "app_insights_key" {
  name         = "app-insights-instrumentation-key"
  value        = azurerm_application_insights.main.instrumentation_key
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.keyvault_admin]
}