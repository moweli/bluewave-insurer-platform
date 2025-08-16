# Networking Resources

# Virtual Network
resource "azurerm_virtual_network" "main" {
  name                = "${var.prefix}-${var.environment}-${var.location_short}-vnet"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = [var.vnet_address_space]

  tags = local.tags
}

# Subnet for Private Endpoints
resource "azurerm_subnet" "private_endpoints" {
  name                 = "private-endpoints-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space, 8, 1)]

  private_endpoint_network_policies = "Enabled"
}

# Subnet for Databricks Public
resource "azurerm_subnet" "databricks_public" {
  name                 = "databricks-public-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space, 8, 2)]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

# Subnet for Databricks Private
resource "azurerm_subnet" "databricks_private" {
  name                 = "databricks-private-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space, 8, 3)]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

# Network Security Group for Private Endpoints
resource "azurerm_network_security_group" "private_endpoints" {
  name                = "${var.prefix}-${var.environment}-pe-nsg"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "DenyInternetOutbound"
    priority                   = 100
    direction                  = "Outbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "Internet"
  }

  tags = local.tags
}

# Network Security Group for Databricks Public Subnet
resource "azurerm_network_security_group" "databricks_public" {
  name                = "${var.prefix}-${var.environment}-databricks-public-nsg"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Network Security Group for Databricks Private Subnet
resource "azurerm_network_security_group" "databricks_private" {
  name                = "${var.prefix}-${var.environment}-databricks-private-nsg"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Associate NSG with Private Endpoints Subnet
resource "azurerm_subnet_network_security_group_association" "private_endpoints" {
  subnet_id                 = azurerm_subnet.private_endpoints.id
  network_security_group_id = azurerm_network_security_group.private_endpoints.id
}

# Associate NSG with Databricks Public Subnet
resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks_public.id
}

# Associate NSG with Databricks Private Subnet
resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks_private.id
}

# Private DNS Zone for Storage
resource "azurerm_private_dns_zone" "storage_blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

resource "azurerm_private_dns_zone" "storage_dfs" {
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Private DNS Zone for Key Vault
resource "azurerm_private_dns_zone" "keyvault" {
  name                = "privatelink.vaultcore.azure.net"
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Private DNS Zone for Event Hubs
resource "azurerm_private_dns_zone" "eventhub" {
  name                = "privatelink.servicebus.windows.net"
  resource_group_name = azurerm_resource_group.rg.name

  tags = local.tags
}

# Link DNS Zones to VNet
resource "azurerm_private_dns_zone_virtual_network_link" "storage_blob" {
  name                  = "${var.prefix}-storage-blob-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.storage_blob.name
  virtual_network_id    = azurerm_virtual_network.main.id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "storage_dfs" {
  name                  = "${var.prefix}-storage-dfs-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.storage_dfs.name
  virtual_network_id    = azurerm_virtual_network.main.id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "keyvault" {
  name                  = "${var.prefix}-keyvault-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.keyvault.name
  virtual_network_id    = azurerm_virtual_network.main.id

  tags = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "eventhub" {
  name                  = "${var.prefix}-eventhub-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.eventhub.name
  virtual_network_id    = azurerm_virtual_network.main.id

  tags = local.tags
}

# Private Endpoint for Storage Account - Blob
resource "azurerm_private_endpoint" "storage_blob" {
  name                = "${var.prefix}-${var.environment}-storage-blob-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "${var.prefix}-storage-blob-connection"
    private_connection_resource_id = azurerm_storage_account.sa.id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "storage-blob-dns-group"
    private_dns_zone_ids = [azurerm_private_dns_zone.storage_blob.id]
  }

  tags = local.tags
}

# Private Endpoint for Storage Account - DFS
resource "azurerm_private_endpoint" "storage_dfs" {
  name                = "${var.prefix}-${var.environment}-storage-dfs-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "${var.prefix}-storage-dfs-connection"
    private_connection_resource_id = azurerm_storage_account.sa.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "storage-dfs-dns-group"
    private_dns_zone_ids = [azurerm_private_dns_zone.storage_dfs.id]
  }

  tags = local.tags
}

# Private Endpoint for Key Vault
resource "azurerm_private_endpoint" "keyvault" {
  name                = "${var.prefix}-${var.environment}-keyvault-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "${var.prefix}-keyvault-connection"
    private_connection_resource_id = azurerm_key_vault.main.id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "keyvault-dns-group"
    private_dns_zone_ids = [azurerm_private_dns_zone.keyvault.id]
  }

  tags = local.tags
}

# Private Endpoint for Event Hub Namespace
resource "azurerm_private_endpoint" "eventhub" {
  name                = "${var.prefix}-${var.environment}-eventhub-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "${var.prefix}-eventhub-connection"
    private_connection_resource_id = azurerm_eventhub_namespace.main.id
    subresource_names              = ["namespace"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "eventhub-dns-group"
    private_dns_zone_ids = [azurerm_private_dns_zone.eventhub.id]
  }

  tags = local.tags
}