# Azure Infrastructure vs Terraform Configuration Comparison

## Summary
The Azure infrastructure is **mostly deployed** with some differences from the Terraform configuration.

## ✅ Resources That Match Terraform Configuration

### Core Infrastructure
- **Resource Group**: `bluewave-rg` in `uksouth` ✅
- **Storage Account**: `bluewaveplatformsa` 
  - Type: StorageV2 ✅
  - HNS Enabled: true ✅
  - Location: uksouth ✅
  - **Note**: Using GRS replication (deployed) vs LRS (Terraform default)

### Networking
- **Virtual Network**: `bw-dev-uks-vnet-001`
  - Address Space: 10.0.0.0/16 ✅
  - Subnets present: private-endpoints, databricks-public, databricks-private ✅

### Event Hubs
- **Namespace**: `bw-dev-uks-eventhubs-001`
  - SKU: Standard ✅
  - Capacity: 2 ✅
  - Auto-inflate: Enabled ✅
- **Event Hubs**:
  - `claims-realtime`: 32 partitions, 7-day retention ✅
  - `audit-events`: 8 partitions (vs 4 in Terraform), 3-day retention ✅

### Security
- **Key Vault**: `bwdevukskv001`
  - Soft Delete: Enabled ✅
  - Purge Protection: Enabled ✅
- **Managed Identity**: `bw-dev-uksouth-identity-001` ✅

### Monitoring
- **Log Analytics**: `bw-dev-uks-logs-001`
  - SKU: PerGB2018 ✅
  - Retention: 730 days (vs 30 in Terraform)
- **Application Insights**: `bw-dev-uks-appinsights-001` ✅
- **Action Groups**: Present ✅

### Data Platform
- **Databricks**: `bw-dev-uks-databricks-001`
  - SKU: Premium ✅
  - Managed RG: bluewave-rg-databricks-managed ✅

### Private DNS Zones ✅
- privatelink.blob.core.windows.net
- privatelink.dfs.core.windows.net
- privatelink.vaultcore.azure.net
- privatelink.servicebus.windows.net
- privatelink.cognitiveservices.azure.com
- privatelink.azurecr.io

### Private Endpoints ✅
- Storage Account (blob and dfs)
- Key Vault
- Event Hubs
- Cognitive Services
- Container Registry

## 🔄 Additional Resources in Azure (Not in Current Terraform)

1. **Azure Cognitive Services**: `bw-dev-uks-cognitive-001`
2. **Azure Container Registry**: `bwdevuksacr001`
3. **Azure Service Bus**: `bw-dev-uks-servicebus-001`
4. **Databricks Access Connector**: `bw-dev-databricks-access`
5. **Additional monitoring solutions**: ContainerInsights, Security

## ⚠️ Configuration Differences

1. **Storage Replication**: 
   - Azure: GRS (Geo-Redundant Storage)
   - Terraform: LRS (Locally Redundant Storage)

2. **Log Retention**:
   - Azure: 730 days (2 years)
   - Terraform: 30 days default

3. **Audit Event Hub Partitions**:
   - Azure: 8 partitions
   - Terraform: 4 partitions

4. **Databricks Managed RG Name**:
   - Azure: `bluewave-rg-databricks-managed`
   - Terraform: `bw-dev-databricks-managed-rg`

## 📋 Recommendations

1. **Update Terraform to match deployed resources**:
   - Add Cognitive Services module
   - Add Container Registry module
   - Add Service Bus module
   - Update storage replication type to GRS for production
   - Adjust log retention days

2. **Verify storage containers**: Unable to list containers due to permissions, need to verify through portal or with proper access

3. **State Management**: The infrastructure appears to have been deployed outside of Terraform or with a different configuration. Consider:
   - Importing existing resources into Terraform state
   - Or adjusting Terraform to match current deployment

## Next Steps

To align Terraform with current Azure deployment:
```bash
# Import existing resources into Terraform state
terraform import azurerm_resource_group.rg /subscriptions/d08a685d-04bd-4a4f-befa-b8cab29d4c71/resourceGroups/bluewave-rg
terraform import azurerm_storage_account.sa /subscriptions/d08a685d-04bd-4a4f-befa-b8cab29d4c71/resourceGroups/bluewave-rg/providers/Microsoft.Storage/storageAccounts/bluewaveplatformsa
# ... continue for other resources
```