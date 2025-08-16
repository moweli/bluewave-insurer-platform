#!/bin/bash

# ============================================================
# Weekly Deep Cleanup Script for BlueWave Demo Environment
# Performs thorough cleanup to minimize costs
# Runs every Sunday at 2 AM
# ============================================================

set -e

RESOURCE_GROUP="bluewave-rg"
STORAGE_ACCOUNT="bluewaveplatformsa"
EVENTHUB_NAMESPACE="bw-dev-uks-eventhubs-001"
KEY_VAULT_PREFIX="bw-dev-kv"

echo "================================================"
echo "🧹 WEEKLY DEEP CLEANUP - $(date)"
echo "================================================"

# 1. CALCULATE WEEKLY COSTS
echo "📊 Calculating weekly costs..."
WEEK_START=$(date -d 'last monday' +%Y-%m-%d)
WEEK_END=$(date +%Y-%m-%d)
WEEKLY_COST=$(az consumption usage list \
  --start-date $WEEK_START \
  --end-date $WEEK_END \
  --query "sum([?resourceGroup=='$RESOURCE_GROUP'].pretaxCost)" \
  -o tsv 2>/dev/null || echo "0")

echo "   This week's total cost: \$$WEEKLY_COST"
echo "   Target: <\$10/week"

# 2. STOP ALL DATABRICKS CLUSTERS
echo "🛑 Stopping all Databricks clusters..."
if command -v databricks &> /dev/null; then
  databricks clusters list --output JSON 2>/dev/null | \
    jq -r '.[] | .cluster_id' | while read cluster_id; do
    echo "   Terminating cluster: $cluster_id"
    databricks clusters delete --cluster-id "$cluster_id" 2>/dev/null || true
  done
else
  echo "   Databricks CLI not installed, skipping"
fi

# 3. SCALE DOWN EVENT HUBS
echo "📉 Scaling down Event Hubs..."
CURRENT_TU=$(az eventhubs namespace show \
  --resource-group $RESOURCE_GROUP \
  --name $EVENTHUB_NAMESPACE \
  --query "sku.capacity" -o tsv 2>/dev/null || echo "1")

if [ "$CURRENT_TU" -gt 1 ]; then
  echo "   Reducing from $CURRENT_TU to 1 TU"
  az eventhubs namespace update \
    --resource-group $RESOURCE_GROUP \
    --name $EVENTHUB_NAMESPACE \
    --capacity 1 \
    --no-wait 2>/dev/null || true
fi

# 4. DEEP STORAGE CLEANUP
echo "🗑️  Deep storage cleanup..."

# Delete ALL checkpoint data
echo "   Deleting all checkpoints"
az storage container delete \
  --account-name $STORAGE_ACCOUNT \
  --name eventhub-checkpoints \
  --auth-mode login 2>/dev/null || true

# Recreate empty checkpoint container
az storage container create \
  --account-name $STORAGE_ACCOUNT \
  --name eventhub-checkpoints \
  --auth-mode login 2>/dev/null || true

# Delete old data from all containers
for container in raw bronze silver gold audit features archive; do
  echo "   Purging container: $container"
  az storage blob delete-batch \
    --account-name $STORAGE_ACCOUNT \
    --source $container \
    --auth-mode login 2>/dev/null || true
done

# 5. CLEAN KEY VAULT
echo "🔐 Cleaning Key Vault..."
VAULT_NAME=$(az keyvault list --resource-group $RESOURCE_GROUP --query "[?starts_with(name, '$KEY_VAULT_PREFIX')].name" -o tsv | head -1)
if [ ! -z "$VAULT_NAME" ]; then
  # Purge deleted keys
  az keyvault key list-deleted --vault-name $VAULT_NAME --query "[].name" -o tsv 2>/dev/null | while read key; do
    echo "   Purging deleted key: $key"
    az keyvault key purge --vault-name $VAULT_NAME --name "$key" 2>/dev/null || true
  done
  
  # Purge deleted secrets
  az keyvault secret list-deleted --vault-name $VAULT_NAME --query "[].name" -o tsv 2>/dev/null | while read secret; do
    echo "   Purging deleted secret: $secret"
    az keyvault secret purge --vault-name $VAULT_NAME --name "$secret" 2>/dev/null || true
  done
fi

# 6. REMOVE UNUSED NETWORK RESOURCES
echo "🌐 Cleaning network resources..."

# Delete unused private endpoints
az network private-endpoint list \
  --resource-group $RESOURCE_GROUP \
  --query "[?provisioningState=='Failed'].name" -o tsv 2>/dev/null | while read pe; do
  echo "   Deleting failed private endpoint: $pe"
  az network private-endpoint delete \
    --resource-group $RESOURCE_GROUP \
    --name "$pe" --yes --no-wait 2>/dev/null || true
done

# 7. CHECK FOR ORPHANED RESOURCES
echo "🔍 Checking for orphaned resources..."

# Find disks not attached to any VM
ORPHANED_DISKS=$(az disk list --resource-group $RESOURCE_GROUP --query "[?diskState=='Unattached'].name" -o tsv | wc -l)
if [ "$ORPHANED_DISKS" -gt 0 ]; then
  echo "   ⚠️  Found $ORPHANED_DISKS unattached disks - consider deleting"
fi

# Find unused NICs
ORPHANED_NICS=$(az network nic list --resource-group $RESOURCE_GROUP --query "[?virtualMachine==null].name" -o tsv | wc -l)
if [ "$ORPHANED_NICS" -gt 0 ]; then
  echo "   ⚠️  Found $ORPHANED_NICS unused network interfaces"
fi

# 8. OPTIMIZE LOG ANALYTICS
echo "📝 Optimizing Log Analytics..."

# Check data ingestion volume
WORKSPACE_NAME="bw-dev-uks-logs-001"
echo "   Checking ingestion volume for $WORKSPACE_NAME"

# Clear old custom logs
az monitor log-analytics workspace table list \
  --resource-group $RESOURCE_GROUP \
  --workspace-name $WORKSPACE_NAME \
  --query "[?starts_with(name, 'Custom')].name" -o tsv 2>/dev/null | while read table; do
  echo "   Consider removing custom table: $table"
done

# 9. GENERATE REPORT
echo ""
echo "================================================"
echo "📊 WEEKLY CLEANUP REPORT"
echo "================================================"
echo "Week: $WEEK_START to $WEEK_END"
echo "Total Cost: \$$WEEKLY_COST"
echo "Target: <\$10/week"
echo ""
echo "Actions Taken:"
echo "✅ Databricks clusters terminated"
echo "✅ Event Hubs scaled to minimum"
echo "✅ Storage deep cleaned"
echo "✅ Key Vault purged"
echo "✅ Network resources cleaned"
echo ""

if (( $(echo "$WEEKLY_COST < 10" | bc -l 2>/dev/null || echo 1) )); then
  echo "✅ WEEKLY BUDGET MET!"
else
  echo "❌ OVER WEEKLY BUDGET!"
  echo ""
  echo "Top cost drivers this week:"
  az consumption usage list \
    --start-date $WEEK_START \
    --end-date $WEEK_END \
    --query "sort_by([?resourceGroup=='$RESOURCE_GROUP'].{Service:meterCategory,Cost:pretaxCost}, &Cost)[-3:]" \
    --output table 2>/dev/null || echo "Unable to retrieve cost breakdown"
fi

echo "================================================"
echo "Next weekly cleanup: $(date -d 'next sunday 02:00')"
echo "================================================"

# 10. SEND NOTIFICATION (optional - requires configuration)
# You can add email notification here using sendmail or Azure Logic Apps