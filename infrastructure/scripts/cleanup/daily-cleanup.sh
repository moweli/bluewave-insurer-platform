#!/bin/bash

# ============================================================
# Daily Cleanup Script for BlueWave Demo Environment
# Runs automatically to prevent cost creep
# Target: Keep costs under $2/day
# ============================================================

set -e

RESOURCE_GROUP="bluewave-rg"
STORAGE_ACCOUNT="bluewaveplatformsa"
EVENTHUB_NAMESPACE="bw-dev-uks-eventhubs-001"

echo "================================================"
echo "🧹 DAILY CLEANUP - $(date)"
echo "================================================"

# 1. CHECK DAILY COSTS
echo "📊 Checking today's costs..."
DAILY_COST=$(az consumption usage list \
  --start-date $(date +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --query "sum([?resourceGroup=='$RESOURCE_GROUP'].pretaxCost)" \
  -o tsv 2>/dev/null || echo "0")

echo "   Today's cost so far: \$$DAILY_COST"

if (( $(echo "$DAILY_COST > 2" | bc -l 2>/dev/null || echo 0) )); then
  echo "   ⚠️  WARNING: Daily budget exceeded!"
fi

# 2. CLEAN STORAGE DATA
echo "🗑️  Cleaning old storage data..."

# Delete blobs older than 7 days
for container in raw bronze silver gold audit; do
  echo "   Cleaning container: $container"
  az storage blob list \
    --account-name $STORAGE_ACCOUNT \
    --container-name $container \
    --query "[?properties.lastModified < '$(date -d '7 days ago' --iso-8601)'].name" \
    -o tsv 2>/dev/null | while read blob; do
    az storage blob delete \
      --account-name $STORAGE_ACCOUNT \
      --container-name $container \
      --name "$blob" \
      --auth-mode login 2>/dev/null || true
  done
done

# 3. CLEAR CHECKPOINTS
echo "🔄 Clearing old checkpoints..."
az storage blob delete-batch \
  --account-name $STORAGE_ACCOUNT \
  --source eventhub-checkpoints \
  --pattern "*" \
  --auth-mode login 2>/dev/null || true

# 4. CHECK DATABRICKS CLUSTERS
echo "🖥️  Checking for running Databricks clusters..."
if command -v databricks &> /dev/null; then
  RUNNING_CLUSTERS=$(databricks clusters list --output JSON 2>/dev/null | jq -r '.[] | select(.state == "RUNNING") | .cluster_id' | wc -l)
  if [ "$RUNNING_CLUSTERS" -gt 0 ]; then
    echo "   ⚠️  WARNING: $RUNNING_CLUSTERS clusters still running!"
    echo "   Run: databricks clusters list"
  fi
else
  echo "   Databricks CLI not installed, skipping cluster check"
fi

# 5. CHECK EVENT HUB USAGE
echo "📡 Checking Event Hub metrics..."
THROUGHPUT=$(az eventhubs namespace show \
  --resource-group $RESOURCE_GROUP \
  --name $EVENTHUB_NAMESPACE \
  --query "sku.capacity" -o tsv 2>/dev/null || echo "0")

if [ "$THROUGHPUT" -gt 1 ]; then
  echo "   ⚠️  Event Hub using $THROUGHPUT TUs - consider reducing to 1"
fi

# 6. GENERATE SUMMARY
echo ""
echo "================================================"
echo "📈 CLEANUP SUMMARY"
echo "================================================"
echo "✅ Storage cleaned"
echo "✅ Checkpoints cleared"
echo "💰 Today's cost: \$$DAILY_COST"
echo "🎯 Target: <\$2/day"

if (( $(echo "$DAILY_COST < 2" | bc -l 2>/dev/null || echo 1) )); then
  echo "✅ Within budget!"
else
  echo "❌ Over budget - review resources!"
fi

echo "================================================"
echo "Next cleanup: $(date -d 'tomorrow 23:00')"
echo "================================================"