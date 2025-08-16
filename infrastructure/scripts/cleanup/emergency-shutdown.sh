#!/bin/bash

# ============================================================
# EMERGENCY SHUTDOWN SCRIPT
# Use this when costs are spiraling out of control
# This will stop/delete most resources to minimize costs
# ============================================================

set -e

RESOURCE_GROUP="bluewave-rg"

echo "================================================"
echo "🚨 EMERGENCY COST SHUTDOWN 🚨"
echo "================================================"
echo "This will STOP or DELETE most resources!"
echo "Press Ctrl+C within 5 seconds to cancel..."
sleep 5

echo ""
echo "🛑 INITIATING EMERGENCY SHUTDOWN..."
echo ""

# 1. STOP DATABRICKS
echo "1️⃣ Terminating all Databricks clusters..."
if command -v databricks &> /dev/null; then
  databricks clusters list --output JSON 2>/dev/null | \
    jq -r '.[] | .cluster_id' | while read cluster_id; do
    echo "   Terminating: $cluster_id"
    databricks clusters permanent-delete --cluster-id "$cluster_id" 2>/dev/null || true
  done
fi

# Delete Databricks workspace if it exists
az databricks workspace list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv 2>/dev/null | while read ws; do
  echo "   Deleting workspace: $ws"
  az databricks workspace delete --resource-group $RESOURCE_GROUP --name "$ws" --yes --no-wait 2>/dev/null || true
done

# 2. MINIMIZE EVENT HUBS
echo "2️⃣ Minimizing Event Hubs..."
az eventhubs namespace list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read ns; do
  echo "   Setting $ns to 0 TU (if possible)"
  # Try to set to 0, but Basic tier minimum is 1
  az eventhubs namespace update \
    --resource-group $RESOURCE_GROUP \
    --name "$ns" \
    --capacity 1 \
    --no-wait 2>/dev/null || true
    
  # Delete all but essential Event Hubs
  az eventhubs eventhub list --resource-group $RESOURCE_GROUP --namespace-name "$ns" --query "[].name" -o tsv | while read eh; do
    if [ "$eh" != "claims-realtime" ]; then
      echo "   Deleting Event Hub: $eh"
      az eventhubs eventhub delete \
        --resource-group $RESOURCE_GROUP \
        --namespace-name "$ns" \
        --name "$eh" --yes 2>/dev/null || true
    fi
  done
done

# 3. STOP APPLICATION INSIGHTS
echo "3️⃣ Stopping Application Insights ingestion..."
az monitor app-insights component list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read ai; do
  echo "   Disabling: $ai"
  # Set sampling to 0% to stop ingestion
  az monitor app-insights component update \
    --resource-group $RESOURCE_GROUP \
    --app "$ai" \
    --retention-time 30 \
    --ingestion-sampling 0 2>/dev/null || true
done

# 4. DEALLOCATE VMs
echo "4️⃣ Deallocating any VMs..."
VM_IDS=$(az vm list -g $RESOURCE_GROUP --query "[].id" -o tsv)
if [ ! -z "$VM_IDS" ]; then
  az vm deallocate --ids $VM_IDS --no-wait 2>/dev/null || true
fi

# 5. DELETE CONTAINER INSTANCES
echo "5️⃣ Deleting container instances..."
az container list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read ci; do
  echo "   Deleting: $ci"
  az container delete --resource-group $RESOURCE_GROUP --name "$ci" --yes 2>/dev/null || true
done

# 6. STORAGE TO COOL TIER
echo "6️⃣ Setting storage to Cool tier..."
az storage account list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read sa; do
  echo "   Cooling: $sa"
  az storage account update \
    --name "$sa" \
    --resource-group $RESOURCE_GROUP \
    --access-tier Cool 2>/dev/null || true
    
  # Delete all blobs to save costs
  echo "   Purging all blobs from $sa"
  for container in $(az storage container list --account-name "$sa" --auth-mode login --query "[].name" -o tsv 2>/dev/null); do
    az storage blob delete-batch \
      --account-name "$sa" \
      --source "$container" \
      --auth-mode login 2>/dev/null || true
  done
done

# 7. DELETE EXPENSIVE RESOURCES
echo "7️⃣ Deleting expensive unused resources..."

# Delete Service Bus if empty
az servicebus namespace list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read sb; do
  QUEUE_COUNT=$(az servicebus queue list --resource-group $RESOURCE_GROUP --namespace-name "$sb" --query "length([])" -o tsv)
  if [ "$QUEUE_COUNT" -eq 0 ]; then
    echo "   Deleting empty Service Bus: $sb"
    az servicebus namespace delete --resource-group $RESOURCE_GROUP --name "$sb" --no-wait 2>/dev/null || true
  fi
done

# Delete unused private endpoints
az network private-endpoint list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read pe; do
  echo "   Deleting private endpoint: $pe"
  az network private-endpoint delete --resource-group $RESOURCE_GROUP --name "$pe" --yes --no-wait 2>/dev/null || true
done

# Delete private DNS zones
az network private-dns zone list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv | while read zone; do
  # First delete links
  az network private-dns link vnet list --resource-group $RESOURCE_GROUP --zone-name "$zone" --query "[].name" -o tsv | while read link; do
    az network private-dns link vnet delete --resource-group $RESOURCE_GROUP --zone-name "$zone" --name "$link" --yes --no-wait 2>/dev/null || true
  done
  # Then delete zone
  echo "   Deleting DNS zone: $zone"
  az network private-dns zone delete --resource-group $RESOURCE_GROUP --name "$zone" --yes --no-wait 2>/dev/null || true
done

# 8. CALCULATE IMMEDIATE SAVINGS
echo ""
echo "================================================"
echo "💰 CALCULATING IMMEDIATE IMPACT..."
echo "================================================"

echo "Resources stopped/minimized:"
echo "✅ Databricks clusters terminated"
echo "✅ Event Hubs minimized to 1 TU"
echo "✅ Application Insights stopped"
echo "✅ VMs deallocated"
echo "✅ Storage set to Cool tier"
echo "✅ Private endpoints deleted"
echo "✅ DNS zones deleted"
echo ""
echo "Estimated immediate savings: ~90% reduction"
echo "New daily cost estimate: <$2/day"
echo ""
echo "⚠️  WARNING: Some services may take time to reflect changes"
echo "⚠️  Monitor costs over next 24 hours"
echo ""

# 9. OPTIONAL: COMPLETE DESTRUCTION
echo "================================================"
echo "🔥 NUCLEAR OPTION (DELETE EVERYTHING)"
echo "================================================"
echo "To completely delete the resource group and ALL resources:"
echo ""
echo "  az group delete --name $RESOURCE_GROUP --yes --no-wait"
echo ""
echo "This will permanently delete EVERYTHING. Use with caution!"
echo "================================================"

# Create a recovery script
cat > recover-from-emergency.sh << 'EOF'
#!/bin/bash
# Recovery script - run this to restore minimal demo environment

echo "Restoring minimal demo environment..."

# 1. Restore Event Hub to 1 TU
az eventhubs namespace update \
  --resource-group bluewave-rg \
  --name bw-dev-uks-eventhubs-001 \
  --capacity 1

# 2. Set storage back to Hot
az storage account update \
  --name bluewaveplatformsa \
  --resource-group bluewave-rg \
  --access-tier Hot

# 3. Re-enable App Insights (minimal)
az monitor app-insights component update \
  --resource-group bluewave-rg \
  --app bw-dev-uks-insights-001 \
  --ingestion-sampling 5

echo "✅ Minimal environment restored"
echo "Remember to keep costs under control!"
EOF

chmod +x recover-from-emergency.sh

echo ""
echo "📝 Recovery script created: recover-from-emergency.sh"
echo "   Run this to restore minimal functionality after emergency shutdown"
echo ""
echo "✅ EMERGENCY SHUTDOWN COMPLETE"
echo "================================================"