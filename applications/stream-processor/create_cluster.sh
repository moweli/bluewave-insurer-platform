#!/bin/bash

# Create cost-optimized Databricks cluster
# Single-node, spot instance, auto-terminate

RESOURCE_GROUP="bluewave-rg"
WORKSPACE_NAME="bw-demo-databricks"
CLUSTER_NAME="demo-single-node"

echo "🚀 Creating cost-optimized Databricks cluster..."

# Get workspace URL and ID
WORKSPACE_URL=$(az databricks workspace show \
  --resource-group $RESOURCE_GROUP \
  --name $WORKSPACE_NAME \
  --query workspaceUrl -o tsv)

WORKSPACE_ID=$(az databricks workspace show \
  --resource-group $RESOURCE_GROUP \
  --name $WORKSPACE_NAME \
  --query id -o tsv)

echo "Workspace URL: https://$WORKSPACE_URL"

# Create cluster configuration
cat > cluster_config.json << EOF
{
  "cluster_name": "$CLUSTER_NAME",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_F4s",
  "driver_node_type_id": "Standard_F4s",
  "num_workers": 0,
  "autotermination_minutes": 10,
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "8",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true"
  },
  "azure_attributes": {
    "availability": "SPOT_AZURE",
    "first_on_demand": 0,
    "spot_bid_max_price": 0.08
  },
  "custom_tags": {
    "Environment": "Demo",
    "CostCenter": "Demo",
    "AutoDelete": "true"
  }
}
EOF

echo "✅ Cluster configuration created"
echo ""
echo "To create the cluster, use Databricks CLI or UI:"
echo ""
echo "Using Databricks CLI:"
echo "databricks clusters create --json-file cluster_config.json"
echo ""
echo "Or access the UI at:"
echo "https://$WORKSPACE_URL"
echo ""
echo "💰 Estimated cost: $0.05-0.08/hour (spot) or $0.20/hour (on-demand)"
echo "⏱️ Auto-terminates after 10 minutes idle"

# Try to configure Databricks CLI if not already done
if ! command -v databricks &> /dev/null; then
    echo ""
    echo "⚠️ Databricks CLI not installed. Install with:"
    echo "pip install databricks-cli"
    echo ""
    echo "Then configure with:"
    echo "databricks configure --token"
    echo "Host: https://$WORKSPACE_URL"
fi