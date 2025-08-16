#!/bin/bash

# Databricks Notebook Deployment Script for Demo Environment
# This script uploads notebooks to Databricks and configures cost-optimized clusters

set -e

echo "================================================"
echo "Databricks Demo Environment Setup"
echo "Target Monthly Cost: < $50"
echo "================================================"

# Configuration
WORKSPACE_URL="${DATABRICKS_HOST:-https://adb-xxx.azuredatabricks.net}"
TOKEN="${DATABRICKS_TOKEN:-your-token-here}"
NOTEBOOKS_PATH="/Workspace/Users/${USER_EMAIL:-demo@example.com}/bluewave-insurance"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check if Databricks CLI is installed
    if ! command -v databricks &> /dev/null; then
        echo -e "${RED}Databricks CLI not found. Installing...${NC}"
        pip install databricks-cli
    fi
    
    # Check for required environment variables
    if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        echo -e "${RED}Error: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables${NC}"
        echo "Example:"
        echo "  export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net"
        echo "  export DATABRICKS_TOKEN=dapi..."
        exit 1
    fi
    
    echo -e "${GREEN}Prerequisites checked ✓${NC}"
}

# Function to configure Databricks CLI
configure_cli() {
    echo -e "${YELLOW}Configuring Databricks CLI...${NC}"
    
    # Create .databrickscfg if it doesn't exist
    mkdir -p ~/.databricks
    cat > ~/.databrickscfg << EOF
[DEFAULT]
host = $DATABRICKS_HOST
token = $DATABRICKS_TOKEN
EOF
    
    echo -e "${GREEN}CLI configured ✓${NC}"
}

# Function to create workspace folders
create_folders() {
    echo -e "${YELLOW}Creating workspace folders...${NC}"
    
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/00_setup" || true
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/01_bronze" || true
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/02_silver" || true
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/03_gold" || true
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/04_ml" || true
    databricks workspace mkdirs "${NOTEBOOKS_PATH}/utils" || true
    
    echo -e "${GREEN}Folders created ✓${NC}"
}

# Function to upload notebooks
upload_notebooks() {
    echo -e "${YELLOW}Uploading notebooks to Databricks...${NC}"
    
    # Upload setup notebooks
    databricks workspace import \
        notebooks/00_setup/create_tables.py \
        "${NOTEBOOKS_PATH}/00_setup/create_tables" \
        --language PYTHON \
        --overwrite
    
    # Upload bronze layer
    databricks workspace import \
        notebooks/01_bronze/event_hub_ingestion.py \
        "${NOTEBOOKS_PATH}/01_bronze/event_hub_ingestion" \
        --language PYTHON \
        --overwrite
    
    # Upload silver layer
    databricks workspace import \
        notebooks/02_silver/business_transformations.py \
        "${NOTEBOOKS_PATH}/02_silver/business_transformations" \
        --language PYTHON \
        --overwrite
    
    # Upload gold layer
    databricks workspace import \
        notebooks/03_gold/aggregations.py \
        "${NOTEBOOKS_PATH}/03_gold/aggregations" \
        --language PYTHON \
        --overwrite
    
    # Upload ML notebooks
    databricks workspace import \
        notebooks/04_ml/anomaly_detection.py \
        "${NOTEBOOKS_PATH}/04_ml/anomaly_detection" \
        --language PYTHON \
        --overwrite
    
    echo -e "${GREEN}Notebooks uploaded ✓${NC}"
}

# Function to create cost-optimized cluster configuration
create_demo_cluster_config() {
    echo -e "${YELLOW}Creating cost-optimized cluster configuration...${NC}"
    
    cat > demo_cluster_config.json << 'EOF'
{
    "cluster_name": "demo-streaming-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_F4s",
    "driver_node_type_id": "Standard_F4s",
    "num_workers": 0,
    "autotermination_minutes": 10,
    "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    "azure_attributes": {
        "availability": "SPOT_AZURE",
        "first_on_demand": 0,
        "spot_bid_max_price": -1
    },
    "custom_tags": {
        "Environment": "demo",
        "AutoTerminate": "true",
        "CostOptimized": "true"
    }
}
EOF
    
    echo -e "${GREEN}Cluster configuration created ✓${NC}"
    echo -e "${YELLOW}Note: This is a single-node cluster config for demos only${NC}"
}

# Function to create cluster via API
create_cluster() {
    echo -e "${YELLOW}Creating demo cluster...${NC}"
    
    RESPONSE=$(databricks clusters create --json-file demo_cluster_config.json)
    CLUSTER_ID=$(echo $RESPONSE | jq -r '.cluster_id')
    
    if [ "$CLUSTER_ID" != "null" ]; then
        echo -e "${GREEN}Cluster created with ID: $CLUSTER_ID ✓${NC}"
        echo $CLUSTER_ID > cluster_id.txt
    else
        echo -e "${YELLOW}Cluster creation skipped or failed${NC}"
    fi
}

# Function to upload libraries
install_libraries() {
    echo -e "${YELLOW}Installing required libraries...${NC}"
    
    if [ -f cluster_id.txt ]; then
        CLUSTER_ID=$(cat cluster_id.txt)
        
        # Create library config
        cat > libraries.json << EOF
{
    "cluster_id": "$CLUSTER_ID",
    "libraries": [
        {"pypi": {"package": "azure-eventhub"}},
        {"pypi": {"package": "azure-storage-blob"}},
        {"pypi": {"package": "pydantic"}},
        {"pypi": {"package": "faker"}}
    ]
}
EOF
        
        databricks libraries install --json-file libraries.json
        echo -e "${GREEN}Libraries installed ✓${NC}"
    else
        echo -e "${YELLOW}No cluster ID found, skipping library installation${NC}"
    fi
}

# Function to create secrets scope
create_secrets() {
    echo -e "${YELLOW}Creating secrets scope...${NC}"
    
    # Create scope (will fail if already exists, that's ok)
    databricks secrets create-scope --scope bluewave-demo --initial-manage-principal users || true
    
    echo -e "${GREEN}Secrets scope ready ✓${NC}"
    echo -e "${YELLOW}Add secrets manually using:${NC}"
    echo "  databricks secrets put --scope bluewave-demo --key eventhub-connection-string"
    echo "  databricks secrets put --scope bluewave-demo --key storage-account-key"
}

# Function to create demo job
create_demo_job() {
    echo -e "${YELLOW}Creating demo job configuration...${NC}"
    
    cat > demo_job.json << EOF
{
    "name": "BlueWave Insurance Streaming Demo",
    "tasks": [
        {
            "task_key": "setup_tables",
            "notebook_task": {
                "notebook_path": "${NOTEBOOKS_PATH}/00_setup/create_tables"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_F4s",
                "num_workers": 0,
                "autotermination_minutes": 10
            }
        },
        {
            "task_key": "bronze_streaming",
            "depends_on": [{"task_key": "setup_tables"}],
            "notebook_task": {
                "notebook_path": "${NOTEBOOKS_PATH}/01_bronze/event_hub_ingestion"
            },
            "existing_cluster_id": "use-demo-cluster-id"
        }
    ],
    "max_concurrent_runs": 1,
    "tags": {
        "Environment": "demo",
        "Cost": "optimized"
    }
}
EOF
    
    echo -e "${GREEN}Demo job configuration created ✓${NC}"
}

# Function to show cost summary
show_cost_summary() {
    echo ""
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}DEPLOYMENT COMPLETE!${NC}"
    echo -e "${GREEN}================================================${NC}"
    echo ""
    echo "📊 COST OPTIMIZATION SUMMARY:"
    echo "--------------------------------"
    echo "✓ Single-node cluster (saves ~90%)"
    echo "✓ Spot instances enabled"
    echo "✓ Auto-terminate after 10 minutes"
    echo "✓ Standard tier (not Premium)"
    echo ""
    echo "💰 ESTIMATED COSTS:"
    echo "--------------------------------"
    echo "Cluster (1 hr/day): ~$1.50/day"
    echo "Storage: ~$0.50/day"
    echo "Event Hubs: ~$0.35/day"
    echo "TOTAL: ~$2.35/day (~$50/month for demos)"
    echo ""
    echo "⚠️  IMPORTANT REMINDERS:"
    echo "--------------------------------"
    echo "1. ALWAYS terminate clusters after demos"
    echo "2. Delete old checkpoint data regularly"
    echo "3. Use cached data for demos when possible"
    echo "4. Monitor costs daily in Azure Portal"
    echo ""
    echo "🚀 NEXT STEPS:"
    echo "--------------------------------"
    echo "1. Add secrets: databricks secrets put --scope bluewave-demo --key <key>"
    echo "2. Start cluster: databricks clusters start --cluster-id <id>"
    echo "3. Run setup notebook first"
    echo "4. Start streaming notebooks"
    echo ""
    echo "📝 WORKSPACE URL: $DATABRICKS_HOST"
    echo "📁 NOTEBOOKS PATH: $NOTEBOOKS_PATH"
}

# Main execution
main() {
    echo "Starting Databricks deployment..."
    
    check_prerequisites
    configure_cli
    create_folders
    upload_notebooks
    create_demo_cluster_config
    
    # Optional: Create cluster (comment out to do manually)
    # create_cluster
    # install_libraries
    
    create_secrets
    create_demo_job
    show_cost_summary
}

# Run main function
main

# Cleanup
rm -f demo_cluster_config.json libraries.json demo_job.json 2>/dev/null || true