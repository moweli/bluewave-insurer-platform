#!/bin/bash

# BlueWave Insurance Platform - Permissions Configuration Script
# This script configures RBAC permissions for the platform

set -e

echo "=========================================="
echo "Configuring RBAC Permissions"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Get deployment outputs
if [ ! -f "terraform/deployment-outputs.json" ]; then
    print_warning "deployment-outputs.json not found. Please run setup.sh first."
    exit 1
fi

# Extract resource information
RESOURCE_GROUP=$(jq -r '.resource_group_name.value' terraform/deployment-outputs.json)
STORAGE_ACCOUNT=$(jq -r '.storage_account_name.value' terraform/deployment-outputs.json)
KEY_VAULT=$(jq -r '.key_vault_name.value // empty' terraform/deployment-outputs.json)
DATABRICKS_WORKSPACE=$(jq -r '.databricks_workspace_id.value // empty' terraform/deployment-outputs.json)

print_status "Configuring permissions for resource group: $RESOURCE_GROUP"

# Function to assign role
assign_role() {
    local principal_id=$1
    local role=$2
    local scope=$3
    
    print_status "Assigning role '$role' to principal '$principal_id'"
    az role assignment create \
        --assignee "$principal_id" \
        --role "$role" \
        --scope "$scope" \
        --output none
}

# Configure Data Engineers group permissions
configure_data_engineers() {
    print_status "Configuring Data Engineers permissions..."
    
    # Get or create the group
    GROUP_NAME="BlueWave-Data-Engineers"
    GROUP_ID=$(az ad group list --filter "displayName eq '$GROUP_NAME'" --query "[0].id" -o tsv)
    
    if [ -z "$GROUP_ID" ]; then
        print_status "Creating Azure AD group: $GROUP_NAME"
        GROUP_ID=$(az ad group create --display-name "$GROUP_NAME" --mail-nickname "bluewave-data-engineers" --query "id" -o tsv)
    fi
    
    # Assign roles
    RG_SCOPE="/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP"
    
    assign_role "$GROUP_ID" "Contributor" "$RG_SCOPE"
    assign_role "$GROUP_ID" "Storage Blob Data Contributor" "$RG_SCOPE"
    
    if [ ! -z "$KEY_VAULT" ]; then
        KV_SCOPE="$RG_SCOPE/providers/Microsoft.KeyVault/vaults/$KEY_VAULT"
        assign_role "$GROUP_ID" "Key Vault Secrets User" "$KV_SCOPE"
    fi
    
    print_status "Data Engineers group configured: $GROUP_ID"
}

# Configure Data Scientists group permissions
configure_data_scientists() {
    print_status "Configuring Data Scientists permissions..."
    
    GROUP_NAME="BlueWave-Data-Scientists"
    GROUP_ID=$(az ad group list --filter "displayName eq '$GROUP_NAME'" --query "[0].id" -o tsv)
    
    if [ -z "$GROUP_ID" ]; then
        print_status "Creating Azure AD group: $GROUP_NAME"
        GROUP_ID=$(az ad group create --display-name "$GROUP_NAME" --mail-nickname "bluewave-data-scientists" --query "id" -o tsv)
    fi
    
    RG_SCOPE="/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP"
    
    assign_role "$GROUP_ID" "Reader" "$RG_SCOPE"
    assign_role "$GROUP_ID" "Storage Blob Data Reader" "$RG_SCOPE"
    
    if [ ! -z "$DATABRICKS_WORKSPACE" ]; then
        assign_role "$GROUP_ID" "Contributor" "$DATABRICKS_WORKSPACE"
    fi
    
    print_status "Data Scientists group configured: $GROUP_ID"
}

# Configure Business Analysts group permissions
configure_business_analysts() {
    print_status "Configuring Business Analysts permissions..."
    
    GROUP_NAME="BlueWave-Business-Analysts"
    GROUP_ID=$(az ad group list --filter "displayName eq '$GROUP_NAME'" --query "[0].id" -o tsv)
    
    if [ -z "$GROUP_ID" ]; then
        print_status "Creating Azure AD group: $GROUP_NAME"
        GROUP_ID=$(az ad group create --display-name "$GROUP_NAME" --mail-nickname "bluewave-business-analysts" --query "id" -o tsv)
    fi
    
    RG_SCOPE="/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP"
    STORAGE_SCOPE="$RG_SCOPE/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
    
    assign_role "$GROUP_ID" "Reader" "$RG_SCOPE"
    
    # Only read access to gold layer
    GOLD_SCOPE="$STORAGE_SCOPE/blobServices/default/containers/gold"
    assign_role "$GROUP_ID" "Storage Blob Data Reader" "$GOLD_SCOPE"
    
    print_status "Business Analysts group configured: $GROUP_ID"
}

# Configure service principal for automation
configure_service_principal() {
    print_status "Configuring service principal for automation..."
    
    SP_NAME="BlueWave-Automation-SP"
    
    # Check if service principal exists
    SP_ID=$(az ad sp list --filter "displayName eq '$SP_NAME'" --query "[0].appId" -o tsv)
    
    if [ -z "$SP_ID" ]; then
        print_status "Creating service principal: $SP_NAME"
        SP_CREDENTIALS=$(az ad sp create-for-rbac --name "$SP_NAME" --skip-assignment --output json)
        SP_ID=$(echo $SP_CREDENTIALS | jq -r '.appId')
        SP_SECRET=$(echo $SP_CREDENTIALS | jq -r '.password')
        
        print_warning "Service Principal created. Please save these credentials securely:"
        echo "  App ID: $SP_ID"
        echo "  Secret: $SP_SECRET"
        
        # Store in Key Vault if available
        if [ ! -z "$KEY_VAULT" ]; then
            print_status "Storing credentials in Key Vault..."
            az keyvault secret set --vault-name "$KEY_VAULT" --name "automation-sp-id" --value "$SP_ID" --output none
            az keyvault secret set --vault-name "$KEY_VAULT" --name "automation-sp-secret" --value "$SP_SECRET" --output none
        fi
    fi
    
    # Assign roles
    RG_SCOPE="/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP"
    assign_role "$SP_ID" "Contributor" "$RG_SCOPE"
    
    print_status "Service principal configured: $SP_ID"
}

# Main execution
main() {
    configure_data_engineers
    configure_data_scientists
    configure_business_analysts
    configure_service_principal
    
    echo
    print_status "=========================================="
    print_status "RBAC configuration completed!"
    print_status "=========================================="
    echo
    print_status "Groups created:"
    echo "  - BlueWave-Data-Engineers"
    echo "  - BlueWave-Data-Scientists"
    echo "  - BlueWave-Business-Analysts"
    echo
    print_status "Service principals created:"
    echo "  - BlueWave-Automation-SP"
    echo
    print_status "Add users to groups using:"
    echo "  az ad group member add --group <group-name> --member-id <user-object-id>"
}

main