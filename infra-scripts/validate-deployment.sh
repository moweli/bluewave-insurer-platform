#!/bin/bash

# BlueWave Insurance Platform - Deployment Validation Script
# This script validates that all resources are deployed and accessible

set -e

echo "=========================================="
echo "Validating BlueWave Platform Deployment"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

print_success() {
    echo -e "${GREEN}✓${NC} $1"
    ((PASSED_CHECKS++))
    ((TOTAL_CHECKS++))
}

print_failure() {
    echo -e "${RED}✗${NC} $1"
    ((FAILED_CHECKS++))
    ((TOTAL_CHECKS++))
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

# Get deployment outputs
if [ ! -f "terraform/deployment-outputs.json" ]; then
    print_failure "deployment-outputs.json not found. Please run setup.sh first."
    exit 1
fi

# Extract resource information
RESOURCE_GROUP=$(jq -r '.resource_group_name.value // empty' terraform/deployment-outputs.json)
STORAGE_ACCOUNT=$(jq -r '.storage_account_name.value // empty' terraform/deployment-outputs.json)
EVENT_HUB_NAMESPACE=$(jq -r '.eventhub_namespace_name.value // empty' terraform/deployment-outputs.json)
KEY_VAULT=$(jq -r '.key_vault_name.value // empty' terraform/deployment-outputs.json)
LOG_ANALYTICS=$(jq -r '.log_analytics_workspace_name.value // empty' terraform/deployment-outputs.json)
APP_INSIGHTS=$(jq -r '.application_insights_name.value // empty' terraform/deployment-outputs.json)

echo
print_status "Validating resources in resource group: $RESOURCE_GROUP"
echo

# Validate Resource Group
validate_resource_group() {
    print_status "Checking Resource Group..."
    if az group show --name "$RESOURCE_GROUP" &> /dev/null; then
        print_success "Resource Group '$RESOURCE_GROUP' exists"
    else
        print_failure "Resource Group '$RESOURCE_GROUP' not found"
    fi
}

# Validate Storage Account
validate_storage_account() {
    print_status "Checking Storage Account..."
    if [ ! -z "$STORAGE_ACCOUNT" ]; then
        if az storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Storage Account '$STORAGE_ACCOUNT' exists"
            
            # Check containers
            CONTAINERS=("raw" "bronze" "silver" "gold" "features" "audit" "archive")
            for container in "${CONTAINERS[@]}"; do
                if az storage container show --name "$container" --account-name "$STORAGE_ACCOUNT" --auth-mode login &> /dev/null; then
                    print_success "  Container '$container' exists"
                else
                    print_failure "  Container '$container' not found"
                fi
            done
        else
            print_failure "Storage Account '$STORAGE_ACCOUNT' not found"
        fi
    else
        print_warning "Storage Account not configured"
    fi
}

# Validate Event Hubs
validate_event_hubs() {
    print_status "Checking Event Hubs..."
    if [ ! -z "$EVENT_HUB_NAMESPACE" ]; then
        if az eventhubs namespace show --name "$EVENT_HUB_NAMESPACE" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Event Hub Namespace '$EVENT_HUB_NAMESPACE' exists"
            
            # Check individual hubs
            HUBS=("claims-realtime" "audit-events")
            for hub in "${HUBS[@]}"; do
                if az eventhubs eventhub show --name "$hub" --namespace-name "$EVENT_HUB_NAMESPACE" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
                    print_success "  Event Hub '$hub' exists"
                else
                    print_failure "  Event Hub '$hub' not found"
                fi
            done
        else
            print_failure "Event Hub Namespace '$EVENT_HUB_NAMESPACE' not found"
        fi
    else
        print_warning "Event Hubs not configured"
    fi
}

# Validate Key Vault
validate_key_vault() {
    print_status "Checking Key Vault..."
    if [ ! -z "$KEY_VAULT" ]; then
        if az keyvault show --name "$KEY_VAULT" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Key Vault '$KEY_VAULT' exists"
            
            # Check if accessible
            if az keyvault secret list --vault-name "$KEY_VAULT" &> /dev/null; then
                print_success "  Key Vault is accessible"
            else
                print_failure "  Key Vault is not accessible (check permissions)"
            fi
        else
            print_failure "Key Vault '$KEY_VAULT' not found"
        fi
    else
        print_warning "Key Vault not configured"
    fi
}

# Validate Monitoring
validate_monitoring() {
    print_status "Checking Monitoring Resources..."
    
    # Log Analytics
    if [ ! -z "$LOG_ANALYTICS" ]; then
        if az monitor log-analytics workspace show --name "$LOG_ANALYTICS" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Log Analytics Workspace '$LOG_ANALYTICS' exists"
        else
            print_failure "Log Analytics Workspace '$LOG_ANALYTICS' not found"
        fi
    else
        print_warning "Log Analytics not configured"
    fi
    
    # Application Insights
    if [ ! -z "$APP_INSIGHTS" ]; then
        if az monitor app-insights component show --app "$APP_INSIGHTS" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Application Insights '$APP_INSIGHTS' exists"
        else
            print_failure "Application Insights '$APP_INSIGHTS' not found"
        fi
    else
        print_warning "Application Insights not configured"
    fi
}

# Validate Databricks
validate_databricks() {
    print_status "Checking Databricks..."
    DATABRICKS_WORKSPACE=$(jq -r '.databricks_workspace_name.value // empty' terraform/deployment-outputs.json)
    
    if [ ! -z "$DATABRICKS_WORKSPACE" ]; then
        if az databricks workspace show --name "$DATABRICKS_WORKSPACE" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
            print_success "Databricks Workspace '$DATABRICKS_WORKSPACE' exists"
        else
            print_failure "Databricks Workspace '$DATABRICKS_WORKSPACE' not found"
        fi
    else
        print_warning "Databricks not configured"
    fi
}

# Validate Network Security
validate_network_security() {
    print_status "Checking Network Security..."
    
    # Check for NSGs
    NSG_COUNT=$(az network nsg list --resource-group "$RESOURCE_GROUP" --query "length(@)" -o tsv 2>/dev/null || echo "0")
    if [ "$NSG_COUNT" -gt 0 ]; then
        print_success "Network Security Groups configured ($NSG_COUNT found)"
    else
        print_warning "No Network Security Groups found"
    fi
    
    # Check for VNets
    VNET_COUNT=$(az network vnet list --resource-group "$RESOURCE_GROUP" --query "length(@)" -o tsv 2>/dev/null || echo "0")
    if [ "$VNET_COUNT" -gt 0 ]; then
        print_success "Virtual Networks configured ($VNET_COUNT found)"
    else
        print_warning "No Virtual Networks found"
    fi
}

# Check connectivity
check_connectivity() {
    print_status "Checking service connectivity..."
    
    # Test Storage Account connectivity
    if [ ! -z "$STORAGE_ACCOUNT" ]; then
        STORAGE_KEY=$(az storage account keys list --account-name "$STORAGE_ACCOUNT" --resource-group "$RESOURCE_GROUP" --query "[0].value" -o tsv 2>/dev/null || echo "")
        if [ ! -z "$STORAGE_KEY" ]; then
            if az storage container list --account-name "$STORAGE_ACCOUNT" --account-key "$STORAGE_KEY" &> /dev/null; then
                print_success "Storage Account connectivity verified"
            else
                print_failure "Cannot connect to Storage Account"
            fi
        else
            print_warning "Cannot retrieve Storage Account keys (check permissions)"
        fi
    fi
}

# Generate summary report
generate_summary() {
    echo
    echo "=========================================="
    echo "Validation Summary"
    echo "=========================================="
    echo
    echo "Total checks performed: $TOTAL_CHECKS"
    echo -e "${GREEN}Passed: $PASSED_CHECKS${NC}"
    if [ $FAILED_CHECKS -gt 0 ]; then
        echo -e "${RED}Failed: $FAILED_CHECKS${NC}"
    fi
    
    SUCCESS_RATE=$((PASSED_CHECKS * 100 / TOTAL_CHECKS))
    echo "Success rate: $SUCCESS_RATE%"
    
    echo
    if [ $FAILED_CHECKS -eq 0 ]; then
        echo -e "${GREEN}✓ All validation checks passed!${NC}"
        echo "The BlueWave Insurance Platform is ready for use."
    else
        echo -e "${RED}✗ Some validation checks failed.${NC}"
        echo "Please review the failures above and remediate as needed."
    fi
    echo
    
    # Save validation report
    REPORT_FILE="validation-report-$(date +%Y%m%d-%H%M%S).txt"
    {
        echo "BlueWave Platform Validation Report"
        echo "Generated: $(date)"
        echo "Resource Group: $RESOURCE_GROUP"
        echo ""
        echo "Results:"
        echo "- Total Checks: $TOTAL_CHECKS"
        echo "- Passed: $PASSED_CHECKS"
        echo "- Failed: $FAILED_CHECKS"
        echo "- Success Rate: $SUCCESS_RATE%"
    } > "$REPORT_FILE"
    
    print_status "Validation report saved to: $REPORT_FILE"
}

# Main execution
main() {
    validate_resource_group
    validate_storage_account
    validate_event_hubs
    validate_key_vault
    validate_monitoring
    validate_databricks
    validate_network_security
    check_connectivity
    generate_summary
}

main