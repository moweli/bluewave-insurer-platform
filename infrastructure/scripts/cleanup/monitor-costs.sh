#!/bin/bash

# Azure Cost Monitoring Script for Demo Environment
# Helps track and control costs for the BlueWave demo

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
RESOURCE_GROUP="${RESOURCE_GROUP:-bwdemo-rg}"
SUBSCRIPTION="${SUBSCRIPTION_ID:-$(az account show --query id -o tsv)}"
BUDGET_LIMIT=50  # USD per month

echo "================================================"
echo "💰 Azure Cost Monitor - BlueWave Demo"
echo "================================================"

# Function to check Azure CLI
check_azure_cli() {
    if ! command -v az &> /dev/null; then
        echo -e "${RED}Azure CLI not found. Please install it first.${NC}"
        exit 1
    fi
    
    if ! az account show &> /dev/null; then
        echo -e "${YELLOW}Not logged in to Azure. Running 'az login'...${NC}"
        az login
    fi
}

# Function to get current month costs
get_current_costs() {
    echo -e "\n${BLUE}📊 Current Month Costs:${NC}"
    echo "------------------------"
    
    # Get costs for current month
    START_DATE=$(date +%Y-%m-01)
    END_DATE=$(date +%Y-%m-%d)
    
    COSTS=$(az consumption usage list \
        --start-date $START_DATE \
        --end-date $END_DATE \
        --query "[?resourceGroup=='$RESOURCE_GROUP'].{Service:meterCategory, Cost:pretaxCost}" \
        --output json 2>/dev/null || echo "[]")
    
    if [ "$COSTS" = "[]" ]; then
        echo -e "${YELLOW}No costs recorded yet for this month${NC}"
    else
        echo "$COSTS" | jq -r '.[] | "\(.Service): $\(.Cost)"' | sort | uniq
    fi
    
    # Get total
    TOTAL=$(echo "$COSTS" | jq '[.[].Cost] | add // 0')
    echo "------------------------"
    echo -e "${GREEN}TOTAL: \$$TOTAL${NC}"
    
    # Check against budget
    if (( $(echo "$TOTAL > $BUDGET_LIMIT" | bc -l) )); then
        echo -e "${RED}⚠️  WARNING: Over budget limit of \$$BUDGET_LIMIT!${NC}"
    else
        REMAINING=$(echo "$BUDGET_LIMIT - $TOTAL" | bc)
        echo -e "${GREEN}✓ Within budget. Remaining: \$$REMAINING${NC}"
    fi
}

# Function to list expensive resources
list_expensive_resources() {
    echo -e "\n${BLUE}💸 Most Expensive Resources:${NC}"
    echo "------------------------"
    
    az consumption usage list \
        --start-date $(date +%Y-%m-01) \
        --end-date $(date +%Y-%m-%d) \
        --query "[?resourceGroup=='$RESOURCE_GROUP'].{Name:instanceName, Service:meterCategory, Cost:pretaxCost}" \
        --output table \
        --top 10 2>/dev/null || echo "No data available"
}

# Function to check running resources
check_running_resources() {
    echo -e "\n${BLUE}🏃 Currently Running Resources:${NC}"
    echo "------------------------"
    
    # Check Databricks clusters
    echo -e "${YELLOW}Databricks Clusters:${NC}"
    az databricks workspace list \
        --resource-group $RESOURCE_GROUP \
        --query "[].{Name:name, State:provisioningState}" \
        --output table 2>/dev/null || echo "  No Databricks workspaces found"
    
    # Check Event Hubs
    echo -e "\n${YELLOW}Event Hub Namespaces:${NC}"
    az eventhubs namespace list \
        --resource-group $RESOURCE_GROUP \
        --query "[].{Name:name, SKU:sku.name, Capacity:sku.capacity}" \
        --output table 2>/dev/null || echo "  No Event Hubs found"
    
    # Check Storage Accounts
    echo -e "\n${YELLOW}Storage Accounts:${NC}"
    az storage account list \
        --resource-group $RESOURCE_GROUP \
        --query "[].{Name:name, SKU:sku.name, Kind:kind}" \
        --output table 2>/dev/null || echo "  No Storage accounts found"
}

# Function to stop expensive resources
stop_expensive_resources() {
    echo -e "\n${BLUE}🛑 Stopping Expensive Resources:${NC}"
    echo "------------------------"
    
    read -p "Do you want to stop all non-essential resources? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Note: Databricks clusters should be stopped via Databricks CLI
        echo -e "${YELLOW}To stop Databricks clusters, run:${NC}"
        echo "  databricks clusters list"
        echo "  databricks clusters delete --cluster-id <id>"
        
        # Note: Event Hubs can't be stopped, only deleted
        echo -e "${YELLOW}Event Hubs cannot be paused (only deleted)${NC}"
        
        echo -e "${GREEN}Manual intervention required for most resources${NC}"
    fi
}

# Function to set up cost alerts
setup_cost_alerts() {
    echo -e "\n${BLUE}🔔 Setting Up Cost Alerts:${NC}"
    echo "------------------------"
    
    # Check if budget exists
    BUDGET_NAME="DemoBudget"
    BUDGET_EXISTS=$(az consumption budget list \
        --resource-group $RESOURCE_GROUP \
        --query "[?name=='$BUDGET_NAME'].name" \
        --output tsv 2>/dev/null)
    
    if [ -z "$BUDGET_EXISTS" ]; then
        echo "Creating budget alert at \$$BUDGET_LIMIT..."
        
        cat > budget.json << EOF
{
    "category": "Cost",
    "amount": $BUDGET_LIMIT,
    "timeGrain": "Monthly",
    "timePeriod": {
        "startDate": "$(date +%Y-%m-01)T00:00:00Z",
        "endDate": "$(date -d '+1 year' +%Y-%m-01)T00:00:00Z"
    },
    "notifications": {
        "Actual_GreaterThan_80_Percent": {
            "enabled": true,
            "operator": "GreaterThan",
            "threshold": 80,
            "contactEmails": ["demo-admin@example.com"]
        }
    }
}
EOF
        
        az consumption budget create \
            --budget-name $BUDGET_NAME \
            --resource-group $RESOURCE_GROUP \
            --amount $BUDGET_LIMIT \
            --category Cost \
            --time-grain Monthly \
            2>/dev/null && echo -e "${GREEN}✓ Budget alert created${NC}" || echo -e "${YELLOW}Budget creation failed${NC}"
        
        rm -f budget.json
    else
        echo -e "${GREEN}✓ Budget alert already exists${NC}"
    fi
}

# Function to show cost-saving tips
show_cost_tips() {
    echo -e "\n${BLUE}💡 Cost-Saving Tips:${NC}"
    echo "------------------------"
    cat << EOF
1. DATABRICKS:
   - Use single-node clusters for demos
   - Enable auto-termination (10 mins)
   - Use spot instances
   - Stop clusters immediately after use

2. EVENT HUBS:
   - Use Basic tier (not Standard)
   - Minimize partitions (use 2)
   - Disable auto-inflate
   - Delete and recreate for demos

3. STORAGE:
   - Use LRS (not GRS/ZRS)
   - Set lifecycle policies
   - Delete old data regularly
   - Use hot tier only

4. MONITORING:
   - Reduce retention to 30 days
   - Disable detailed logging
   - Use sampling (10%)
   - Disable Application Insights for demos

5. GENERAL:
   - Delete resources after demo
   - Use tags for cost tracking
   - Set up auto-shutdown
   - Use Azure free tier benefits
EOF
}

# Function to generate cost report
generate_report() {
    echo -e "\n${BLUE}📄 Generating Cost Report:${NC}"
    echo "------------------------"
    
    REPORT_FILE="cost-report-$(date +%Y%m%d).txt"
    
    {
        echo "BlueWave Demo - Cost Report"
        echo "Generated: $(date)"
        echo "=========================="
        echo ""
        echo "Resource Group: $RESOURCE_GROUP"
        echo "Budget Limit: \$$BUDGET_LIMIT"
        echo ""
        
        echo "Current Month Costs:"
        az consumption usage list \
            --start-date $(date +%Y-%m-01) \
            --end-date $(date +%Y-%m-%d) \
            --query "[?resourceGroup=='$RESOURCE_GROUP'].{Service:meterCategory, Cost:pretaxCost}" \
            --output table 2>/dev/null
        
        echo ""
        echo "Daily Average: \$$(echo "$TOTAL / $(date +%d)" | bc -l | cut -c1-5)"
        echo "Projected Monthly: \$$(echo "$TOTAL * 30 / $(date +%d)" | bc -l | cut -c1-5)"
    } > $REPORT_FILE
    
    echo -e "${GREEN}Report saved to: $REPORT_FILE${NC}"
}

# Function to show daily costs
show_daily_costs() {
    echo -e "\n${BLUE}📅 Last 7 Days Costs:${NC}"
    echo "------------------------"
    
    for i in {0..6}; do
        DATE=$(date -d "-$i days" +%Y-%m-%d)
        COST=$(az consumption usage list \
            --start-date $DATE \
            --end-date $DATE \
            --query "[?resourceGroup=='$RESOURCE_GROUP'].pretaxCost" \
            --output tsv 2>/dev/null | awk '{s+=$1} END {printf "%.2f", s}')
        
        [ -z "$COST" ] && COST="0.00"
        echo "$DATE: \$$COST"
    done
}

# Main menu
show_menu() {
    echo -e "\n${BLUE}📊 Cost Management Menu:${NC}"
    echo "------------------------"
    echo "1) View current month costs"
    echo "2) List expensive resources"
    echo "3) Check running resources"
    echo "4) Show daily costs (last 7 days)"
    echo "5) Stop expensive resources"
    echo "6) Setup cost alerts"
    echo "7) Show cost-saving tips"
    echo "8) Generate cost report"
    echo "9) Exit"
    echo ""
    read -p "Select option: " choice
    
    case $choice in
        1) get_current_costs ;;
        2) list_expensive_resources ;;
        3) check_running_resources ;;
        4) show_daily_costs ;;
        5) stop_expensive_resources ;;
        6) setup_cost_alerts ;;
        7) show_cost_tips ;;
        8) generate_report ;;
        9) exit 0 ;;
        *) echo -e "${RED}Invalid option${NC}" ;;
    esac
}

# Main execution
main() {
    check_azure_cli
    
    echo -e "${GREEN}Connected to subscription: $(az account show --query name -o tsv)${NC}"
    echo -e "${GREEN}Monitoring resource group: $RESOURCE_GROUP${NC}"
    
    while true; do
        show_menu
    done
}

# Run if not sourced
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main
fi