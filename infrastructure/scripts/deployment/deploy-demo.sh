#!/bin/bash

# ============================================================
# BlueWave Demo Environment Deployment Script
# Deploys cost-optimized demo environment (<$50/month)
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
ENVIRONMENT="demo"
TERRAFORM_DIR="../../terraform/environments/demo"
APPS_DIR="../../../applications"

echo "================================================"
echo -e "${BLUE}🚀 BlueWave Demo Environment Deployment${NC}"
echo "================================================"
echo -e "Environment: ${YELLOW}$ENVIRONMENT${NC}"
echo -e "Target Cost: ${GREEN}<\$50/month${NC}"
echo ""

# Check prerequisites
check_prerequisites() {
    echo -e "${BLUE}Checking prerequisites...${NC}"
    
    # Check Azure CLI
    if ! command -v az &> /dev/null; then
        echo -e "${RED}❌ Azure CLI not found${NC}"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        echo -e "${RED}❌ Terraform not found${NC}"
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python 3 not found${NC}"
        exit 1
    fi
    
    # Check Azure login
    if ! az account show &> /dev/null; then
        echo -e "${YELLOW}Not logged in to Azure. Running 'az login'...${NC}"
        az login
    fi
    
    echo -e "${GREEN}✅ All prerequisites met${NC}"
}

# Deploy infrastructure
deploy_infrastructure() {
    echo ""
    echo -e "${BLUE}📦 Deploying Infrastructure...${NC}"
    
    cd $TERRAFORM_DIR
    
    # Initialize Terraform
    echo "Initializing Terraform..."
    terraform init
    
    # Validate configuration
    echo "Validating configuration..."
    terraform validate
    
    # Plan deployment
    echo "Planning deployment..."
    terraform plan -out=demo.tfplan
    
    # Show cost estimate
    echo ""
    echo -e "${YELLOW}📊 Estimated Monthly Costs:${NC}"
    echo "  Storage: ~\$5-10"
    echo "  Event Hubs: ~\$15-20"
    echo "  Databricks: ~\$10-15 (with auto-termination)"
    echo "  Monitoring: ~\$5"
    echo "  Networking: ~\$5"
    echo -e "  ${GREEN}Total: ~\$40-50/month${NC}"
    echo ""
    
    # Confirm deployment
    read -p "Deploy infrastructure? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        terraform apply demo.tfplan
        echo -e "${GREEN}✅ Infrastructure deployed${NC}"
    else
        echo -e "${YELLOW}Deployment cancelled${NC}"
        exit 0
    fi
    
    # Save outputs
    terraform output -json > ../../../outputs.json
}

# Deploy data generator
deploy_data_generator() {
    echo ""
    echo -e "${BLUE}🔄 Setting up Data Generator...${NC}"
    
    cd $APPS_DIR/data-generator
    
    # Create virtual environment
    if [ ! -d "venv" ]; then
        python3 -m venv venv
    fi
    
    # Activate and install dependencies
    source venv/bin/activate
    pip install -r requirements.txt
    
    # Create demo config
    cat > config/demo.yaml << EOF
# Demo Configuration - Cost Optimized
event_hub:
  connection_string: \${EVENT_HUB_CONNECTION_STRING}
  name: claims-realtime

generator:
  rate_per_second: 10  # Low rate for demo
  batch_size: 100
  
data:
  num_customers: 1000
  num_policies: 5000
  fraud_percentage: 0.05
  
runtime:
  duration_hours: 1  # Run for 1 hour then stop
  checkpoint_interval: 100
EOF
    
    echo -e "${GREEN}✅ Data generator configured${NC}"
}

# Deploy Databricks notebooks
deploy_databricks() {
    echo ""
    echo -e "${BLUE}📓 Deploying Databricks Notebooks...${NC}"
    
    cd $APPS_DIR/stream-processor
    
    # Check if Databricks CLI is installed
    if command -v databricks &> /dev/null; then
        # Get workspace URL from Terraform output
        WORKSPACE_URL=$(cd $TERRAFORM_DIR && terraform output -raw databricks_workspace_url 2>/dev/null || echo "")
        
        if [ ! -z "$WORKSPACE_URL" ]; then
            echo "Uploading notebooks to $WORKSPACE_URL..."
            
            # Upload notebooks
            databricks workspace import_dir notebooks /demo-notebooks --overwrite
            
            echo -e "${GREEN}✅ Notebooks deployed${NC}"
        else
            echo -e "${YELLOW}⚠️  Databricks workspace not ready yet${NC}"
            echo "Manual upload required: $APPS_DIR/stream-processor/notebooks/"
        fi
    else
        echo -e "${YELLOW}Databricks CLI not installed${NC}"
        echo "Install with: pip install databricks-cli"
        echo "Manual upload required: $APPS_DIR/stream-processor/notebooks/"
    fi
}

# Setup monitoring
setup_monitoring() {
    echo ""
    echo -e "${BLUE}📊 Setting up Cost Monitoring...${NC}"
    
    # Create budget alert
    RESOURCE_GROUP="bluewave-demo-rg"
    
    az consumption budget create \
        --budget-name "DemoBudget" \
        --resource-group $RESOURCE_GROUP \
        --amount 50 \
        --category Cost \
        --time-grain Monthly \
        --start-date $(date +%Y-%m-01) \
        --end-date $(date -d '+1 year' +%Y-%m-01) \
        2>/dev/null && echo -e "${GREEN}✅ Budget alert created (\$50/month)${NC}" || echo -e "${YELLOW}Budget already exists${NC}"
    
    # Setup cleanup cron job
    echo ""
    echo -e "${YELLOW}📅 Setting up automated cleanup...${NC}"
    
    # Create cron entries
    CLEANUP_DIR="$(pwd)/../../scripts/cleanup"
    
    echo "Add these to your crontab (crontab -e):"
    echo ""
    echo "# Daily cleanup at 11 PM"
    echo "0 23 * * * $CLEANUP_DIR/daily-cleanup.sh"
    echo ""
    echo "# Weekly cleanup on Sunday at 2 AM"
    echo "0 2 * * 0 $CLEANUP_DIR/weekly-cleanup.sh"
    echo ""
}

# Final summary
show_summary() {
    echo ""
    echo "================================================"
    echo -e "${GREEN}✅ DEPLOYMENT COMPLETE${NC}"
    echo "================================================"
    echo ""
    echo -e "${BLUE}📋 Next Steps:${NC}"
    echo "1. Start data generator:"
    echo "   cd applications/data-generator"
    echo "   source venv/bin/activate"
    echo "   python run_generator.py --config config/demo.yaml"
    echo ""
    echo "2. Monitor costs:"
    echo "   infrastructure/scripts/cleanup/monitor-costs.sh"
    echo ""
    echo "3. Daily cleanup (run at end of day):"
    echo "   infrastructure/scripts/cleanup/daily-cleanup.sh"
    echo ""
    echo "4. Emergency shutdown (if needed):"
    echo "   infrastructure/scripts/cleanup/emergency-shutdown.sh"
    echo ""
    echo -e "${YELLOW}⚠️  Remember:${NC}"
    echo "- This is a DEMO environment"
    echo "- Costs are optimized for <\$50/month"
    echo "- Data is automatically cleaned after 7 days"
    echo "- Run daily cleanup to prevent cost creep"
    echo ""
    echo -e "${GREEN}Happy demo-ing! 🎉${NC}"
}

# Main execution
main() {
    check_prerequisites
    deploy_infrastructure
    deploy_data_generator
    deploy_databricks
    setup_monitoring
    show_summary
}

# Run if not sourced
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main
fi