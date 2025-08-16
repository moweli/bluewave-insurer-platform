#!/bin/bash

# BlueWave Insurance Platform - Infrastructure Setup Script
# This script deploys the complete Azure infrastructure for the platform

set -e

echo "=========================================="
echo "BlueWave Insurance Platform Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Azure CLI
    if ! command -v az &> /dev/null; then
        print_error "Azure CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed. Please install it first."
        exit 1
    fi
    
    # Check Azure login
    if ! az account show &> /dev/null; then
        print_error "Not logged into Azure. Please run 'az login' first."
        exit 1
    fi
    
    print_status "Prerequisites check completed."
}

# Select Azure subscription
select_subscription() {
    print_status "Current Azure subscription:"
    az account show --query "[name, id]" -o tsv
    
    read -p "Do you want to use this subscription? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_status "Available subscriptions:"
        az account list --query "[].{Name:name, ID:id}" -o table
        read -p "Enter subscription ID: " subscription_id
        az account set --subscription "$subscription_id"
        print_status "Switched to subscription: $(az account show --query name -o tsv)"
    fi
}

# Initialize Terraform
init_terraform() {
    print_status "Initializing Terraform..."
    cd terraform
    
    # Clean any existing state
    if [ -d ".terraform" ]; then
        print_warning "Existing .terraform directory found. Cleaning..."
        rm -rf .terraform
    fi
    
    terraform init
    
    # Validate configuration
    print_status "Validating Terraform configuration..."
    terraform validate
    
    cd ..
}

# Deploy infrastructure
deploy_infrastructure() {
    print_status "Planning Terraform deployment..."
    cd terraform
    
    # Create terraform.tfvars if it doesn't exist
    if [ ! -f "terraform.tfvars" ]; then
        print_status "Creating terraform.tfvars from example..."
        cp terraform.tfvars.example terraform.tfvars
        print_warning "Please review terraform.tfvars and update values as needed."
        read -p "Press enter to continue after reviewing the file..."
    fi
    
    # Plan deployment
    terraform plan -out=tfplan
    
    # Ask for confirmation
    read -p "Do you want to apply this plan? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Applying Terraform configuration..."
        terraform apply tfplan
        
        # Save outputs
        print_status "Saving deployment outputs..."
        terraform output -json > deployment-outputs.json
        print_status "Outputs saved to deployment-outputs.json"
    else
        print_warning "Deployment cancelled."
        rm tfplan
        exit 0
    fi
    
    cd ..
}

# Configure permissions
configure_permissions() {
    print_status "Configuring permissions..."
    ./infra-scripts/configure-permissions.sh
}

# Validate deployment
validate_deployment() {
    print_status "Validating deployment..."
    ./infra-scripts/validate-deployment.sh
}

# Main execution
main() {
    echo
    print_status "Starting BlueWave Insurance Platform setup..."
    echo
    
    check_prerequisites
    select_subscription
    init_terraform
    deploy_infrastructure
    configure_permissions
    validate_deployment
    
    echo
    print_status "=========================================="
    print_status "Deployment completed successfully!"
    print_status "=========================================="
    echo
    print_status "Next steps:"
    echo "  1. Review the deployment outputs in terraform/deployment-outputs.json"
    echo "  2. Configure data pipelines in Azure Data Factory"
    echo "  3. Set up Databricks workspaces and notebooks"
    echo "  4. Configure monitoring alerts in Azure Monitor"
    echo
}

# Run main function
main