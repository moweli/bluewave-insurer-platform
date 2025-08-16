# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BlueWave Insurance Data Platform - An Azure-based streaming-first insurance data platform for the London insurance market. The platform focuses on real-time claims processing, fraud detection, and regulatory compliance using Azure cloud services.

## Technology Stack

- **Infrastructure as Code**: Terraform (v1.3.0+) with Azure Provider (v3.100+)
- **Cloud Platform**: Microsoft Azure (UK South region)
- **Core Services**:
  - Azure Data Lake Storage Gen2 (Medallion architecture: raw → bronze → silver → gold)
  - Azure Event Hubs (Real-time claims streaming)
  - Azure Databricks (Data processing and ML)
  - Azure Key Vault (Secrets management)
  - Azure Application Insights & Log Analytics (Monitoring)

## Common Development Commands

### Terraform Operations

```bash
# Initialize Terraform
cd terraform
terraform init

# Validate configuration
terraform validate

# Plan infrastructure changes
terraform plan

# Apply infrastructure changes (with approval)
terraform apply

# Destroy infrastructure (use with caution)
terraform destroy

# Format Terraform files
terraform fmt -recursive
```

### Azure CLI Commands

```bash
# Login to Azure
az login

# Set subscription
az account set --subscription <subscription-id>

# List resource groups
az group list --output table

# Check deployment status
az deployment group list --resource-group bluewave-rg --output table
```

## High-Level Architecture

### Infrastructure Layout
The platform follows a streaming-first architecture with batch fallback capabilities:

1. **Data Ingestion Layer**
   - Event Hubs receive real-time insurance claims (100k+ claims/day capacity)
   - Multiple consumer groups for parallel processing
   - 7-day retention for claims-realtime, 3-day for audit-events

2. **Storage Layer (Medallion Architecture)**
   - **Raw**: Unprocessed ingested data
   - **Bronze**: Cleaned and standardized data
   - **Silver**: Enriched and validated data
   - **Gold**: Business-ready aggregated data
   - Additional containers: features (ML), audit, archive

3. **Processing Layer**
   - Databricks workspaces for data transformation and ML
   - Unity Catalog for data governance
   - Auto-terminating clusters for cost optimization

4. **Security & Compliance**
   - All data encrypted at rest and in transit
   - Private endpoints for all PaaS services
   - RBAC with Azure AD integration
   - GDPR compliance with UK/EU data residency
   - 7-year claims retention, 10-year audit log retention

### Resource Naming Convention
Pattern: `{prefix}-{environment}-{location}-{service}-{instance}`
- Example: `bw-dev-uks-eventhubs-001`
- Prefix: `bw` (BlueWave)
- Environments: `dev`, `test`, `prod`
- Location: `uks` (UK South)

## Project Structure

```
bluewave-insurer-platform/
├── terraform/           # Infrastructure as Code
│   ├── main.tf         # Core resource definitions
│   ├── variables.tf    # Input variables
│   └── outputs.tf      # Output values
├── docs/               # Documentation
│   └── day1-requirements.md  # Detailed requirements
├── adf/                # Azure Data Factory (future)
├── databricks/         # Databricks notebooks (future)
├── notebooks/          # Jupyter notebooks (future)
├── data/               # Sample data (future)
├── great_expectations/ # Data quality (future)
├── validation/         # Validation scripts (future)
└── infra-scripts/      # Infrastructure scripts (future)
```

## Key Implementation Considerations

### When Adding New Infrastructure
1. Follow the existing Terraform module structure
2. Use consistent resource naming following the convention
3. Enable encryption and private endpoints for all data services
4. Add appropriate tags for cost tracking and compliance
5. Update outputs.tf with connection strings/URIs

### Security Requirements
- No public internet access to data services
- Use managed identities for service-to-service auth
- Store all secrets in Key Vault
- Enable audit logging for all data operations
- Implement network isolation with NSGs

### Cost Optimization
- Enable auto-pause for Databricks clusters (10-minute idle timeout)
- Configure storage lifecycle policies for cold/archive tiers
- Use Event Hub auto-inflate for traffic spikes
- Set up budget alerts at 50%, 80%, 100% thresholds

### Compliance & Regulatory
- Ensure data residency in UK/EU only
- Maintain immutable audit logs
- Implement proper data retention policies:
  - Claims data: 7 years
  - Audit logs: 10 years
  - Operational logs: 2 years

## Next Implementation Phases

Based on day1-requirements.md, the following components need implementation:

1. **Remaining Core Services**:
   - Azure Cognitive Services (PII detection)
   - Azure Container Registry
   - Azure Service Bus
   - Network Security Groups
   - Private Endpoints

2. **Security Enhancements**:
   - Customer-managed encryption keys
   - Just-in-time access configuration
   - Advanced Key Vault policies

3. **Monitoring Setup**:
   - Log Analytics workspace configuration
   - Application Insights integration
   - Cost and budget alerts

4. **Deployment Scripts**:
   - setup.sh (deployment automation)
   - configure-permissions.sh (RBAC setup)
   - validate-deployment.sh (health checks)

## Performance Targets
- Sub-200ms fraud scoring latency
- 100,000+ claims processing per day
- 32 partitions for claims-realtime Event Hub
- Premium tier Databricks for enterprise features