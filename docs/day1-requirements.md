# Day 1 - BlueWave Insurance Data Platform Foundation

## Project Overview

Create a complete Azure infrastructure foundation for a streaming-first insurance data platform targeting the London insurance market. This foundation must support real-time claims processing, fraud detection, and regulatory compliance.

## Business Context

- **Target Market**: London insurance sector (fraud detection, claims processing)
- **Architecture Pattern**: Streaming-first with batch fallback capabilities
- **Compliance Requirements**: GDPR, UK insurance regulations, enterprise security
- **Performance Targets**: 100k+ claims/day, sub-200ms fraud scoring
- **Demo Requirement**: Must be presentable to senior hiring managers

## Infrastructure Components Required

### 1. Core Data Services

#### Azure Data Lake Storage Gen2

- **Purpose**: Multi-layered data lake for insurance claims processing
- **Configuration**:
  - Hierarchical namespace enabled
  - Containers: `raw`, `bronze`, `silver`, `gold`, `features`, `audit`, `archive`
  - Lifecycle management policies for cost optimization
  - Encryption at rest with customer-managed keys
  - Soft delete enabled for compliance
  - Versioning enabled for audit trails

#### Azure Event Hubs Namespace

- **Purpose**: Real-time claims ingestion and event streaming
- **Configuration**:
  - Tier: Standard (for auto-inflate capabilities)
  - Event Hub: `claims-realtime` (32 partitions, 7-day retention)
  - Event Hub: `audit-events` (8 partitions, 3-day retention)
  - Consumer Groups: `streaming-processor`, `audit-consumer`, `backup-consumer`
  - Auto-inflate enabled with maximum throughput units

#### Azure Databricks Workspace

- **Purpose**: Data processing, ML training, and real-time analytics
- **Configuration**:
  - Tier: Premium (for RBAC and compliance features)
  - Auto-terminating clusters for cost optimization
  - Unity Catalog integration for data governance
  - Network isolation with private endpoints
  - Custom VNet integration

### 2. Security & Governance

#### Azure Key Vault

- **Purpose**: Centralized secrets, keys, and certificate management
- **Configuration**:
  - Advanced access policies enabled
  - Purge protection enabled
  - Soft delete enabled
  - Network access restrictions (private endpoint only)
  - RBAC-based access control

#### Azure Application Insights

- **Purpose**: Application performance monitoring and telemetry
- **Configuration**:
  - Workspace-based resource
  - Log retention: 730 days (regulatory requirement)
  - Live metrics stream enabled
  - Continuous export to storage for long-term retention

#### Azure Log Analytics Workspace

- **Purpose**: Centralized logging and security monitoring
- **Configuration**:
  - Data retention: 730 days minimum
  - Data export for compliance archiving
  - Security center integration
  - Custom log tables for insurance-specific events

### 3. Networking & Security

#### Network Security Groups

- **Purpose**: Network-level security for insurance compliance
- **Rules Required**:
  - Deny all inbound internet traffic
  - Allow specific ports for Azure services
  - Log all blocked traffic for audit
  - Geo-restriction for UK/EU only

#### Azure Private Endpoints

- **Purpose**: Secure private connectivity to PaaS services
- **Services to Secure**:
  - Storage Account
  - Key Vault
  - Databricks workspace
  - Event Hubs namespace

#### Azure Managed Identity

- **Purpose**: Secure service-to-service authentication
- **Configuration**:
  - System-assigned identities for Azure services
  - User-assigned identity for cross-service access
  - RBAC assignments following principle of least privilege

### 4. Supporting Services

#### Azure Container Registry

- **Purpose**: Custom Docker images for data processing
- **Configuration**:
  - Premium tier for geo-replication
  - Content trust enabled
  - Vulnerability scanning enabled

#### Azure Service Bus

- **Purpose**: Reliable messaging for orchestration
- **Configuration**:
  - Standard tier
  - Topics for publish/subscribe patterns
  - Dead letter queues for error handling

#### Azure Cognitive Services

- **Purpose**: AI services for PII detection and document processing
- **Configuration**:
  - Text Analytics for PII detection
  - Form Recognizer for document processing
  - Network restrictions applied

## Resource Naming Convention

### Pattern

`{prefix}-{environment}-{location}-{service}-{instance}`

### Examples

- Resource Group: `bw-dev-uks-platform-rg`
- Storage Account: `bwdevuksdatalake001`
- Event Hub Namespace: `bw-dev-uks-eventhubs-001`
- Databricks Workspace: `bw-dev-uks-databricks-001`
- Key Vault: `bw-dev-uks-keyvault-001`

### Abbreviations

- `bw` = BlueWave
- `dev/test/prod` = Environment
- `uks` = UK South region
- Sequential numbering for multiple instances

## Security Requirements

### Data Protection

- All data encrypted at rest and in transit
- Customer-managed encryption keys where supported
- Network isolation using private endpoints
- No public internet access to data services

### Access Control

- Azure AD integration for all services
- Role-based access control (RBAC)
- Principle of least privilege
- Just-in-time access for administrative tasks

### Compliance

- GDPR compliance with data residency in UK/EU
- Audit logging for all data access and modifications
- Data retention policies meeting insurance regulations
- Backup and disaster recovery procedures

### Monitoring

- Comprehensive logging to Log Analytics
- Real-time alerting for security events
- Performance monitoring and capacity planning
- Cost monitoring and budget alerts

## Terraform Configuration Structure

### File Organization

```
terraform/
├── main.tf              # Main resource definitions
├── variables.tf         # Input variables
├── outputs.tf          # Output values
├── security.tf         # Security-related resources
├── networking.tf       # Network configuration
├── monitoring.tf       # Monitoring and alerting
├── databricks.tf       # Databricks-specific resources
├── terraform.tfvars.example  # Example variable values
└── README.md           # Deployment instructions
```

### Required Variables

- `environment` (dev/test/prod)
- `location` (ukouth/uksouth2)
- `resource_group_name`
- `project_prefix` (default: "bw")
- `admin_group_object_id` (Azure AD group for admin access)
- `allowed_ip_ranges` (for network security)

### Required Outputs

- Storage account connection strings
- Event Hub connection strings
- Databricks workspace URL
- Key Vault URI
- Application Insights instrumentation key

## Deployment Scripts Required

### setup.sh

- Check Azure CLI authentication
- Validate prerequisites
- Initialize Terraform
- Apply configuration with approval
- Run post-deployment validation

### configure-permissions.sh

- Set up RBAC assignments
- Configure Key Vault access policies
- Create service principals if needed
- Set up Databricks workspace permissions

### validate-deployment.sh

- Test connectivity to all services
- Verify security configurations
- Check monitoring setup
- Generate deployment report

## Cost Optimization Features

### Immediate Optimizations

- Auto-pause for Databricks clusters (10 minutes idle)
- Storage lifecycle policies (cool/archive tiers)
- Event Hub auto-inflate to handle traffic spikes
- Reserved capacity where applicable

### Monitoring

- Budget alerts at 50%, 80%, 100% of monthly budget
- Cost allocation tags for chargeback
- Resource utilization monitoring
- Recommendations for right-sizing

## Insurance-Specific Configurations

### Data Residency

- Primary region: UK South
- Secondary region: UK West (for disaster recovery)
- No data replication outside UK/EU

### Retention Policies

- Claims data: 7 years (regulatory requirement)
- Audit logs: 10 years
- Operational logs: 2 years
- ML model artifacts: 3 years

### Compliance Features

- Immutable storage for audit logs
- Legal hold capabilities
- Data classification and labeling
- Automated data discovery for GDPR

## Validation Tests

### Connectivity Tests

```bash
# Event Hub connectivity
az eventhubs eventhub show --resource-group $RG_NAME --namespace-name $EVENTHUB_NAMESPACE --name claims-realtime

# Storage account access
az storage container list --account-name $STORAGE_ACCOUNT_NAME

# Key Vault access
az keyvault secret list --vault-name $KEYVAULT_NAME

# Databricks workspace
az databricks workspace show --resource-group $RG_NAME --name $DATABRICKS_WORKSPACE_NAME
```

### Security Tests

```bash
# Verify private endpoints
az network private-endpoint list --resource-group $RG_NAME

# Check network security groups
az network nsg list --resource-group $RG_NAME

# Validate RBAC assignments
az role assignment list --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RG_NAME
```

### Performance Tests

```bash
# Event Hub throughput test
# Storage account performance test
# Network latency test
```

## Demo Preparation

### Health Check Dashboard

- All services status (green/amber/red)
- Cost utilization vs budget
- Security posture score
- Performance metrics summary

### Sample Data Sets

- Synthetic insurance claims (1000 records)
- Customer data with GDPR considerations
- Policy data with coverage details
- Geographic reference data (UK postcodes)

### Demo Scripts

- "Infrastructure in 5 minutes" presentation
- Live deployment demonstration
- Security and compliance walkthrough
- Cost optimization showcase

## Troubleshooting Guide

### Common Issues

1. **Terraform state conflicts**: Use remote state backend
2. **Permission errors**: Verify Azure AD roles
3. **Network connectivity**: Check NSG rules and private endpoints
4. **Key Vault access**: Validate access policies and RBAC
5. **Cost overruns**: Review auto-scaling configurations

### Resolution Scripts

- Reset Terraform state
- Permission repair utility
- Network diagnostic tool
- Cost analysis and optimization

## Success Criteria

### Technical

- [ ] All infrastructure deployed successfully
- [ ] Security baseline implemented
- [ ] Monitoring and alerting configured
- [ ] Validation tests pass
- [ ] Cost optimization features enabled

### Demo Readiness

- [ ] Sample data available
- [ ] Health check dashboard functional
- [ ] Demo scripts prepared
- [ ] Troubleshooting guide available
- [ ] Architecture documentation complete

### Business Value

- [ ] Cost estimates documented
- [ ] Security compliance verified
- [ ] Scalability demonstrated
- [ ] Time-to-value minimized
- [ ] Regulatory requirements addressed

## Next Steps (Day 2 Preparation)

### Event Hub Configuration

- Consumer group setup for streaming processor
- Partition key strategy for claims data
- Dead letter queue configuration

### Databricks Preparation

- Cluster policies for cost control
- Unity Catalog workspace setup
- Mount point configuration for ADLS

### Security Hardening

- Network security group fine-tuning
- Key Vault secret organization
- RBAC role assignment validation

### Data Pipeline Foundation

- Schema registry setup
- Data format standardization
- Error handling strategy
