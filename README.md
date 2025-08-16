# BlueWave Insurance Data Platform

A streaming-first insurance data platform built on Microsoft Azure, designed for the London insurance market. This demo environment showcases real-time claims processing, fraud detection, and regulatory compliance capabilities.

## 🎯 Project Overview

BlueWave is a modern insurance data platform that demonstrates:
- Real-time claims processing (100k+ claims/day capacity)
- ML-powered fraud detection
- UK regulatory compliance (GDPR, FCA)
- Cost-optimized demo environment (<$50/month)

## 📁 Project Structure

```
bluewave-insurer-platform/
├── infrastructure/          # Infrastructure as Code and scripts
│   ├── terraform/          # Terraform configurations
│   │   ├── modules/        # Reusable Terraform modules
│   │   └── environments/   # Environment-specific configs
│   │       ├── demo/       # Demo environment (cost-optimized)
│   │       ├── dev/        # Development environment
│   │       └── prod/       # Production environment
│   └── scripts/            # Operational scripts
│       ├── cleanup/        # Cost control and cleanup scripts
│       ├── deployment/     # Deployment automation
│       └── monitoring/     # Monitoring and alerting
├── applications/           # Application code
│   ├── data-generator/     # Insurance data generator
│   └── stream-processor/   # Databricks streaming notebooks
└── docs/                   # Documentation
    ├── architecture/       # Architecture diagrams and decisions
    ├── runbooks/          # Operational runbooks
    ├── guides/            # User guides and tutorials
    └── day-plans/         # Implementation day plans

```

## 🚀 Quick Start

### Prerequisites
- Azure subscription
- Azure CLI installed and configured
- Terraform v1.3.0+
- Python 3.8+
- Databricks CLI (optional)

### Demo Environment Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/bluewave-insurer-platform
   cd bluewave-insurer-platform
   ```

2. **Deploy infrastructure (cost-optimized demo)**
   ```bash
   cd infrastructure/terraform
   terraform init
   terraform plan -var-file=environments/demo/terraform.tfvars
   terraform apply -var-file=environments/demo/terraform.tfvars
   ```

3. **Start data generator**
   ```bash
   cd applications/data-generator
   pip install -r requirements.txt
   python run_generator.py --config config/demo.yaml
   ```

4. **Deploy Databricks notebooks**
   ```bash
   cd applications/stream-processor
   ./deploy-notebooks.sh
   ```

## 💰 Cost Management

This demo environment is optimized to run at <$50/month:

### Daily Operations
- Run cleanup script: `infrastructure/scripts/cleanup/daily-cleanup.sh`
- Monitor costs: `infrastructure/scripts/cleanup/monitor-costs.sh`
- Emergency shutdown: `infrastructure/scripts/cleanup/emergency-shutdown.sh`

### Key Cost Optimizations
- Storage: LRS replication only
- Event Hubs: Basic tier, 1 TU
- Databricks: Standard tier with auto-termination
- Log Analytics: 30-day retention
- Auto-cleanup scripts for data and resources

## 📊 Architecture Overview

```
Event Sources → Event Hubs → Databricks Streaming → Delta Lake → Analytics
                                ↓
                          Fraud Detection ML
                                ↓
                          Real-time Alerts
```

### Core Components
- **Event Hubs**: Real-time event ingestion
- **Databricks**: Stream processing and ML
- **Delta Lake**: ACID transactions on data lake
- **Key Vault**: Secrets management
- **Log Analytics**: Monitoring and alerting

## 🔧 Development

### Running Tests
```bash
cd applications/data-generator
pytest tests/

cd applications/stream-processor
./run-tests.sh
```

### Local Development
See [docs/guides/local-development.md](docs/guides/local-development.md)

## 📝 Documentation

- [Architecture Overview](docs/architecture/README.md)
- [Day 1-5 Implementation Plans](docs/day-plans/)
- [Cost Optimization Guide](docs/guides/CLEANUP-PLAN.md)
- [Databricks Deployment](docs/guides/DATABRICKS-DEPLOYMENT.md)
- [Infrastructure Audit](docs/guides/INFRASTRUCTURE-AUDIT.md)

## 🤝 Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file.

## 🆘 Support

For issues or questions:
- Check [docs/runbooks/](docs/runbooks/) for operational guides
- Review [docs/guides/](docs/guides/) for tutorials
- Open an issue in GitHub

## ⚠️ Important Notes

1. **This is a DEMO environment** - not for production use
2. **Cost control is critical** - run daily cleanup scripts
3. **Data is ephemeral** - automatically cleaned after 7 days
4. **Resources auto-scale down** - to minimize costs

---

**Current Monthly Cost Target: ~$45-50** ✅