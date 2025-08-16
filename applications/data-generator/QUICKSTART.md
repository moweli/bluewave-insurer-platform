# BlueWave Insurance Data Generator - Quick Start Guide

## Installation

1. **Clone and navigate to the project:**
```bash
cd insurance-data-generator
```

2. **Create and activate virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Running the Demos

### Option 1: Non-Interactive Demo (Recommended for first run)
```bash
python demo/run_demo.py
```
This will showcase all fraud detection scenarios automatically.

### Option 2: Interactive Demo
```bash
python demo/demo_fraud_scenarios.py
```
Choose from:
- Early Claim Fraud
- Inflated Claim Fraud  
- Fraud Ring Detection
- Repeat Offender Pattern
- Opportunistic Fraud
- Real-time Streaming Simulation

## Key Features Demonstrated

### 🎯 Fraud Detection Scenarios
- **Early Claims**: Claims filed suspiciously soon after policy inception
- **Inflated Claims**: Claims approaching policy maximums
- **Fraud Rings**: Networks of related fraudulent claims
- **Repeat Offenders**: Multiple suspicious claims from same policyholder
- **Opportunistic Fraud**: Legitimate incidents with inflated damages

### 📍 UK-Specific Features
- Realistic UK postcodes and addresses
- Geographic risk profiling by postcode area
- UK insurance companies and brokers
- UK phone number formats

### 📊 Fraud Scoring
- Multi-factor analysis (timing, amount, frequency, geography)
- Network analysis for fraud ring detection
- Confidence scoring
- Risk level classification (MINIMAL to CRITICAL)
- Recommended actions for each risk level

## Testing

Run the test suite to verify everything is working:
```bash
pytest tests/ -v
```

## Configuration

To use with Azure Event Hubs:
1. Copy `.env.template` to `.env`
2. Add your Azure credentials:
```bash
EVENTHUB_CONNECTION_STRING=<your-connection-string>
EVENTHUB_NAME_CLAIMS=claims-realtime
EVENTHUB_NAME_POLICIES=policies-stream
```

## Sample Output

```
📋 Scenario: Claim filed suspiciously soon after policy inception
⚠️  Claim filed 20 days after policy inception
💰 Claim amount: £396,768.91
🎯 Fraud score: 70.00%
📍 Location: Belfast, BA61 6DY
```

## Next Steps

1. **Stream to Event Hubs**: Configure Azure credentials and run `python src/streaming_service.py`
2. **Monitor in Real-time**: Launch dashboard with `streamlit run dashboards/monitoring_dashboard.py`
3. **Customize Fraud Rates**: Edit `FRAUD_INJECTION_RATE` in `.env`
4. **Extend Scenarios**: Add custom fraud patterns in `src/fraud/fraud_scenarios.py`

## Troubleshooting

- **ModuleNotFoundError**: Ensure virtual environment is activated
- **Azure Connection Errors**: Verify Event Hub connection string
- **High Memory Usage**: Reduce batch size in configuration

## Support

For issues or questions, see the main [README.md](README.md) or contact the BlueWave Platform Team.