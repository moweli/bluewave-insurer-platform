# Insurance Data Generator for BlueWave Platform

A comprehensive Python-based synthetic data generator and streaming service for the BlueWave Insurance Platform. This tool generates realistic UK insurance data including policies, claims, and fraud scenarios, then streams them to Azure Event Hubs for real-time processing.

## Features

### 🎯 Core Capabilities
- **Synthetic Data Generation**: Generate realistic insurance policies, claims, and policyholders
- **UK-Specific Data**: UK postcodes, addresses, phone numbers, and geographic risk profiling
- **Fraud Pattern Injection**: Sophisticated fraud scenarios with configurable detection patterns
- **Real-time Streaming**: Stream data to Azure Event Hubs with batching and retry logic
- **Monitoring Dashboard**: Streamlit-based real-time monitoring interface
- **Comprehensive Testing**: Full test coverage for all components

### 🔍 Fraud Detection Scenarios
1. **Early Claim Fraud**: Claims filed suspiciously soon after policy inception
2. **Inflated Claims**: Claims with amounts close to policy maximums
3. **Fraud Rings**: Networks of related fraudulent claims
4. **Repeat Offenders**: Multiple suspicious claims from same policyholder
5. **Phantom Claims**: Claims for incidents that likely never occurred
6. **Opportunistic Fraud**: Legitimate incidents with inflated damages

### 📍 UK Postcode Risk Profiling
- Risk scoring based on postcode areas
- Crime rate integration
- Flood risk assessment
- Population density factors
- Income level correlation
- Geographic clustering detection

## Installation

### Prerequisites
- Python 3.9+
- Azure Event Hubs namespace and connection string
- Azure Storage Account (for checkpointing)

### Setup

1. Clone the repository:
```bash
cd insurance-data-generator
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.template .env
# Edit .env with your Azure credentials
```

## Usage

### 1. Run Fraud Detection Demo
Interactive demonstration of all fraud scenarios:
```bash
python demo/demo_fraud_scenarios.py
```

### 2. Start Streaming Service
Stream synthetic data to Event Hubs:
```bash
python src/streaming_service.py
```

### 3. Launch Monitoring Dashboard
Real-time monitoring interface:
```bash
streamlit run dashboards/monitoring_dashboard.py
```

### 4. Run Tests
Execute comprehensive test suite:
```bash
pytest tests/ -v
```

## Project Structure

```
insurance-data-generator/
├── src/
│   ├── models.py                 # Data models (Policy, Claim, etc.)
│   ├── streaming_service.py      # Main streaming service
│   ├── generators/
│   │   ├── base_generator.py     # Base data generation
│   │   ├── policy_generator.py   # Policy generation
│   │   └── claim_generator.py    # Claim generation with fraud
│   ├── fraud/
│   │   ├── fraud_detector.py     # Fraud detection engine
│   │   └── fraud_scenarios.py    # Fraud scenario generators
│   ├── streaming/
│   │   ├── producer.py           # Event Hub producer
│   │   └── consumer.py           # Event Hub consumer
│   └── utils/
│       └── uk_postcode_profiler.py  # UK postcode risk profiling
├── config/
│   └── settings.py               # Configuration management
├── dashboards/
│   └── monitoring_dashboard.py   # Streamlit dashboard
├── tests/
│   ├── test_generators.py       # Generator tests
│   ├── test_fraud_detection.py  # Fraud detection tests
│   └── test_uk_postcode.py      # Postcode profiling tests
├── demo/
│   └── demo_fraud_scenarios.py  # Interactive demos
├── requirements.txt              # Python dependencies
├── .env.template                 # Environment template
└── README.md                     # This file
```

## Configuration

### Environment Variables
Key settings in `.env`:

```bash
# Event Hub Configuration
EVENTHUB_CONNECTION_STRING=<your-connection-string>
EVENTHUB_NAME_CLAIMS=claims-realtime
EVENTHUB_NAME_POLICIES=policies-stream
EVENTHUB_NAME_AUDIT=audit-events

# Fraud Settings
FRAUD_INJECTION_RATE=0.05  # 5% fraud rate
SUSPICIOUS_PATTERN_RATE=0.02

# Streaming Settings
MAX_EVENTS_PER_SECOND=100
BATCH_SIZE=100
```

## Data Models

### Policy
- Policy types: Motor, Property, Liability, Professional Indemnity, Cyber, Marine, Aviation, D&O
- Risk profiles: Low, Medium, High, Very High
- Premium calculation based on risk factors

### Claim
- Claim types: Collision, Theft, Fire, Flood, Storm, Liability, Cyber Breach, etc.
- Status tracking: Submitted → Under Review → Investigation → Approved/Rejected → Paid
- Fraud scoring: 0.0 (legitimate) to 1.0 (definite fraud)

### Fraud Indicators
- Duplicate claims
- Suspicious timing
- Inflated amounts
- Inconsistent details
- Blacklisted entities
- Unusual frequency
- Network fraud rings
- Document forgery

## Streaming Architecture

### Event Hub Integration
- **Producer**: Async batching with automatic retry
- **Consumer**: Checkpointing support with Azure Blob Storage
- **Topics**:
  - `claims-realtime`: Real-time claim submissions
  - `policies-stream`: Policy creation and updates
  - `audit-events`: Compliance and audit trail

### Performance
- Configurable batch sizes (default: 100 events)
- Automatic flush intervals (default: 5 seconds)
- Retry logic with exponential backoff
- Connection pooling for efficiency

## Monitoring Dashboard

### Key Features
- Real-time metrics display
- Fraud alert notifications
- Geographic visualization (UK map)
- Claims volume time series
- Status distribution charts
- Recent claims table with fraud scores

### Metrics Tracked
- Total claims and policies
- Fraud detection rate
- Average fraud scores
- High-risk claim counts
- Geographic distribution
- Processing times

## Testing

### Test Coverage
- Unit tests for all generators
- Fraud detection algorithm tests
- UK postcode validation tests
- Integration tests for streaming
- Performance benchmarks

### Running Tests
```bash
# All tests
pytest

# Specific test file
pytest tests/test_fraud_detection.py

# With coverage
pytest --cov=src tests/
```

## Performance Considerations

### Optimization Tips
1. **Batching**: Adjust `BATCH_SIZE` based on throughput needs
2. **Partitioning**: Use partition keys for Event Hub scaling
3. **Caching**: Postcode profiles are cached in memory
4. **Async Operations**: All I/O operations are async
5. **Connection Pooling**: Reuse Event Hub connections

### Scalability
- Handles 100k+ events per day
- Horizontal scaling via Event Hub partitions
- Stateless design for container deployment
- Checkpoint support for fault tolerance

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Event Hub connection string
   - Check firewall/network settings
   - Ensure Event Hubs exist

2. **Performance Issues**
   - Increase batch size
   - Add more Event Hub partitions
   - Optimize fraud detection thresholds

3. **Memory Usage**
   - Limit in-memory claim history
   - Use streaming iterators
   - Enable garbage collection

## Future Enhancements

- [ ] ML-based fraud detection models
- [ ] Real-time anomaly detection
- [ ] Advanced geographic analytics
- [ ] Custom fraud rule engine
- [ ] Data quality monitoring
- [ ] Performance profiling tools
- [ ] Docker containerization
- [ ] Kubernetes deployment manifests

## License

Part of the BlueWave Insurance Platform - Internal Use Only

## Support

For issues or questions, contact the BlueWave Platform Team.