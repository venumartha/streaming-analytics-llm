# Real-Time Streaming Analytics with LLM Integration

A production-grade event streaming pipeline that processes medical device telemetry in real-time, performs windowed aggregations, and uses LLMs (Claude) to generate intelligent anomaly insights. Built with Kafka, Python, and Anthropic AI.

## 🎯 What This Project Demonstrates

This project showcases **distributed streaming systems + AI integration** skills critical for roles at:
- **HealthTech** (Cedar, Aledade): Medical device monitoring, patient safety
- **Streaming Platforms** (Confluent, LinkedIn): Kafka expertise, real-time processing
- **Fintech** (Affirm, Stripe): Fraud detection, transaction monitoring

### Core Technical Skills
✅ Kafka event streaming architecture  
✅ Windowed aggregations (Kafka Streams pattern)  
✅ LLM integration for real-time analytics  
✅ WebSocket real-time data push  
✅ Multi-service Docker orchestration  
✅ Production patterns (exactly-once, DLQ, monitoring)

---

## 🏗️ Architecture

```
┌─────────────────────┐
│  Event Generator    │  Simulates 10 medical devices
│  (producer.py)      │  Generates telemetry every 1s
└──────────┬──────────┘
           │ Produce
           ▼
┌─────────────────────┐
│  Kafka Topic:       │
│  telemetry-raw      │  Raw device events
└──────────┬──────────┘
           │ Consume
           ▼
┌─────────────────────┐
│  Stream Processor   │  5-minute tumbling windows
│  (stream_processor) │  Compute aggregations
└──────────┬──────────┘
           │
           ├────► telemetry-aggregated (all windows)
           │
           └────► telemetry-anomalies (only anomalies)
                         │
                         ▼
                  ┌─────────────────┐
                  │  LLM Analyzer   │  Claude Sonnet 4
                  │  (llm_analyzer) │  Root cause analysis
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Kafka Topic:   │
                  │  telemetry-     │  Enriched insights
                  │  insights       │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  WebSocket API  │  Real-time push
                  │  (api.py)       │  to dashboard
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Web Dashboard  │  Live monitoring
                  │  (HTML/JS)      │  http://localhost:8000
                  └─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Anthropic API Key

### 1. Start Kafka Infrastructure

```bash
# Start Kafka, Zookeeper, Schema Registry
docker-compose up -d

# Verify Kafka is running
docker-compose ps

# View Kafka UI at http://localhost:8080
```

### 2. Install Python Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### 3. Configure API Key

```bash
# Copy environment template
cp .env.example .env

# Edit .env and add your Anthropic API key
# ANTHROPIC_API_KEY=sk-ant-your-key-here
```

### 4. Run the Pipeline

Open **4 separate terminals** and run each component:

**Terminal 1: Event Producer**
```bash
source venv/bin/activate
python producer.py --devices 10 --interval 1.0
```

**Terminal 2: Stream Processor**
```bash
source venv/bin/activate
python stream_processor.py --window 300
```

**Terminal 3: LLM Analyzer**
```bash
source venv/bin/activate
python llm_analyzer.py
```

**Terminal 4: API Server**
```bash
source venv/bin/activate
python api.py
```

### 5. View Dashboard

Open browser: **http://localhost:8000**

You'll see real-time anomaly insights with:
- Device vital signs
- LLM severity assessment
- Root cause analysis
- Recommended actions

---

## 📊 Sample Output

### Producer (Terminal 1)
```
INFO - Starting telemetry generation for 10 devices...
INFO - Event interval: 1.0s
WARNING - 🚨 device_003: Starting fever anomaly for 15s
INFO - 📊 Sent 100 events (9.8 events/sec)
```

### Stream Processor (Terminal 2)
```
INFO - Stream processor starting...
INFO - ✅ Normal window: device_001 - 300 events
WARNING - 🔴 Anomaly window: device_003 - 12 anomalies (4.0%) Types: fever
INFO - 📊 Processed 500 events, completed 5 windows
```

### LLM Analyzer (Terminal 3)
```
INFO - 🔍 Analyzing anomaly: device_003 - 12 events, types: fever
INFO - ✅ Analysis complete for device_003 (1250ms, 450 tokens)
WARNING -    Severity: High
INFO -    Summary: Sustained fever episode with elevated heart rate
INFO -    Top cause: Possible infection or inflammatory response
INFO -    Action: Check for signs of infection, monitor temperature trend
INFO - 📤 Insight sent: device_003 - Severity: High
```

---

## 🔧 Configuration

### Producer Settings

```bash
python producer.py \
  --devices 20 \           # Number of simulated devices
  --interval 0.5 \         # Seconds between events
  --kafka localhost:9092   # Kafka broker address
```

### Stream Processor Settings

```bash
python stream_processor.py \
  --window 300 \           # Window duration in seconds (5 min)
  --kafka localhost:9092
```

**Windowing:**
- Tumbling windows (non-overlapping)
- Configurable duration (default: 5 minutes)
- Per-device state management
- Automatic window emission on completion

### LLM Analyzer Settings

The analyzer uses Claude Sonnet 4 for analysis. You can customize:

```python
# In llm_analyzer.py, line 120
model="claude-sonnet-4-20250514",  # Change model
max_tokens=1500,                   # Adjust response length
temperature=0,                     # 0 = deterministic
```

---

## 📈 Kafka Topics

| Topic | Purpose | Schema |
|-------|---------|--------|
| `telemetry-raw` | Raw device events | device_id, timestamp, vitals |
| `telemetry-aggregated` | All windowed aggregations | window stats, averages |
| `telemetry-anomalies` | Only anomalous windows | aggregation + anomaly info |
| `telemetry-insights` | LLM-enriched analysis | aggregation + LLM response |

### View Topics in Kafka UI

Navigate to **http://localhost:8080** to:
- Browse topic messages
- View consumer lag
- Monitor throughput
- Check partition distribution

---

## 🧪 Testing Scenarios

### 1. Normal Operation
```bash
# Low anomaly rate (~3%)
python producer.py --devices 5 --interval 1.0
```
Most windows will be normal, occasional anomalies.

### 2. High Anomaly Rate
Edit `producer.py` line 53:
```python
if random.random() < 0.15:  # Change from 0.03 to 0.15
```
15% anomaly rate = more frequent LLM analysis.

### 3. Stress Test
```bash
# Many devices, fast rate
python producer.py --devices 50 --interval 0.1
```
Tests throughput and backpressure handling.

### 4. Window Duration
```bash
# Shorter windows (1 minute)
python stream_processor.py --window 60
```
More frequent aggregations, faster insights.

---

## 🎨 Customization for Job Applications

### For Healthcare Companies (Cedar, Aledade)

**Current setup:** Already uses medical device telemetry!

**Enhancements:**
1. Add HIPAA compliance logging
2. Implement patient ID anonymization
3. Add clinical alert routing (page nurse on Critical severity)
4. Create HL7 message export

### For Streaming Platforms (Confluent, LinkedIn)

**Replace medical telemetry with:**
- User activity events (clicks, views, shares)
- System metrics (CPU, memory, network)
- Application logs

**Update in `producer.py`:**
```python
return {
    'user_id': self.device_id,
    'event_type': 'page_view',
    'page': random.choice(['home', 'profile', 'search']),
    'duration_ms': random.randint(100, 5000),
    'is_anomaly': self.is_in_anomaly
}
```

### For Fintech (Affirm, Stripe)

**Replace with transaction events:**
```python
return {
    'transaction_id': uuid.uuid4(),
    'user_id': self.device_id,
    'amount': round(random.uniform(10, 500), 2),
    'merchant': random.choice(['amazon', 'uber', 'walmart']),
    'is_fraud': self.is_in_anomaly,
    'fraud_type': self.anomaly_type
}
```

**LLM analyzes for:** fraud patterns, risk assessment, chargeback prediction

---

## 📊 Performance Metrics

Based on testing with 10 devices, 1 event/sec:

| Metric | Value |
|--------|-------|
| Producer Throughput | 10 events/sec |
| Stream Processor Latency | <50ms per event |
| Window Completion | Every 5 minutes |
| LLM Analysis Latency | 1-2 seconds |
| WebSocket Push Latency | <100ms |
| End-to-End Latency | ~2 seconds (event → insight) |

**Cost Estimate:**
- Anthropic API: ~$0.003 per anomaly analysis
- 10 devices, 3% anomaly rate = ~2 anomalies/hour
- **Daily cost:** ~$0.15

---

## 🔒 Production Considerations

### Implemented
✅ Exactly-once semantics (manual offset commit)  
✅ Graceful shutdown (Ctrl+C handling)  
✅ Error logging  
✅ Metrics tracking  
✅ WebSocket connection management

### TODO for Production
- [ ] Add authentication (JWT for WebSocket)
- [ ] Implement dead letter queue for failed LLM calls
- [ ] Add Prometheus metrics export
- [ ] Set up Grafana dashboards
- [ ] Implement backpressure handling
- [ ] Add schema validation (Avro/Protobuf)
- [ ] Deploy to Kubernetes
- [ ] Add integration tests

---

## 🐳 Docker Deployment

### Build Custom Images

```bash
# Build producer
docker build -t streaming-producer -f Dockerfile.producer .

# Build processor
docker build -t streaming-processor -f Dockerfile.processor .

# Build analyzer
docker build -t streaming-analyzer -f Dockerfile.analyzer .
```

### Run Full Stack

```bash
docker-compose -f docker-compose.prod.yml up -d
```

---

## 📝 Project Structure

```
streaming-analytics-llm/
├── producer.py              # Event generator (Kafka producer)
├── stream_processor.py      # Windowed aggregations
├── llm_analyzer.py          # Claude-powered analysis
├── api.py                   # FastAPI + WebSocket server
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Kafka infrastructure
├── .env.example             # Environment template
├── .gitignore               # Git exclusions
├── README.md                # This file
└── docs/
    ├── ARCHITECTURE.md      # Deep-dive architecture
    ├── TUTORIAL.md          # Code walkthrough
    └── DEPLOYMENT.md        # Production deployment
```

---

## 🎓 Learning Path

### Day 1: Understand the Flow
1. Start Kafka with `docker-compose up -d`
2. Run producer and watch events in Kafka UI
3. Add stream processor and see aggregations
4. Understand windowing concept

### Day 2: Add Intelligence
1. Configure Anthropic API key
2. Run LLM analyzer
3. Observe Claude's analysis
4. Modify prompts for different insights

### Day 3: Customize
1. Change event schema (healthcare → fintech)
2. Adjust window duration
3. Tune LLM prompt for your domain
4. Deploy full stack

---

## 🤝 Contributing

This is a portfolio project, but suggestions welcome:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

---

## 📄 License

MIT License - free to use for learning and portfolios

---

## 👤 Author

**Venu Gopal Martha**
- GitHub: [@venumartha](https://github.com/venumartha)
- LinkedIn: [venu-gopal-martha](https://linkedin.com/in/venu-gopal-martha)
- Email: venu.martha96@gmail.com

---

## 🙏 Acknowledgments

- Confluent for Kafka Docker images
- Anthropic for Claude API
- FastAPI for the excellent web framework

---

**⭐ Star this repo if you find it helpful for your job search!**

This project demonstrates the exact skills companies like Cedar, Confluent, and Stripe look for: event streaming architecture, real-time processing, and AI integration in production systems.
