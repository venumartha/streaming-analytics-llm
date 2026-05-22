"""
LLM Analyzer
Uses Claude to analyze anomalies and generate natural language insights
"""

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from anthropic import Anthropic
import json
import os
import time
from datetime import datetime
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """Uses Claude to analyze telemetry anomalies"""
    
    def __init__(
        self,
        bootstrap_servers: list = ['localhost:9092'],
        anthropic_api_key: Optional[str] = None
    ):
        self.consumer = KafkaConsumer(
            'telemetry-anomalies',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='llm-analyzer',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        
        # Initialize Anthropic client
        api_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY must be set")
        
        self.client = Anthropic(api_key=api_key)
        
        # Metrics
        self.anomalies_analyzed = 0
        self.total_tokens = 0
        
        logger.info("LLM Analyzer initialized")
    
    def build_analysis_prompt(self, aggregation: Dict) -> str:
        """Build prompt for Claude to analyze the anomaly"""
        
        prompt = f"""You are analyzing medical device telemetry anomalies for clinical decision support.

**Device Information:**
- Device ID: {aggregation['device_id']}
- Time Window: {aggregation['window_start']} to {aggregation['window_end']}
- Total Events: {aggregation['event_count']}
- Anomaly Events: {aggregation['anomaly_count']} ({aggregation['anomaly_percentage']}% of window)
- Anomaly Types: {', '.join(aggregation['anomaly_types'])}

**Vital Signs During Window:**

Temperature:
- Min: {aggregation['temperature']['min']}°F
- Max: {aggregation['temperature']['max']}°F
- Average: {aggregation['temperature']['avg']}°F
- Std Dev: {aggregation['temperature']['std']}°F

Heart Rate:
- Min: {aggregation['heart_rate']['min']} bpm
- Max: {aggregation['heart_rate']['max']} bpm
- Average: {aggregation['heart_rate']['avg']} bpm
- Std Dev: {aggregation['heart_rate']['std']} bpm

Blood Pressure (Systolic/Diastolic):
- Min: {aggregation['blood_pressure']['systolic']['min']}/{aggregation['blood_pressure']['diastolic']['min']} mmHg
- Max: {aggregation['blood_pressure']['systolic']['max']}/{aggregation['blood_pressure']['diastolic']['max']} mmHg
- Average: {aggregation['blood_pressure']['systolic']['avg']}/{aggregation['blood_pressure']['diastolic']['avg']} mmHg

Oxygen Saturation:
- Min: {aggregation['oxygen_saturation']['min']}%
- Max: {aggregation['oxygen_saturation']['max']}%
- Average: {aggregation['oxygen_saturation']['avg']}%

**Task:**
Analyze this anomaly pattern and provide:
1. Severity assessment (Low/Medium/High/Critical)
2. Possible root causes (2-3 clinical or technical explanations)
3. Recommended immediate actions for clinical staff
4. Related metrics to monitor closely

**IMPORTANT:** Return ONLY valid JSON with no other text. Use this exact format:

{{
  "severity": "Low" | "Medium" | "High" | "Critical",
  "summary": "Brief one-sentence summary of the issue",
  "root_causes": [
    "First possible cause",
    "Second possible cause",
    "Third possible cause"
  ],
  "recommended_actions": [
    "First action to take",
    "Second action to take",
    "Third action to take"
  ],
  "monitor_metrics": [
    "First metric to watch",
    "Second metric to watch",
    "Third metric to watch"
  ],
  "clinical_notes": "Additional context or considerations for clinical team"
}}"""
        
        return prompt
    
    def analyze_anomaly(self, aggregation: Dict) -> Optional[Dict]:
        """
        Analyze anomaly using Claude
        Returns parsed analysis or None if failed
        """
        prompt = self.build_analysis_prompt(aggregation)
        
        try:
            start_time = time.time()
            
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                temperature=0,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Extract text from response
            response_text = message.content[0].text.strip()
            
            # Parse JSON (Claude should return valid JSON)
            analysis = json.loads(response_text)
            
            # Track token usage
            self.total_tokens += message.usage.input_tokens + message.usage.output_tokens
            
            logger.info(
                f"✅ Analysis complete for {aggregation['device_id']} "
                f"({latency_ms:.0f}ms, {message.usage.output_tokens} tokens)"
            )
            
            return analysis
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.error(f"Response was: {response_text[:200]}...")
            return None
        
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return None
    
    def emit_insight(self, aggregation: Dict, analysis: Dict):
        """Send analyzed insight to Kafka"""
        
        insight = {
            'device_id': aggregation['device_id'],
            'window_start': aggregation['window_start'],
            'window_end': aggregation['window_end'],
            'event_count': aggregation['event_count'],
            'anomaly_count': aggregation['anomaly_count'],
            'anomaly_percentage': aggregation['anomaly_percentage'],
            'anomaly_types': aggregation['anomaly_types'],
            
            # Vital signs summary
            'vitals_summary': {
                'temperature_avg': aggregation['temperature']['avg'],
                'heart_rate_avg': aggregation['heart_rate']['avg'],
                'bp_systolic_avg': aggregation['blood_pressure']['systolic']['avg'],
                'bp_diastolic_avg': aggregation['blood_pressure']['diastolic']['avg'],
                'oxygen_avg': aggregation['oxygen_saturation']['avg']
            },
            
            # LLM analysis
            'llm_analysis': analysis,
            
            # Metadata
            'analyzed_at': datetime.utcnow().isoformat(),
            'analyzer_version': '1.0'
        }
        
        try:
            self.producer.send('telemetry-insights', value=insight)
            
            logger.info(
                f"📤 Insight sent: {insight['device_id']} - "
                f"Severity: {analysis['severity']}, "
                f"Summary: {analysis['summary']}"
            )
        
        except KafkaError as e:
            logger.error(f"Failed to send insight: {e}")
    
    def run(self):
        """Main analysis loop"""
        logger.info("LLM Analyzer starting...")
        logger.info("Listening for anomalies...")
        
        try:
            for message in self.consumer:
                aggregation = message.value
                
                logger.info(
                    f"\n🔍 Analyzing anomaly: {aggregation['device_id']} - "
                    f"{aggregation['anomaly_count']} events, "
                    f"types: {', '.join(aggregation['anomaly_types'])}"
                )
                
                # Analyze with Claude
                analysis = self.analyze_anomaly(aggregation)
                
                if analysis:
                    # Display key findings
                    logger.warning(f"   Severity: {analysis['severity']}")
                    logger.info(f"   Summary: {analysis['summary']}")
                    logger.info(f"   Top cause: {analysis['root_causes'][0]}")
                    logger.info(f"   Action: {analysis['recommended_actions'][0]}")
                    
                    # Send to insights topic
                    self.emit_insight(aggregation, analysis)
                    
                    self.anomalies_analyzed += 1
                else:
                    logger.error("   Analysis failed, skipping...")
                
                # Stats every 10 anomalies
                if self.anomalies_analyzed > 0 and self.anomalies_analyzed % 10 == 0:
                    logger.info(
                        f"\n📊 Stats: {self.anomalies_analyzed} anomalies analyzed, "
                        f"{self.total_tokens} total tokens used"
                    )
        
        except KeyboardInterrupt:
            logger.info("\n\nShutting down LLM analyzer...")
            self.consumer.close()
            self.producer.close()
            
            logger.info(
                f"Final stats: {self.anomalies_analyzed} anomalies analyzed, "
                f"{self.total_tokens} tokens used"
            )


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='LLM-powered anomaly analyzer')
    parser.add_argument('--kafka', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--api-key', help='Anthropic API key (or set ANTHROPIC_API_KEY env var)')
    
    args = parser.parse_args()
    
    analyzer = LLMAnalyzer(
        bootstrap_servers=[args.kafka],
        anthropic_api_key=args.api_key
    )
    
    analyzer.run()


if __name__ == "__main__":
    main()
