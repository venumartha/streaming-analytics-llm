"""
Stream Processor
Performs windowed aggregations and anomaly detection on telemetry events
"""

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from collections import defaultdict, deque
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Deque
import logging
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TumblingWindow:
    """5-minute tumbling window for aggregations"""
    
    def __init__(self, device_id: str, window_duration_seconds: int = 300):
        self.device_id = device_id
        self.window_duration = timedelta(seconds=window_duration_seconds)
        self.events: Deque[Dict] = deque()
        self.start_time: datetime = None
        self.end_time: datetime = None
    
    def add_event(self, event: Dict) -> bool:
        """
        Add event to window
        Returns True if window is complete
        """
        event_time = datetime.fromisoformat(event['timestamp'])
        
        # Initialize window
        if self.start_time is None:
            self.start_time = event_time
            self.end_time = self.start_time + self.window_duration
        
        # Check if event belongs to this window
        if event_time >= self.end_time:
            return True  # Window is complete
        
        self.events.append(event)
        return False
    
    def compute_aggregation(self) -> Dict:
        """Compute aggregations for the window"""
        if not self.events:
            return None
        
        events_list = list(self.events)
        
        # Extract metrics
        temps = [e['temperature'] for e in events_list]
        hrs = [e['heart_rate'] for e in events_list]
        bp_sys = [e['blood_pressure_systolic'] for e in events_list]
        bp_dia = [e['blood_pressure_diastolic'] for e in events_list]
        o2s = [e['oxygen_saturation'] for e in events_list]
        
        # Count anomalies
        anomaly_count = sum(1 for e in events_list if e.get('is_anomaly', False))
        anomaly_types = [e['anomaly_type'] for e in events_list if e.get('anomaly_type')]
        
        # Compute stats
        aggregation = {
            'device_id': self.device_id,
            'window_start': self.start_time.isoformat(),
            'window_end': self.end_time.isoformat(),
            'event_count': len(events_list),
            'temperature': {
                'min': round(min(temps), 1),
                'max': round(max(temps), 1),
                'avg': round(statistics.mean(temps), 1),
                'std': round(statistics.stdev(temps), 2) if len(temps) > 1 else 0
            },
            'heart_rate': {
                'min': int(min(hrs)),
                'max': int(max(hrs)),
                'avg': int(statistics.mean(hrs)),
                'std': round(statistics.stdev(hrs), 2) if len(hrs) > 1 else 0
            },
            'blood_pressure': {
                'systolic': {
                    'min': int(min(bp_sys)),
                    'max': int(max(bp_sys)),
                    'avg': int(statistics.mean(bp_sys))
                },
                'diastolic': {
                    'min': int(min(bp_dia)),
                    'max': int(max(bp_dia)),
                    'avg': int(statistics.mean(bp_dia))
                }
            },
            'oxygen_saturation': {
                'min': int(min(o2s)),
                'max': int(max(o2s)),
                'avg': int(statistics.mean(o2s))
            },
            'anomaly_count': anomaly_count,
            'anomaly_types': list(set(anomaly_types)) if anomaly_types else [],
            'anomaly_percentage': round((anomaly_count / len(events_list)) * 100, 1)
        }
        
        return aggregation
    
    def reset(self, first_event: Dict = None):
        """Reset window for next period"""
        self.events.clear()
        
        if first_event:
            self.start_time = datetime.fromisoformat(first_event['timestamp'])
            self.end_time = self.start_time + self.window_duration
            self.events.append(first_event)
        else:
            self.start_time = None
            self.end_time = None


class StreamProcessor:
    """Kafka Streams-like processor for windowed aggregations"""
    
    def __init__(
        self,
        bootstrap_servers: List[str] = ['localhost:9092'],
        window_duration: int = 300
    ):
        self.consumer = KafkaConsumer(
            'telemetry-raw',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='stream-processor',
            auto_offset_reset='latest',
            enable_auto_commit=False  # Manual commit for exactly-once
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Window state per device
        self.windows: Dict[str, TumblingWindow] = {}
        self.window_duration = window_duration
        
        # Metrics
        self.events_processed = 0
        self.windows_completed = 0
        
        logger.info(f"Stream processor initialized (window: {window_duration}s)")
    
    def process_event(self, event: Dict):
        """Process a single event"""
        device_id = event['device_id']
        
        # Get or create window for device
        if device_id not in self.windows:
            self.windows[device_id] = TumblingWindow(device_id, self.window_duration)
        
        window = self.windows[device_id]
        
        # Add event to window
        window_complete = window.add_event(event)
        
        if window_complete:
            # Compute and emit aggregation
            self.emit_aggregation(window)
            
            # Reset window with current event
            window.reset(event)
            
            self.windows_completed += 1
        
        self.events_processed += 1
    
    def emit_aggregation(self, window: TumblingWindow):
        """Emit windowed aggregation to Kafka"""
        aggregation = window.compute_aggregation()
        
        if aggregation is None:
            return
        
        try:
            # Send to aggregated topic
            self.producer.send('telemetry-aggregated', value=aggregation)
            
            # If anomalies detected, also send to anomaly topic
            if aggregation['anomaly_count'] > 0:
                self.producer.send('telemetry-anomalies', value=aggregation)
                
                logger.warning(
                    f"🔴 Anomaly window: {aggregation['device_id']} - "
                    f"{aggregation['anomaly_count']} anomalies "
                    f"({aggregation['anomaly_percentage']}%) "
                    f"Types: {', '.join(aggregation['anomaly_types'])}"
                )
            else:
                logger.info(
                    f"✅ Normal window: {aggregation['device_id']} - "
                    f"{aggregation['event_count']} events"
                )
        
        except KafkaError as e:
            logger.error(f"Failed to emit aggregation: {e}")
    
    def run(self):
        """Main processing loop"""
        logger.info("Stream processor starting...")
        logger.info("Listening for events...")
        
        try:
            batch_size = 0
            last_commit_time = time.time()
            
            for message in self.consumer:
                event = message.value
                self.process_event(event)
                
                batch_size += 1
                
                # Commit offsets every 100 events or 10 seconds
                if batch_size >= 100 or (time.time() - last_commit_time) > 10:
                    self.consumer.commit()
                    batch_size = 0
                    last_commit_time = time.time()
                
                # Stats every 500 events
                if self.events_processed % 500 == 0:
                    logger.info(
                        f"📊 Processed {self.events_processed} events, "
                        f"completed {self.windows_completed} windows"
                    )
        
        except KeyboardInterrupt:
            logger.info("\nShutting down stream processor...")
            self.consumer.close()
            self.producer.close()
            
            logger.info(
                f"Final stats: {self.events_processed} events processed, "
                f"{self.windows_completed} windows completed"
            )


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream processor for telemetry')
    parser.add_argument('--window', type=int, default=300, help='Window duration in seconds')
    parser.add_argument('--kafka', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    processor = StreamProcessor(
        bootstrap_servers=[args.kafka],
        window_duration=args.window
    )
    
    processor.run()


if __name__ == "__main__":
    main()
