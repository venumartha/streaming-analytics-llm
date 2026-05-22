"""
Telemetry Event Producer
Generates realistic medical device telemetry with occasional anomalies
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeviceSimulator:
    """Simulates a single medical device with realistic telemetry patterns"""
    
    def __init__(self, device_id: str):
        self.device_id = device_id
        
        # Base vital signs (normal ranges)
        self.base_temperature = 98.6
        self.base_heart_rate = 75
        self.base_bp_systolic = 120
        self.base_bp_diastolic = 80
        self.base_oxygen = 98
        
        # Current state
        self.temperature = self.base_temperature
        self.heart_rate = self.base_heart_rate
        self.bp_systolic = self.base_bp_systolic
        self.bp_diastolic = self.base_bp_diastolic
        self.oxygen = self.base_oxygen
        
        # Anomaly tracking
        self.is_in_anomaly = False
        self.anomaly_duration = 0
        self.anomaly_type = None
    
    def generate_event(self) -> Dict:
        """Generate a single telemetry event"""
        
        # 3% chance to start an anomaly
        if not self.is_in_anomaly and random.random() < 0.03:
            self.start_anomaly()
        
        # If in anomaly, continue for a duration
        if self.is_in_anomaly:
            self.anomaly_duration -= 1
            if self.anomaly_duration <= 0:
                self.end_anomaly()
        
        # Generate readings based on current state
        if self.is_in_anomaly:
            temp, hr, bp_sys, bp_dia, o2 = self.generate_anomaly_readings()
        else:
            temp, hr, bp_sys, bp_dia, o2 = self.generate_normal_readings()
        
        # Update state with smoothing
        self.temperature = 0.7 * self.temperature + 0.3 * temp
        self.heart_rate = 0.7 * self.heart_rate + 0.3 * hr
        self.bp_systolic = 0.7 * self.bp_systolic + 0.3 * bp_sys
        self.bp_diastolic = 0.7 * self.bp_diastolic + 0.3 * bp_dia
        self.oxygen = 0.7 * self.oxygen + 0.3 * o2
        
        return {
            'device_id': self.device_id,
            'timestamp': datetime.utcnow().isoformat(),
            'temperature': round(self.temperature, 1),
            'heart_rate': int(self.heart_rate),
            'blood_pressure_systolic': int(self.bp_systolic),
            'blood_pressure_diastolic': int(self.bp_diastolic),
            'oxygen_saturation': int(self.oxygen),
            'is_anomaly': self.is_in_anomaly,
            'anomaly_type': self.anomaly_type
        }
    
    def start_anomaly(self):
        """Start an anomaly episode"""
        self.is_in_anomaly = True
        self.anomaly_duration = random.randint(10, 30)  # 10-30 seconds
        
        # Pick anomaly type
        self.anomaly_type = random.choice([
            'fever',
            'tachycardia',
            'hypertension',
            'hypoxia',
            'combined'
        ])
        
        logger.warning(f"🚨 {self.device_id}: Starting {self.anomaly_type} anomaly for {self.anomaly_duration}s")
    
    def end_anomaly(self):
        """End anomaly episode"""
        logger.info(f"✅ {self.device_id}: Anomaly ended, returning to normal")
        self.is_in_anomaly = False
        self.anomaly_type = None
    
    def generate_normal_readings(self):
        """Generate normal readings with small variations"""
        temp = self.base_temperature + random.uniform(-0.3, 0.3)
        hr = self.base_heart_rate + random.uniform(-5, 5)
        bp_sys = self.base_bp_systolic + random.uniform(-5, 5)
        bp_dia = self.base_bp_diastolic + random.uniform(-3, 3)
        o2 = self.base_oxygen + random.uniform(-1, 1)
        
        return temp, hr, bp_sys, bp_dia, o2
    
    def generate_anomaly_readings(self):
        """Generate anomalous readings based on type"""
        
        if self.anomaly_type == 'fever':
            temp = self.base_temperature + random.uniform(2, 4)
            hr = self.base_heart_rate + random.uniform(10, 20)
            bp_sys = self.base_bp_systolic + random.uniform(5, 15)
            bp_dia = self.base_bp_diastolic + random.uniform(0, 5)
            o2 = self.base_oxygen + random.uniform(-2, 0)
        
        elif self.anomaly_type == 'tachycardia':
            temp = self.base_temperature + random.uniform(-0.5, 1)
            hr = self.base_heart_rate + random.uniform(30, 50)
            bp_sys = self.base_bp_systolic + random.uniform(10, 20)
            bp_dia = self.base_bp_diastolic + random.uniform(5, 10)
            o2 = self.base_oxygen + random.uniform(-1, 0)
        
        elif self.anomaly_type == 'hypertension':
            temp = self.base_temperature + random.uniform(-0.2, 0.5)
            hr = self.base_heart_rate + random.uniform(5, 15)
            bp_sys = self.base_bp_systolic + random.uniform(30, 50)
            bp_dia = self.base_bp_diastolic + random.uniform(15, 25)
            o2 = self.base_oxygen
        
        elif self.anomaly_type == 'hypoxia':
            temp = self.base_temperature + random.uniform(-0.5, 0)
            hr = self.base_heart_rate + random.uniform(15, 30)
            bp_sys = self.base_bp_systolic + random.uniform(10, 20)
            bp_dia = self.base_bp_diastolic + random.uniform(5, 10)
            o2 = self.base_oxygen - random.uniform(5, 15)
        
        else:  # combined
            temp = self.base_temperature + random.uniform(2, 4)
            hr = self.base_heart_rate + random.uniform(25, 40)
            bp_sys = self.base_bp_systolic + random.uniform(20, 40)
            bp_dia = self.base_bp_diastolic + random.uniform(10, 20)
            o2 = self.base_oxygen - random.uniform(3, 8)
        
        return temp, hr, bp_sys, bp_dia, o2


class TelemetryProducer:
    """Kafka producer for telemetry events"""
    
    def __init__(self, bootstrap_servers: List[str] = ['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1  # Ensure ordering
        )
        
        self.topic = 'telemetry-raw'
        logger.info(f"Producer initialized for topic: {self.topic}")
    
    def send_event(self, event: Dict):
        """Send event to Kafka"""
        try:
            future = self.producer.send(self.topic, value=event)
            # Block to get result (synchronous for demo purposes)
            record_metadata = future.get(timeout=10)
            
            if event['is_anomaly']:
                logger.warning(
                    f"⚠️  Anomaly sent: {event['device_id']} - "
                    f"{event['anomaly_type']} (partition {record_metadata.partition})"
                )
        
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


def main():
    """Main producer loop"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Medical device telemetry producer')
    parser.add_argument('--devices', type=int, default=10, help='Number of devices to simulate')
    parser.add_argument('--interval', type=float, default=1.0, help='Seconds between events')
    parser.add_argument('--kafka', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    # Initialize producer
    producer = TelemetryProducer(bootstrap_servers=[args.kafka])
    
    # Create device simulators
    devices = [DeviceSimulator(f"device_{i:03d}") for i in range(args.devices)]
    
    logger.info(f"Starting telemetry generation for {args.devices} devices...")
    logger.info(f"Event interval: {args.interval}s")
    logger.info("Press Ctrl+C to stop")
    
    try:
        event_count = 0
        start_time = time.time()
        
        while True:
            # Generate event from each device
            for device in devices:
                event = device.generate_event()
                producer.send_event(event)
                event_count += 1
            
            # Stats every 100 events
            if event_count % 100 == 0:
                elapsed = time.time() - start_time
                rate = event_count / elapsed if elapsed > 0 else 0
                logger.info(f"📊 Sent {event_count} events ({rate:.1f} events/sec)")
            
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
        producer.close()
        
        elapsed = time.time() - start_time
        logger.info(f"Final stats: {event_count} events in {elapsed:.1f}s ({event_count/elapsed:.1f} events/sec)")


if __name__ == "__main__":
    main()
