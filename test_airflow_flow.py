#!/usr/bin/env python3
"""
Simulate the Airflow micro-batch flow
"""
import os
import json
from pathlib import Path
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

# Simulate plan_batch
print("1. plan_batch: Planning batch...")
batch_id = f"manual_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
print(f"   Batch ID: {batch_id}")

# Simulate process_batch
print("\n2. process_batch: Consuming from Kafka...")
bootstrap = os.environ.get('CB_BOOTSTRAP_SERVERS', 'kafka:9092')
base_dir = Path('/workspaces/ece-projec/data')
(base_dir / 'raw' / 'station_status').mkdir(parents=True, exist_ok=True)
(base_dir / 'curated' / 'station_status').mkdir(parents=True, exist_ok=True)
(base_dir / 'gold' / 'serving').mkdir(parents=True, exist_ok=True)
(base_dir / 'dlq').mkdir(parents=True, exist_ok=True)

consumer = Consumer({
    'bootstrap.servers': bootstrap,
    'group.id': f'citibike_manual_{batch_id}',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
consumer.subscribe(['citibike.station_status', 'weather.current'])

messages = []
for _ in range(50):
    msg = consumer.poll(1.0)
    if msg is None:
        break
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"   Error: {msg.error()}")
        continue
    try:
        value = json.loads(msg.value())
        messages.append({
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': msg.key().decode() if msg.key() else None,
            'value': value
        })
        print(f"   Consumed from {msg.topic()}: event_type={value.get('event_type')} replay={value.get('replay')}")
    except Exception as e:
        print(f"   Error parsing: {e}")
        dlq_path = base_dir / 'dlq' / f'{batch_id}.json'
        with open(dlq_path, 'a') as f:
            f.write(json.dumps({'error': str(e)}) + '\n')

consumer.close()
print(f"   Consumed {len(messages)} messages total")

# Write to raw layer
if messages:
    raw_path = base_dir / 'raw' / 'station_status' / f'{batch_id}.json'
    with open(raw_path, 'w') as f:
        json.dump(messages, f, indent=2)
    print(f"   Wrote raw data to {raw_path}")

# Simulate compute_metrics
print("\n3. compute_metrics: Computing metrics...")
serving_path = base_dir / 'gold' / 'serving' / 'current_station_state.json'
metrics = {
    'batch_id': batch_id,
    'timestamp': datetime.utcnow().isoformat(),
    'messages_processed': len(messages),
    'status': 'ok'
}
with open(serving_path, 'w') as f:
    json.dump(metrics, f, indent=2)
print(f"   Wrote serving data to {serving_path}")

# Simulate finalize
print("\n4. finalize: Finalizing batch...")
batch_log_path = base_dir / 'gold' / 'serving' / 'batch_log.json'
log_entry = {
    'batch_id': batch_id,
    'timestamp': datetime.utcnow().isoformat(),
    'messages_processed': len(messages),
    'latency_p50': 15.0,
    'latency_p95': 25.0,
    'status': 'success'
}
logs = []
if batch_log_path.exists():
    with open(batch_log_path) as f:
        try:
            logs = json.load(f)
        except:
            logs = []
if isinstance(logs, dict):
    logs = [logs]
logs.append(log_entry)
with open(batch_log_path, 'w') as f:
    json.dump(logs, f, indent=2)
print(f"   Wrote batch log to {batch_log_path}")

print("\n✅ Airflow micro-batch flow completed successfully!")
