#!/usr/bin/env python3
import gzip
import json
from datetime import datetime, timedelta

def make_sample():
    """Generate a synthetic sample capture matching the envelope format"""
    envs = []
    now = datetime.utcnow()
    
    # Create station status envelopes
    for i in range(3):
        station_data = {
            "station_id": str(i+1),
            "num_bikes_available": (i % 5) + 1,
            "num_docks_available": 10 - i,
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 1,
            "last_reported": int(now.timestamp()) - i*30
        }
        env = {
            "schema_version": 1,
            "event_type": "station_status",
            "poll_id": f"sample-poll-{now.strftime('%Y%m%d%H%M%S')}",
            "source": "https://gbfs.citibikenyc.com/gbfs/gbfs.json",
            "source_url": "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
            "gbfs_version": "2.3",
            "feed_last_updated": int(now.timestamp()) - 10,
            "fetched_at": (now - timedelta(seconds=i*5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00",
            "producer_version": "0.1.0",
            "payload": station_data
        }
        envs.append(env)
    
    # Add poll log
    poll_log = {
        "schema_version": 1,
        "event_type": "poll_log",
        "poll_id": f"sample-poll-{now.strftime('%Y%m%d%H%M%S')}",
        "source": "https://gbfs.citibikenyc.com/gbfs/gbfs.json",
        "source_url": "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
        "gbfs_version": "2.3",
        "feed_last_updated": int(now.timestamp()) - 10,
        "fetched_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00",
        "producer_version": "0.1.0",
        "payload": {
            "job": "station_status",
            "outcome": "ok",
            "feed_last_updated": int(now.timestamp()) - 10,
            "n_records": 3,
            "messages_delivered": 3,
            "messages_failed": 0,
            "latency_ms": 150
        }
    }
    envs.append(poll_log)
    
    return envs

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/citibike_sample.jsonl.gz"
    envs = make_sample()
    with gzip.open(path, "wt") as fh:
        for e in envs:
            fh.write(json.dumps(e) + "\n")
    print(f"Wrote {len(envs)} envelopes to {path}")
