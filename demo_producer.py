"""Demo producer — injects synthetic vehicle events WITH the new `occupancy`
field into the SAME Event Hub the VM producer uses.

Run this from your laptop, on demand, during the schema-evolution demo.
It does NOT touch the VM producer or production data.

Setup (once, on your laptop):
    pip install azure-eventhub
    set EH_CONN_STR = <the SEND connection string, same as the VM>
    set EH_NAME     = <the Event Hub name, e.g. live_transit_monitor_evh>

Run (each run sends 10 events with occupancy):
    python demo_producer.py
"""

import os
import json
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()  # load .env file if present

CONN = os.environ["EH_CONN_STR"]     # SEND connection string (same as VM producer)
EH   = os.environ["EH_NAME"]         # Event Hub name
N    = int(os.environ.get("N", "10"))  # how many demo events to send


def demo_event(i: int) -> dict:
    """One synthetic Gdansk vehicle event that INCLUDES the new occupancy field."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "generated": now,
        "routeShortName": "999",           # obvious demo line
        "tripId": None,
        "routeId": 999,
        "headsign": "DEMO",
        "vehicleCode": f"D{i:03d}",         # D000, D001, ... -> easy to spot
        "vehicleService": "demo",
        "vehicleId": 900000 + i,
        "speed": 20,
        "direction": 0,
        "delay": 30,
        "scheduledTripStartTime": None,
        "lat": 54.35 + random.uniform(-0.02, 0.02),   # around Gdansk centre
        "lon": 18.64 + random.uniform(-0.02, 0.02),
        "gpsQuality": 3,
        "lastUpdate": now,
        "occupancy": random.choice(["LOW", "MEDIUM", "HIGH"]),   # <-- THE NEW FIELD
    }


def main():
    producer = EventHubProducerClient.from_connection_string(CONN, eventhub_name=EH)
    with producer:
        batch = producer.create_batch()
        for i in range(N):
            batch.add(EventData(json.dumps(demo_event(i), ensure_ascii=False).encode("utf-8")))
        producer.send_batch(batch)
    print(f"Sent {N} demo events WITH occupancy to Event Hub '{EH}'.")


if __name__ == "__main__":
    main()
