import os, json, time, logging
import requests
from azure.eventhub import EventHubProducerClient, EventData

API_URL = "https://ckan2.multimediagdansk.pl/gpsPositions?v=2"
CONN = os.environ["EH_CONN_STR"]
EH   = os.environ["EH_NAME"]
INTERVAL = int(os.environ.get("INTERVAL", "20"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

producer = EventHubProducerClient.from_connection_string(CONN, eventhub_name=EH)

def send_snapshot(data):
    batch = producer.create_batch()
    for v in data["vehicles"]:
        v["lastUpdate"] = data.get("lastUpdate")
        batch.add(EventData(json.dumps(v, ensure_ascii=False).encode("utf-8")))
    producer.send_batch(batch)
    return len(data["vehicles"])

logging.info("producer -> Event Hub starting | interval=%ss", INTERVAL)
polls = 0
while True:
    try:
        r = requests.get(API_URL, timeout=10); r.raise_for_status()
        n = send_snapshot(r.json())
        polls += 1
        if polls % 30 == 0:
            logging.info("heartbeat | polls=%s last_vehicles=%s", polls, n)
    except Exception as e:
        logging.warning("cycle failed: %s", e)   # network/EH hiccup -> keep going
    time.sleep(INTERVAL)