import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Finland Traffic Measurements API
API_URL = "https://tie.digitraffic.fi/api/tms/v1/stations/data"


# Kafka configuration
KAFKA_TOPIC = "traffic-finland"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_traffic_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def produce_events(data):
    for station in data.get("results", []):
        try:
            event = {
                "event_time": station.get("measurementTime")
                              or datetime.utcnow().isoformat(),
                "station_id": station.get("id"),
                "road_name": station.get("name"),
                "vehicle_count": station.get("vehicleCount"),
                "avg_speed": station.get("averageSpeed"),
                "latitude": station.get("location", {}).get("coordinates", [None, None])[1],
                "longitude": station.get("location", {}).get("coordinates", [None, None])[0],
                "source": "Helsinki Traffic API"
            }

            producer.send(KAFKA_TOPIC, event)

        except Exception as e:
            print(f"Skipping invalid record: {e}")

    producer.flush()

def main():
    while True:
        try:
            data = fetch_traffic_data() 
            print("Fetched traffic data from API")
            produce_events(data)
            print("Traffic data sent to Kafka")

        except Exception as e:
            print(f"Error fetching or producing data: {e}")

        # Poll API every 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    main()
