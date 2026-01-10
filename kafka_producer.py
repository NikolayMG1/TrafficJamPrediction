import pprint

import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

API_URL = "https://tie.digitraffic.fi/api/tms/v1/stations/data"

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
    for station in data.get("stations", []):
        station_id = station.get("id")
        sensor_values = station.get("sensorValues", [])

        for sensor in sensor_values:
            try:
                event = {
                    "event_time": sensor.get("measuredTime"),
                    "station_id": station_id,

                    "entry_id": sensor.get("id"),
                    "station_id_sensor_value": sensor.get("stationId"),
                    "sensor_name": sensor.get("name"),
                    "sensor_short_name": sensor.get("shortName"),
                    "unit": sensor.get("unit"),
                    "value": sensor.get("value"),
                }

                producer.send(
                    KAFKA_TOPIC,
                    key=str(station_id).encode("utf-8"),
                    value=event
                )

            except Exception as e:
                print(f"Skipping sensor at station {station_id}: {e}")

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

        time.sleep(60)

if __name__ == "__main__":
    main()
