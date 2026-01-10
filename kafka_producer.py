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
    result = {}

    for station in data.get("stations", []):
        try:
            date = station.get("dataUpdatedTime")
            tmsNumber = station.get("tmsNumber")
            sensorValues = station.get("sensorValues", [])

            # result must be a dict, not a list
            result[tmsNumber] = []

            for sensor in sensorValues:
                if sensor.get("unit") == "km/h":
                    start_str = sensor.get("timeWindowStart")
                    end_str = sensor.get("timeWindowEnd")
  
                    start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    elapsed_time = (end_time - start_time).total_seconds()
                    print(f"start_time: {start_time}, end_time: {end_time} and elapsed_time: {elapsed_time}")
                    speed = sensor.get("value")
                    print(f"Station {tmsNumber} - Elapsed Time: {elapsed_time}, Speed: {speed}")
                    
                    result[tmsNumber].append({
                        "elapsed_time": str(elapsed_time),
                        "speed": speed
                    })

            event = {
                "event_time": date,
                "station_id": tmsNumber,
                "measurements": result[tmsNumber]
            }

            # print(f"Producing event for station event {event}")
            producer.send(KAFKA_TOPIC, value=event)

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
