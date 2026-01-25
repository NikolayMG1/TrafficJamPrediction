from pymongo import MongoClient
import pandas as pd
from datetime import datetime

def get_traffic_data(window_start: datetime, window_end: datetime):

    if window_start.tzinfo is not None:
        window_start = window_start.replace(tzinfo=None)
        window_end = window_end.replace(tzinfo=None)

    MONGO_URI = "mongodb://localhost:27017"
    DB_NAME = "digi-traffic"

    TRAFFIC_COLLECTION = "traffic-data"
    STATIONS_COLLECTION = "stations"

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    traffic_col = db[TRAFFIC_COLLECTION]
    stations_col = db[STATIONS_COLLECTION]

    traffic_query = {
        "window.start": {"$lte": window_end},
        "window.end": {"$gte": window_start}
    }

    print("Time window:", window_start, "→", window_end)

    traffic_docs = list(traffic_col.find(traffic_query, {"_id": 0}))
    print("Traffic docs found:", len(traffic_docs))

    if not traffic_docs:
        return pd.DataFrame()

    traffic_df = pd.DataFrame(traffic_docs)
    traffic_df["station_id"] = traffic_df["station_id"].astype(str)

    stations_docs = list(
        stations_col.find(
            {"station_id": {"$in": traffic_df["station_id"].unique().tolist()}},
            {"_id": 0}
        )
    )

    stations_df = pd.DataFrame(stations_docs)
    stations_df["station_id"] = stations_df["station_id"].astype(str)

    return traffic_df.merge(stations_df, on="station_id", how="left")
