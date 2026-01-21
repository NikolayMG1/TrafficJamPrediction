from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

def get_from_db_format():
    MONGO_URI = "mongodb://localhost:27017"
    DB_NAME = "digi-traffic"

    TRAFFIC_COLLECTION = "traffic"
    STATIONS_COLLECTION = "stations"

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    traffic_col = db[TRAFFIC_COLLECTION]
    stations_col = db[STATIONS_COLLECTION]

    # now = datetime.utcnow()
    # five_minutes_ago = now - timedelta(minutes=5)

    # traffic_query = {
    #     "window.start": {"$gte": five_minutes_ago},
    #     "window.end": {"$lte": now}
    # }

    window_start = datetime(2026, 1, 18, 16, 10, 0)  # 16:10 UTC
    window_end   = datetime(2026, 1, 18, 16, 15, 0)  # 16:15 UTC

    traffic_query = {
        "window.start": {"$gte": window_start},
        "window.end": {"$lte": window_end}
    }

    traffic_docs = list(
        traffic_col.find(traffic_query, {"_id": 0})
    )

    traffic_df = pd.DataFrame(traffic_docs)

    if traffic_df.empty:
        print("No traffic data found in the last 5 minutes.")
        exit(0)

    traffic_df["station_id"] = traffic_df["station_id"].astype(str)

    station_ids = traffic_df["station_id"].unique().tolist()

    stations_query = {
        "station_id": {"$in": station_ids}
    }

    stations_docs = list(
        stations_col.find(stations_query, {"_id": 0})
    )

    stations_df = pd.DataFrame(stations_docs)

    stations_df["station_id"] = stations_df["station_id"].astype(str)

    final_df = traffic_df.merge(
        stations_df,
        on="station_id",
        how="left"
    )

    return final_df
