import pandas as pd
import json

def create_df():
    with open("digi-traffic.stations.json") as f:
        data1 = json.load(f)

    with open("digi-traffic.traffic-data.json") as f:
        data2 = json.load(f)

    df1 = pd.json_normalize(data1)
    df2 = pd.json_normalize(data2)

    df1 = df1[["station_id", "latitude", "longitude"]]
    df2 = df2[["station_id", "vehicle_count.$numberLong", "avg_speed", "congestion_state"]]

    df1["station_id"] = pd.to_numeric(df1["station_id"], errors="coerce")
    df2["station_id"] = pd.to_numeric(df2["station_id"], errors="coerce")

    df2["vehicle_count"] = pd.to_numeric(
        df2["vehicle_count.$numberLong"], errors="coerce"
    )

    df1 = df1.dropna(subset=["station_id"])
    df2 = df2.dropna(subset=["station_id", "vehicle_count"])

    df1["station_id"] = df1["station_id"].astype(int)
    df2["station_id"] = df2["station_id"].astype(int)
    df2["vehicle_count"] = df2["vehicle_count"].astype(int)

    merged_df = pd.merge(df2, df1, on="station_id", how="inner")

    merged_df = merged_df[
        ["station_id", "vehicle_count", "avg_speed", "congestion_state", "latitude", "longitude"]
    ]

    return merged_df


print(create_df())