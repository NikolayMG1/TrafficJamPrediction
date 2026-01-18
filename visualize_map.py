from flask import Flask, jsonify, render_template_string
import pandas as pd
import time
from format_data import create_df

app = Flask(__name__)

base_map_html = """
<!DOCTYPE html>
<html>
<head>
    <title>Finland Traffic Map</title>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        html, body, #map { height: 100%; margin: 0; padding: 0; }
    </style>
</head>
<body>
<div id="map"></div>
<script>
var map = L.map('map').setView([64.0, 26.0], 6);

// Add OpenStreetMap tiles
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19
}).addTo(map);

// Keep markers in a layer group
var markers = L.layerGroup().addTo(map);

// Congestion colors
var congestion_colors = {
    "Full stop": "red",
    "Low congestion": "yellow",
    "No congestion": "green",
    "Slightly congested": "orange"
};

// Load points from backend
function loadPoints() {
    fetch('/data')
    .then(response => response.json())
    .then(data => {
        markers.clearLayers();  // Remove old markers
        data.forEach(row => {
            var color = congestion_colors[row.congestion_state] || "gray";
            var circle = L.circleMarker([row.latitude, row.longitude], {
                radius: 5,
                color: color,
                fillColor: color,
                fillOpacity: 0.7
            }).bindPopup(
                "Station ID: " + row.station_id + "<br>" +
                "Vehicle Count: " + row.vehicle_count + "<br>" +
                "Avg Speed: " + row.avg_speed.toFixed(2) + "<br>" +
                "Congestion: " + row.congestion_state
            );
            markers.addLayer(circle);
        });
    });
}

// Load initially
loadPoints();

// Refresh every 60 seconds
setInterval(loadPoints, 60000);
</script>
</body>
</html>
"""

cached_df = None
last_update = 0

@app.route('/')
def index():
    return render_template_string(base_map_html)

@app.route('/data')
def data():
    global cached_df, last_update
    now = time.time()
    # Refresh every 60 seconds
    if cached_df is None or now - last_update > 60:
        cached_df = create_df()
        time.sleep(5)
        last_update = now
    return jsonify(cached_df.to_dict(orient='records'))

if __name__ == "__main__":
    app.run(debug=True)
