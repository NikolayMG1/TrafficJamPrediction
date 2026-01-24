from flask import Flask, jsonify, render_template_string, request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from client.service import get_traffic_data

app = Flask(__name__)

FINLAND_TZ = ZoneInfo("Europe/Helsinki")

base_map_html = """
<!DOCTYPE html>
<html>
<head>
    <title>Finland Traffic Map</title>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

    <style>
        html, body, #map { height: 100%; margin:0; padding:0; }
        .control {
            position:absolute; top:10px; right:10px; z-index:1000;
            background:white; padding:10px; font-family:Arial; font-size:13px;
            box-shadow:0 0 6px rgba(0,0,0,0.3);
        }
        #calendarBox { margin-top:8px; }
        button { margin-right:4px; }
        #status { margin-top:6px; font-size:12px; color:gray; }
    </style>
</head>
<body>

<div class="control">
    <button onclick="toggleCalendar()">Select time</button>
    <button onclick="backToLive()">Live</button>

    <div id="calendarBox" style="display:none;">
        <input type="datetime-local" id="historyTime"><br><br>
        <button onclick="confirmTime()">Confirm</button>
        <button onclick="toggleCalendar()">Cancel</button>
    </div>

    <div id="status"></div>
</div>

<div id="map"></div>

<script>
var map = L.map('map').setView([64.0,26.0],6);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:19 }).addTo(map);

var markers = L.layerGroup().addTo(map);
var congestion_colors = {
    "Heavy traffic":"red",
    "Slightly congested":"yellow",
    "Not congested":"green"
};

var refreshInterval = null;
var liveMode = true;

function loadPoints() {
    var selectedTime = document.getElementById("historyTime").value;
    var url = "/data";

    if (selectedTime) {
        liveMode = false;
        url += "?start=" + selectedTime;
    }

    fetch(url, { cache: "no-store" })
        .then(response => response.json())
        .then(data => {
            markers.clearLayers();

            data.forEach(row => {
                var color = congestion_colors[row.congestion_state] || "gray";
                var circle = L.circleMarker(
                    [row.latitude, row.longitude],
                    { radius:5, color:color, fillColor:color, fillOpacity:0.7 }
                ).bindPopup(
                    "Station ID: " + row.station_id + "<br>" +
                    "Vehicle Count: " + row.vehicle_count + "<br>" +
                    "Avg Speed: " + row.avg_speed.toFixed(2) + "<br>" +
                    "Congestion: " + row.congestion_state
                );
                markers.addLayer(circle);
            });

            document.getElementById("status").innerText =
                liveMode ? "Live update: " + new Date().toLocaleTimeString()
                         : "Historical snapshot";
        });
}

function toggleCalendar(){
    var box = document.getElementById("calendarBox");
    box.style.display = box.style.display === "none" ? "block" : "none";
}

function confirmTime(){
    if (refreshInterval) clearInterval(refreshInterval);
    toggleCalendar();
    loadPoints();
}

function startLiveMode(){
    liveMode = true;
    loadPoints();
    refreshInterval = setInterval(loadPoints, 300000);
}

function backToLive(){
    document.getElementById("historyTime").value = "";
    if (refreshInterval) clearInterval(refreshInterval);
    startLiveMode();
}

startLiveMode();
</script>

</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(base_map_html)

@app.route("/data")
def data():
    start = request.args.get("start")

    if not start:
        window_end = datetime.now(timezone.utc) + timedelta(minutes=120)
        temp = (window_end.minute // 5) * 5
        window_end = window_end.replace(minute=temp, second=0, microsecond=0)
        window_start = window_end - timedelta(minutes=5)
    else:
        local_dt = datetime.fromisoformat(start)
        local_dt = local_dt.replace(tzinfo=FINLAND_TZ)
        window_end = local_dt.astimezone(timezone.utc) + timedelta(minutes=120)
        temp = (window_end.minute // 5) * 5
        window_end = window_end.replace(minute=temp, second=0, microsecond=0)
        window_start = window_end - timedelta(minutes=5)

    df = get_traffic_data(window_start, window_end)

    if df.empty:
        return jsonify([])

    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
