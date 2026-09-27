import os
import re
import threading
from flask import Flask, jsonify, request
from databricks.sdk import WorkspaceClient

app = Flask(__name__)
w = WorkspaceClient()
WAREHOUSE_ID = "71cf7b1e39e3ffb7"

LOCATIONS = [
    {"name": "Park Royal Mall", "lat": 49.3265, "lng": -123.138, "table": "synthetic_park_royal_mall"},
    {"name": "UBC", "lat": 49.2606, "lng": -123.246, "table": "synthetic_data_ubc"},
    {"name": "Waterfront Station", "lat": 49.2857, "lng": -123.1115, "table": "synthetic_data_waterfront"},
]

STATS = {}
ALL_COUNTS = {}
CUMULATIVE_STATS = {}
CUMULATIVE_COUNTS = {}
DATES = []
INITIALIZED = False
init_lock = threading.Lock()


def execute_sql(statement, byte_limit=None, retries=3):
    kwargs = dict(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        catalog="dbacademy",
        schema="default",
        wait_timeout="50s",
    )
    if byte_limit:
        kwargs["byte_limit"] = byte_limit
    for attempt in range(retries):
        result = w.statement_execution.execute_statement(**kwargs)
        if result.manifest and result.manifest.schema:
            break
        if attempt < retries - 1:
            import time
            time.sleep(5)
    else:
        raise RuntimeError('SQL execution returned no result after ' + str(retries) + ' attempts. Warehouse may be starting up.')
    columns = [c.name for c in result.manifest.schema.columns]
    rows = []
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            rows.append(dict(zip(columns, row)))
    return rows


def initialize():
    global STATS, ALL_COUNTS, CUMULATIVE_STATS, CUMULATIVE_COUNTS, DATES, INITIALIZED
    if INITIALIZED:
        return
    with init_lock:
        if INITIALIZED:
            return

        date_sql = """
        SELECT DISTINCT DATE(timestamp) as visit_date FROM dbacademy.default.synthetic_park_royal_mall
        UNION
        SELECT DISTINCT DATE(timestamp) FROM dbacademy.default.synthetic_data_ubc
        UNION
        SELECT DISTINCT DATE(timestamp) FROM dbacademy.default.synthetic_data_waterfront
        ORDER BY visit_date
        """
        date_rows = execute_sql(date_sql)
        DATES = [r["visit_date"] for r in date_rows]

        for loc in LOCATIONS:
            table = loc["table"]
            sql = f"""
            WITH bucketed AS (
              SELECT
                DAYOFWEEK(timestamp) as weekday,
                FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as time_bucket,
                DATE(timestamp) as visit_date,
                COUNT(*) as visitor_count
              FROM dbacademy.default.{table}
              GROUP BY DAYOFWEEK(timestamp), DATE(timestamp),
                       FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30)
            )
            SELECT
              weekday,
              time_bucket,
              ROUND(AVG(visitor_count), 2) as avg_count,
              ROUND(STDDEV(visitor_count), 2) as std_count
            FROM bucketed
            GROUP BY weekday, time_bucket
            ORDER BY weekday, time_bucket
            """
            rows = execute_sql(sql)
            loc_stats = {}
            for r in rows:
                wd = int(r["weekday"])
                tb = int(r["time_bucket"])
                if wd not in loc_stats:
                    loc_stats[wd] = {}
                loc_stats[wd][tb] = {
                    "avg": float(r["avg_count"]),
                    "std": float(r["std_count"]) if r["std_count"] is not None else 0.0,
                }
            STATS[loc["name"]] = loc_stats

        # Pre-compute all counts per (date, time_bucket) per location
        for loc in LOCATIONS:
            table = loc["table"]
            sql = f"""
            SELECT
              DATE(timestamp) as visit_date,
              FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as time_bucket,
              COUNT(*) as visitor_count
            FROM dbacademy.default.{table}
            GROUP BY DATE(timestamp), FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30)
            ORDER BY visit_date, time_bucket
            """
            rows = execute_sql(sql, byte_limit=10485760)
            loc_counts = {}
            for r in rows:
                d = r["visit_date"]
                tb = int(r["time_bucket"])
                vc = int(r["visitor_count"])
                if d not in loc_counts:
                    loc_counts[d] = [0] * 48
                loc_counts[d][tb] = vc
            ALL_COUNTS[loc["name"]] = loc_counts

        # Compute cumulative occupancy (people present including dwell time)
        for loc in LOCATIONS:
            table = loc["table"]
            # For each time bucket, count people whose arrival + dwell overlaps that bucket
            sql = f"""
            WITH arrivals AS (
              SELECT
                DATE(timestamp) as visit_date,
                FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as arrival_bucket,
                FLOOR(dwell_time / 30.0) as dwell_buckets,
                DAYOFWEEK(timestamp) as weekday
              FROM dbacademy.default.{table}
            ),
            occupancy AS (
              SELECT
                visit_date,
                weekday,
                bucket_id as time_bucket,
                COUNT(*) as occupancy_count
              FROM arrivals
              CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, 47)) as bucket_id)
              WHERE bucket_id >= arrival_bucket
                AND bucket_id < arrival_bucket + dwell_buckets + 1
              GROUP BY visit_date, weekday, bucket_id
            )
            SELECT
              weekday,
              time_bucket,
              ROUND(AVG(occupancy_count), 2) as avg_count,
              ROUND(STDDEV(occupancy_count), 2) as std_count
            FROM occupancy
            GROUP BY weekday, time_bucket
            ORDER BY weekday, time_bucket
            """
            rows = execute_sql(sql)
            loc_stats = {}
            for r in rows:
                wd = int(r["weekday"])
                tb = int(r["time_bucket"])
                if wd not in loc_stats:
                    loc_stats[wd] = {}
                loc_stats[wd][tb] = {
                    "avg": float(r["avg_count"]),
                    "std": float(r["std_count"]) if r["std_count"] is not None else 0.0,
                }
            CUMULATIVE_STATS[loc["name"]] = loc_stats

            # Cumulative counts per date/bucket
            sql2 = f"""
            WITH arrivals AS (
              SELECT
                DATE(timestamp) as visit_date,
                FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as arrival_bucket,
                FLOOR(dwell_time / 30.0) as dwell_buckets
              FROM dbacademy.default.{table}
            )
            SELECT
              visit_date,
              bucket_id as time_bucket,
              COUNT(*) as occupancy_count
            FROM arrivals
            CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, 47)) as bucket_id)
            WHERE bucket_id >= arrival_bucket
              AND bucket_id < arrival_bucket + dwell_buckets + 1
            GROUP BY visit_date, bucket_id
            ORDER BY visit_date, bucket_id
            """
            rows2 = execute_sql(sql2, byte_limit=10485760)
            loc_counts = {}
            for r in rows2:
                d = r["visit_date"]
                tb = int(r["time_bucket"])
                vc = int(r["occupancy_count"])
                if d not in loc_counts:
                    loc_counts[d] = [0] * 48
                loc_counts[d][tb] = vc
            CUMULATIVE_COUNTS[loc["name"]] = loc_counts

        INITIALIZED = True


@app.route("/")
def index():
    return HTML_PAGE


@app.route("/api/init")
def api_init():
    try:
        initialize()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({
        "locations": [{"name": l["name"], "lat": l["lat"], "lng": l["lng"]} for l in LOCATIONS],
        "dates": DATES,
        "stats": STATS,
        "all_counts": ALL_COUNTS,
        "cumulative_stats": CUMULATIVE_STATS,
        "cumulative_counts": CUMULATIVE_COUNTS,
    })


@app.route("/api/counts")
def api_counts():
    initialize()
    date = request.args.get("date", "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return jsonify({"error": "Invalid date format"}), 400

    counts = {}
    for loc in LOCATIONS:
        table = loc["table"]
        sql = f"""
        SELECT
          FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as time_bucket,
          COUNT(*) as visitor_count
        FROM dbacademy.default.{table}
        WHERE DATE(timestamp) = '{date}'
        GROUP BY FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30)
        ORDER BY time_bucket
        """
        rows = execute_sql(sql)
        counts[loc["name"]] = {int(r["time_bucket"]): int(r["visitor_count"]) for r in rows}

    return jsonify({"counts": counts})


@app.route("/api/details")
def api_details():
    initialize()
    location = request.args.get("location", "")
    date = request.args.get("date", "")
    bucket = request.args.get("bucket", "")
    mode = request.args.get("mode", "new")

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return jsonify({"error": "Invalid date"}), 400
    if not bucket.isdigit() or not (0 <= int(bucket) <= 47):
        return jsonify({"error": "Invalid bucket"}), 400

    loc = next((l for l in LOCATIONS if l["name"] == location), None)
    if not loc:
        return jsonify({"error": "Invalid location"}), 400

    table = loc["table"]
    bucket_int = int(bucket)

    if mode == "cumulative":
        sql = f"""
        WITH arrivals AS (
          SELECT
            origin,
            FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) as arrival_bucket,
            FLOOR(dwell_time / 30.0) as dwell_buckets,
            dwell_time
          FROM dbacademy.default.{table}
          WHERE DATE(timestamp) = '{date}'
        ),
        present AS (
          SELECT origin, dwell_time
          FROM arrivals
          WHERE {bucket_int} >= arrival_bucket AND {bucket_int} < arrival_bucket + dwell_buckets + 1
        ),
        details AS (
          SELECT origin, COUNT(*) as origin_count, AVG(dwell_time) as avg_dwell
          FROM present GROUP BY origin
        ),
        totals AS (
          SELECT COUNT(*) as total_count, AVG(dwell_time) as total_avg_dwell FROM present
        )
        SELECT d.origin, d.origin_count, ROUND(d.avg_dwell, 1) as avg_dwell,
               t.total_count, ROUND(t.total_avg_dwell, 1) as total_avg_dwell
        FROM details d CROSS JOIN totals t
        ORDER BY d.origin_count DESC LIMIT 10
        """
    else:
        sql = f"""
    WITH details AS (
      SELECT
        origin,
        COUNT(*) as origin_count,
        AVG(dwell_time) as avg_dwell
      FROM dbacademy.default.{table}
      WHERE DATE(timestamp) = '{date}'
        AND FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) = {bucket_int}
      GROUP BY origin
    ),
    totals AS (
      SELECT
        COUNT(*) as total_count,
        AVG(dwell_time) as total_avg_dwell
      FROM dbacademy.default.{table}
      WHERE DATE(timestamp) = '{date}'
        AND FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) = {bucket_int}
    )
    SELECT
      d.origin,
      d.origin_count,
      ROUND(d.avg_dwell, 1) as avg_dwell,
      t.total_count,
      ROUND(t.total_avg_dwell, 1) as total_avg_dwell
    FROM details d
    CROSS JOIN totals t
    ORDER BY d.origin_count DESC
    LIMIT 10
    """
    rows = execute_sql(sql)

    if not rows:
        return jsonify({"total_count": 0, "avg_dwell": 0, "origins": []})

    first = rows[0]
    total_count = int(first["total_count"]) if first["total_count"] else 0
    total_avg_dwell = float(first["total_avg_dwell"]) if first["total_avg_dwell"] else 0.0

    origins = []
    for r in rows:
        origins.append({
            "origin": r["origin"],
            "count": int(r["origin_count"]),
            "avg_dwell": float(r["avg_dwell"]) if r["avg_dwell"] else 0.0,
        })

    return jsonify({
        "total_count": total_count,
        "avg_dwell": total_avg_dwell,
        "origins": origins,
    })


import requests as http_requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def compute_anomaly_duration(loc_name, weekday, date, bucket, mode):
    """Count consecutive anomalous 30-min buckets leading up to and including current bucket."""
    current_stats = CUMULATIVE_STATS if mode == "cumulative" else STATS
    source_counts = CUMULATIVE_COUNTS if mode == "cumulative" else ALL_COUNTS
    loc_counts = source_counts.get(loc_name, {})
    date_arr = loc_counts.get(date, [0] * 48)
    duration = 0
    for b in range(bucket, -1, -1):
        count = date_arr[b] if b < len(date_arr) else 0
        stat = current_stats.get(loc_name, {}).get(weekday, {}).get(b)
        if not stat or count == 0:
            break
        if count > stat["avg"] + 2 * stat["std"]:
            duration += 1
        else:
            break
    return duration


@app.route("/api/investigate")
def api_investigate():
    initialize()
    location = request.args.get("location", "")
    date = request.args.get("date", "")
    bucket = request.args.get("bucket", "")
    mode = request.args.get("mode", "new")

    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "Anthropic API key not configured."}), 500
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return jsonify({"error": "Invalid date"}), 400
    if not bucket.isdigit() or not (0 <= int(bucket) <= 47):
        return jsonify({"error": "Invalid bucket"}), 400

    loc = next((l for l in LOCATIONS if l["name"] == location), None)
    if not loc:
        return jsonify({"error": "Invalid location"}), 400

    bucket_int = int(bucket)
    weekday = None
    for l in LOCATIONS:
        if l["name"] == location:
            pass
    # Compute weekday from date string
    from datetime import datetime
    dt = datetime.strptime(date, "%Y-%m-%d")
    weekday = dt.isoweekday()  # 1=Monday, 7=Sunday

    # Compute anomaly duration
    duration = compute_anomaly_duration(location, weekday, date, bucket_int, mode)
    duration_str = f"{duration} interval{'s' if duration != 1 else ''} ({duration * 30} minutes)"

    # Build time label
    h = bucket_int // 2
    m = (bucket_int % 2) * 30
    time_label = f"{h:02d}:{m:02d}"

    prompt = (
        f"An anomalous cell tower spike was detected in Metro Vancouver, BC:\n"
        f"Location: {location} | Date: {date} | Time: {time_label} | Duration: {duration_str}\n\n"
        f"Search for events near {location}, Vancouver BC on {date} that explain this spike. "
        f"Context: Waterfront Station = downtown near Canada Place/cruise terminal; "
        f"Park Royal Mall = West Vancouver shopping centre; UBC = university campus.\n\n"
        f"RULES: Output EXACTLY 1-3 sentences. No preamble. No 'Based on my search' filler. "
        f"If an event explains it, name it directly. If nothing found, say 'No known event explains this spike. "
        f"This should be investigated as a potential cause for concern.'"
    )

    try:
        resp = http_requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 2}],
            },
            timeout=60,
        )
        data = resp.json()
        if resp.status_code != 200:
            return jsonify({"error": f"Claude API error: {data.get('error', {}).get('message', 'Unknown')}"}), 500

        # Extract text from response
        text_parts = []
        for block in data.get("content", []):
            if block.get("type") == "text":
                text_parts.append(block["text"])
        explanation = " ".join(text_parts).strip()
        return jsonify({"explanation": explanation, "duration": duration_str})
    except Exception as e:
        return jsonify({"error": f"Failed to call Claude API: {str(e)}"}), 500


@app.route("/api/origins")
def api_origins():
    initialize()
    date = request.args.get("date", "")
    bucket = request.args.get("bucket", "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return jsonify({"error": "Invalid date"}), 400
    if not bucket.isdigit() or not (0 <= int(bucket) <= 47):
        return jsonify({"error": "Invalid bucket"}), 400
    bucket_int = int(bucket)
    result = {}
    for loc in LOCATIONS:
        table = loc["table"]
        sql = f"""
        SELECT origin, COUNT(*) as origin_count
        FROM dbacademy.default.{table}
        WHERE DATE(timestamp) = '{date}'
          AND FLOOR(HOUR(timestamp) * 2 + MINUTE(timestamp) / 30) = {bucket_int}
        GROUP BY origin
        ORDER BY origin_count DESC
        LIMIT 5
        """
        rows = execute_sql(sql)
        result[loc["name"]] = [{"origin": r["origin"], "count": int(r["origin_count"])} for r in rows]
    return jsonify({"origins": result})


WEATHER_CODE_MAP = {0: ("Clear", "\u2600\ufe0f"), 1: ("Mainly Clear", "\u26c5\ufe0f"), 2: ("Partly Cloudy", "\u26c5"), 3: ("Overcast", "\u2601\ufe0f"), 45: ("Fog", "\u1f32b\ufe0f"), 48: ("Rime Fog", "\u1f32b\ufe0f"), 51: ("Light Drizzle", "\u1f326\ufe0f"), 53: ("Drizzle", "\u1f326\ufe0f"), 55: ("Heavy Drizzle", "\u1f327\ufe0f"), 61: ("Light Rain", "\u1f326\ufe0f"), 63: ("Rain", "\u1f327\ufe0f"), 65: ("Heavy Rain", "\u1f327\ufe0f"), 66: ("Freezing Rain", "\u1f327\ufe0f"), 67: ("Freezing Rain", "\u1f327\ufe0f"), 71: ("Light Snow", "\u1f328\ufe0f"), 73: ("Snow", "\u2744\ufe0f"), 75: ("Heavy Snow", "\u2744\ufe0f"), 77: ("Snow Grains", "\u1f328\ufe0f"), 80: ("Rain Showers", "\u1f326\ufe0f"), 81: ("Rain Showers", "\u1f327\ufe0f"), 82: ("Heavy Showers", "\u26c8\ufe0f"), 85: ("Snow Showers", "\u1f328\ufe0f"), 86: ("Snow Showers", "\u2744\ufe0f"), 95: ("Thunderstorm", "\u26c8\ufe0f"), 96: ("Thunderstorm + Hail", "\u26c8\ufe0f"), 99: ("Thunderstorm + Hail", "\u26c8\ufe0f")}


@app.route("/api/weather")
def api_weather():
    lat = request.args.get("lat", "")
    lng = request.args.get("lng", "")
    date = request.args.get("date", "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return jsonify({"error": "Invalid date"}), 400
    try:
        resp = http_requests.get(
            f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation,weather_code,wind_speed_10m&timezone=America/Vancouver",
            timeout=15,
        )
        data = resp.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        precip = hourly.get("precipitation", [])
        codes = hourly.get("weather_code", [])
        winds = hourly.get("wind_speed_10m", [])
        hourly_data = []
        for i in range(len(times)):
            code = codes[i] if i < len(codes) else 0
            label, icon = WEATHER_CODE_MAP.get(code, ("Unknown", "\u2753"))
            hour_str = times[i].split("T")[1] if "T" in times[i] else times[i]
            hourly_data.append({
                "hour": hour_str,
                "temp": round(temps[i], 1) if i < len(temps) and temps[i] is not None else None,
                "precip": precip[i] if i < len(precip) and precip[i] is not None else 0,
                "weather_code": code,
                "weather_label": label,
                "weather_icon": icon,
                "wind": round(winds[i], 1) if i < len(winds) and winds[i] is not None else None,
            })
        return jsonify({"hourly": hourly_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vancouver Cell Tower Anomaly Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', Arial, sans-serif; background: #eef2f5; }
#loading { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 100vh; }
#loading .spinner { width: 50px; height: 50px; border: 5px solid #ccc; border-top: 5px solid #2c3e50; border-radius: 50%; animation: spin 1s linear infinite; margin-bottom: 15px; }
@keyframes spin { 100% { transform: rotate(360deg); } }
#loading p { color: #666; font-size: 1.1em; }
#app { display: none; height: 100vh; flex-direction: column; }
.header { background: #1a2937; color: white; padding: 10px 20px; display: flex; align-items: center; gap: 15px; }
.header h1 { font-size: 1.2em; font-weight: 600; }
.header .legend { margin-left: auto; display: flex; gap: 15px; font-size: 0.8em; align-items: center; }
.header .legend-item { display: flex; align-items: center; gap: 5px; }
.header .legend-dot { width: 12px; height: 12px; border-radius: 50%; }
.controls { background: white; padding: 12px 20px; border-top: 1px solid #ddd; flex-shrink: 0; }
.control-group { margin-bottom: 8px; }
.control-group:last-child { margin-bottom: 0; }
.control-group label { font-weight: 600; font-size: 0.9em; color: #333; margin-bottom: 4px; display: flex; align-items: center; gap: 8px; }
.control-group label .value { color: #2c6fa0; font-size: 0.95em; }
#date-slider { width: 100%; height: 6px; -webkit-appearance: none; appearance: none; background: #d0d8e0; border-radius: 3px; outline: none; }
#date-slider::-webkit-slider-thumb { -webkit-appearance: none; appearance: none; width: 18px; height: 18px; border-radius: 50%; background: #2c6fa0; cursor: pointer; border: 2px solid white; box-shadow: 0 1px 3px rgba(0,0,0,0.3); }
#date-slider::-moz-range-thumb { width: 18px; height: 18px; border-radius: 50%; background: #2c6fa0; cursor: pointer; border: 2px solid white; }
.time-slider-container { position: relative; }
.time-slider { display: flex; height: 32px; width: 100%; border-radius: 6px; overflow: hidden; box-shadow: inset 0 1px 3px rgba(0,0,0,0.15); }
.time-segment { flex: 1; background: #4a90d9; cursor: pointer; border-right: 1px solid rgba(255,255,255,0.12); transition: filter 0.15s; position: relative; }
.time-segment:hover { filter: brightness(1.2); }
.time-segment.anomalous { background: #f5d300; }
.time-segment.highly-anomalous { background: #e74c3c; }
.time-segment.selected { box-shadow: 0 0 0 3px #1a2937 inset; z-index: 10; }
.time-labels { display: flex; justify-content: space-between; margin-top: 3px; font-size: 0.72em; color: #888; }
.main-content { display: flex; flex: 1; overflow: hidden; }
#map { flex: 1; z-index: 1; }
.side-panel { width: 420px; background: white; border-left: 1px solid #ddd; overflow-y: auto; display: flex; flex-direction: column; }
.panel-section { padding: 15px; border-bottom: 1px solid #eee; }
.panel-section h2 { font-size: 1em; color: #1a2937; margin-bottom: 10px; font-weight: 700; }
.anomaly-alert { background: #fffbe6; border: 1px solid #f5d300; border-radius: 6px; padding: 10px; margin-bottom: 8px; }
.anomaly-alert .loc-name { font-weight: 700; color: #9a7d00; font-size: 0.95em; margin-bottom: 4px; }
.anomaly-alert .stat-row { display: flex; justify-content: space-between; font-size: 0.85em; margin: 2px 0; }
.anomaly-alert .stat-label { color: #777; }
.anomaly-alert .stat-value { font-weight: 600; }
.anomaly-alert .stat-value.highlight { color: #9a7d00; }
.anomaly-alert.high { background: #fdf0f0; border-color: #e74c3c; }
.anomaly-alert.high .loc-name { color: #c0392b; }
.anomaly-alert.high .stat-value.highlight { color: #c0392b; }
.no-data-msg { color: #aaa; font-style: italic; font-size: 0.88em; padding: 5px 0; }
.detail-header { font-weight: 700; font-size: 1.05em; margin-bottom: 6px; color: #1a2937; }
.detail-row { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #f0f0f0; font-size: 0.88em; }
.detail-row .label { color: #666; }
.detail-row .value { font-weight: 600; }
.origin-section-title { font-weight: 600; color: #2c3e50; margin: 10px 0 5px; font-size: 0.88em; }
.origin-bar { display: flex; align-items: center; margin: 3px 0; font-size: 0.82em; }
.origin-bar .origin-rank { width: 18px; color: #999; }
.origin-bar .origin-name { width: 95px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.origin-bar .bar-container { flex: 1; height: 10px; background: #e8e8e8; border-radius: 3px; margin: 0 8px; overflow: hidden; }
.origin-bar .bar-fill { height: 100%; background: #4a90d9; border-radius: 3px; }
.origin-bar .origin-count { width: 45px; text-align: right; font-weight: 600; }
.tower-marker { background: transparent !important; border: none !important; }
.chart-container { margin: 12px 0; padding: 10px; background: #f9f9f9; border-radius: 6px; }
.chart-title { font-weight: 600; color: #2c3e50; margin-bottom: 8px; font-size: 0.88em; }
#time-chart { width: 100%; height: 150px; display: block; background: white; border-radius: 4px; }
.investigate-btn { display: block; width: 100%; padding: 8px 12px; margin-top: 10px; background: #7c3aed; color: white; border: none; border-radius: 6px; font-size: 0.88em; font-weight: 600; cursor: pointer; transition: background 0.15s; }
.investigate-btn:hover { background: #6d28d9; }
.investigate-btn:disabled { background: #aaa; cursor: not-allowed; }
.investigate-result { margin-top: 10px; padding: 10px; background: #f3f0ff; border: 1px solid #d4c5f9; border-radius: 6px; font-size: 0.85em; line-height: 1.4; color: #2c1e5a; }
.weather-time { font-size: 0.78em; color: #2c6fa0; font-weight: 600; margin-bottom: 6px; }
.weather-card { background: #f0f4f8; border-radius: 8px; padding: 8px 10px; margin-bottom: 6px; cursor: pointer; transition: background 0.15s; }
.weather-card:hover { background: #e3f2fd; }
.weather-card.selected { background: #e3f2fd; border: 1px solid #2196f3; }
.weather-card .weather-row { display: flex; align-items: center; justify-content: space-between; }
.weather-card .weather-icon { font-size: 1.6em; }
.weather-card .weather-temp { font-size: 1.15em; font-weight: 700; color: #1a2937; }
.weather-card .weather-label { font-size: 0.82em; color: #555; }
.weather-card .weather-meta { font-size: 0.75em; color: #777; margin-top: 4px; display: flex; gap: 10px; }
</style>
</head>
<body>
<div id="loading">
  <div class="spinner"></div>
  <p>Loading data and computing anomaly statistics...</p>
</div>
<div id="app">
  <div class="header">
    <h1>Vancouver Cell Tower Anomaly Dashboard</h1>
    <div class="legend">
      <div class="legend-item"><div class="legend-dot" style="background:#4a90d9;"></div> Normal</div>
      <div class="legend-item"><div class="legend-dot" style="background:#f5d300;"></div> Anomalous</div>
      <div class="legend-item"><div class="legend-dot" style="background:#e74c3c;"></div> Critical</div>
    </div>
  </div>
  <div class="main-content">
    <div id="map"></div>
    <div class="side-panel">
      <div class="panel-section">
        <h2>Anomaly Alerts</h2>
        <div id="anomaly-content"><div class="no-data-msg">Select a time to check for anomalies</div></div>
        <button id="investigate-btn" class="investigate-btn" style="display:none;">Investigate Spike with AI</button>
        <div id="investigate-result" class="investigate-result" style="display:none;"></div>
      </div>
      <div class="panel-section">
        <h2>Tower Details</h2>
        <div id="details-content"><div class="no-data-msg">Click a cell tower on the map for details</div></div>
      </div>
      <div class="panel-section">
        <h2>Weather Conditions</h2>
        <div id="weather-content"><div class="no-data-msg">Select a date to view weather</div></div>
      </div>
    </div>
  </div>
  <div class="controls">
    <div class="control-group">
      <label>Date: <span class="value" id="date-label">--</span> <span class="value" id="weekday-label" style="color:#888;font-weight:400;"></span></label>
      <input type="range" id="date-slider" min="0" max="0" value="0" step="1">
    </div>
    <div class="control-group">
      <label>Time: <span class="value" id="time-label">--</span></label>
      <div class="time-slider-container">
        <div class="time-slider" id="time-slider"></div>
        <div class="time-labels">
          <span>00:00</span><span>03:00</span><span>06:00</span><span>09:00</span><span>12:00</span><span>15:00</span><span>18:00</span><span>21:00</span><span>23:30</span>
        </div>
      </div>
    </div>
    <div class="control-group">
      <label><input type="checkbox" id="show-arrows" style="margin-right:5px;"> Show origin flow arrows (top 5)</label>
    </div>
    <div class="control-group">
      <label style="display:flex;align-items:center;gap:8px;">
        <span>Metric:</span>
        <select id="metric-mode" style="padding:4px 8px;border:1px solid #ccc;border-radius:4px;font-size:0.9em;">
          <option value="new">New Arrivals</option>
          <option value="cumulative">Cumulative Occupancy</option>
        </select>
      </label>
    </div>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map, markers = {};
var dates = [], stats = {}, locations = [], allCounts = {};
var cumulativeStats = {}, cumulativeCounts = {};
var currentMode = 'new';
var currentDateIdx = 0, currentBucket = 24;
var currentCounts = {};
var lastClickedTower = null;
var detailsRequestId = 0;
var arrowsRequestId = 0;
var arrowLayers = [];
var showArrows = false;
var arrowsDebounce;
var detailsDebounce;
var weatherCache = {};
var weatherRequestId = 0;
var ORIGIN_COORDS = {'West Vancouver':[49.33,-123.165],'North Vancouver':[49.32,-123.07],'Ontario':[43.65,-79.38],'British Columbia Other':[49.28,-123.12],'Burnaby':[49.25,-122.95],'Surrey':[49.19,-122.85],'Downtown':[49.28,-123.12],'West End':[49.28,-123.14],'Richmond':[49.17,-123.13],'Alberta':[51.05,-114.07],'Kitsilano':[49.27,-123.16],'New Westminster':[49.21,-122.91],'Renfrew-Collingwood':[49.24,-123.04],'Mount Pleasant':[49.26,-123.10],'Hastings-Sunrise':[49.28,-123.06],'Arbutus Ridge':[49.25,-123.16],'Atlantic Canada':[44.65,-63.57],'Delta':[49.09,-123.06],'Dunbar-Southlands':[49.24,-123.19],'Fairview':[49.27,-123.13],'Grandview-Woodland':[49.27,-123.07],'International':[49.20,-123.18],'Kensington-Cedar Cottage':[49.24,-123.07],'Killarney':[49.23,-123.03],'Langley':[49.10,-122.66],'Manitoba':[49.90,-97.14],'Maple Ridge':[49.22,-122.60],'Marpole':[49.21,-123.13],'Oakridge':[49.23,-123.11],'Pitt Meadows':[49.22,-122.69],'Port Moody':[49.28,-122.85],'Saskatchewan and Territories':[52.94,-106.61],'Strathcona':[49.28,-123.09],'Sunset':[49.22,-123.09],'UBC':[49.26,-123.25],'Victoria-Fraserview':[49.22,-123.07]};
function clearArrows() { for (var i = 0; i < arrowLayers.length; i++) { map.removeLayer(arrowLayers[i]); } arrowLayers = []; }
function debouncedUpdateArrows() { clearTimeout(arrowsDebounce); arrowsDebounce = setTimeout(function() { updateArrows(); }, 400); }
function debouncedLoadDetails(locName) { clearTimeout(detailsDebounce); detailsDebounce = setTimeout(function() { loadDetails(locName); }, 200); }
async function updateArrows() {
  clearArrows();
  if (!showArrows || !lastClickedTower) return;
  var date = dates[currentDateIdx];
  var bucket = currentBucket;
  var requestId = ++arrowsRequestId;
  var response = await fetch('/api/origins?date=' + date + '&bucket=' + bucket);
  if (requestId !== arrowsRequestId) return;
  var data = await response.json();
  var origins = data.origins;
  var loc = locations.find(function(l) { return l.name === lastClickedTower; });
  if (!loc) return;
  var locOrigins = origins[loc.name] || [];
  var weekday = getWeekday(date);
  var count = (currentCounts[loc.name] || {})[bucket] || 0;
  var level = getAnomalyLevel(loc.name, weekday, bucket, count);
  var color = level === 2 ? '#e74c3c' : (level === 1 ? '#f5d300' : '#4a90d9');
  var maxCount = 1;
  for (var j = 0; j < locOrigins.length; j++) { if (locOrigins[j].count > maxCount) maxCount = locOrigins[j].count; }
  for (var j = 0; j < locOrigins.length; j++) {
    var o = locOrigins[j];
    var coords = ORIGIN_COORDS[o.origin];
    if (!coords) continue;
    var width = 1 + (o.count / maxCount) * 5;
    var line = L.polyline([coords, [loc.lat, loc.lng]], {color: color, weight: width, opacity: 0.7, dashArray: '5,5'}).addTo(map);
    line.bindTooltip(o.origin + ': ' + o.count, {sticky: true});
    arrowLayers.push(line);
    var dot = L.circleMarker(coords, {radius: 4, color: color, fillColor: color, fillOpacity: 0.8}).addTo(map);
    dot.bindTooltip(o.origin + ': ' + o.count, {sticky: true});
    arrowLayers.push(dot);
  }
}

async function investigateSpike() {
  if (!lastClickedTower) return;
  var btn = document.getElementById('investigate-btn');
  var result = document.getElementById('investigate-result');
  btn.disabled = true;
  btn.textContent = 'Investigating...';
  result.style.display = 'block';
  result.innerHTML = '<em>Searching for possible explanations...</em>';
  try {
    var date = dates[currentDateIdx];
    var response = await fetch('/api/investigate?location=' + encodeURIComponent(lastClickedTower) + '&date=' + date + '&bucket=' + currentBucket + '&mode=' + currentMode);
    var data = await response.json();
    if (data.error) {
      result.innerHTML = '<strong>Error:</strong> ' + data.error;
    } else {
      result.innerHTML = '<strong>AI Analysis</strong> (anomaly lasted ' + data.duration + '):<br><br>' + data.explanation;
    }
  } catch (err) {
    result.innerHTML = '<strong>Error:</strong> ' + err.message;
  }
  btn.disabled = false;
  btn.textContent = 'Investigate Spike with AI';
}

async function loadWeatherForDate(date) {
  var allCached = locations.every(function(l) { return weatherCache[l.name + '_' + date]; });
  if (allCached) { updateWeatherPanel(); return; }
  var requestId = ++weatherRequestId;
  document.getElementById('weather-content').innerHTML = '<div class="no-data-msg">Loading weather...</div>';
  for (var i = 0; i < locations.length; i++) {
    var loc = locations[i];
    var cacheKey = loc.name + '_' + date;
    if (weatherCache[cacheKey]) continue;
    try {
      var resp = await fetch('/api/weather?lat=' + loc.lat + '&lng=' + loc.lng + '&date=' + date);
      var data = await resp.json();
      if (requestId !== weatherRequestId) return;
      if (data.error) { weatherCache[cacheKey] = { error: data.error }; }
      else { weatherCache[cacheKey] = data; }
    } catch (e) {
      if (requestId !== weatherRequestId) return;
      weatherCache[cacheKey] = { error: e.message };
    }
  }
  if (requestId === weatherRequestId) updateWeatherPanel();
}

function updateWeatherPanel() {
  if (!dates.length) return;
  var date = dates[currentDateIdx];
  var h = Math.floor(currentBucket / 2);
  var m = (currentBucket % 2) * 30;
  var timeStr = String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
  var hourKey = String(h).padStart(2, '0') + ':00';
  var content = document.getElementById('weather-content');
  if (!content) return;
  var html = '<div class="weather-time">Weather at ' + timeStr + '</div>';
  for (var i = 0; i < locations.length; i++) {
    var loc = locations[i];
    var cacheKey = loc.name + '_' + date;
    var w = weatherCache[cacheKey];
    if (!w) { html += '<div class="weather-card"><div class="weather-label">' + loc.name + '</div><div class="no-data-msg">Loading...</div></div>'; continue; }
    if (w.error) { html += '<div class="weather-card"><div class="weather-label">' + loc.name + '</div><div class="no-data-msg">Weather unavailable</div></div>'; continue; }
    var hourly = w.hourly || [];
    var entry = null;
    for (var j = 0; j < hourly.length; j++) { if (hourly[j].hour === hourKey) { entry = hourly[j]; break; } }
    if (!entry && hourly.length > 0) { entry = hourly[Math.min(h, hourly.length - 1)]; }
    if (!entry) { html += '<div class="weather-card"><div class="weather-label">' + loc.name + '</div><div class="no-data-msg">No data</div></div>'; continue; }
    var isSelected = lastClickedTower === loc.name;
    html += '<div class="weather-card' + (isSelected ? ' selected' : '') + '" data-loc="' + loc.name + '">' +
      '<div class="weather-row"><div><div class="weather-label">' + loc.name + '</div></div>' +
      '<div class="weather-icon">' + entry.weather_icon + '</div>' +
      '<div class="weather-temp">' + (entry.temp !== null ? entry.temp + '\u00b0C' : '--') + '</div></div>' +
      '<div class="weather-meta"><span>' + entry.weather_label + '</span>' +
      '<span>\u{1f4a7} ' + entry.precip + 'mm</span>' +
      '<span>\u{1f4a8} ' + (entry.wind !== null ? entry.wind + 'km/h' : '--') + '</span></div></div>';
  }
  content.innerHTML = html;
  document.querySelectorAll('.weather-card[data-loc]').forEach(function(card) {
    card.addEventListener('click', function() {
      var name = this.dataset.loc;
      if (markers[name]) markers[name].fire('click');
    });
  });
}

async function init() {
  try {
    var response = await fetch('/api/init');
    var data = await response.json();
    if (data.error) throw new Error(data.error);
    locations = data.locations;
    dates = data.dates;
    stats = data.stats;
    allCounts = data.all_counts;
    cumulativeStats = data.cumulative_stats;
    cumulativeCounts = data.cumulative_counts;
    setupDateSlider();
    setupTimeSlider();
    setupMap();
    loadDateData();
    document.getElementById('loading').style.display = 'none';
    document.getElementById('app').style.display = 'flex';
    document.getElementById('show-arrows').addEventListener('change', function() {
      showArrows = this.checked;
      updateArrows();
    });
    document.getElementById('metric-mode').addEventListener('change', function() {
      currentMode = this.value;
      loadDateData();
    });
    document.getElementById('investigate-btn').addEventListener('click', investigateSpike);
    setTimeout(function() { map.invalidateSize(); }, 100);
  } catch (err) {
    document.getElementById('loading').innerHTML = '<p style="color:red;">Error: ' + err.message + '</p>';
  }
}

function setupDateSlider() {
  var slider = document.getElementById('date-slider');
  slider.max = dates.length - 1;
  slider.value = 0;
  currentDateIdx = 0;
  updateDateLabel();
  slider.addEventListener('input', function() {
    currentDateIdx = parseInt(slider.value);
    updateDateLabel();
    loadDateData();
  });
}

function updateDateLabel() {
  var dateStr = dates[currentDateIdx];
  document.getElementById('date-label').textContent = dateStr;
  var parts = dateStr.split('-');
  var date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
  var days = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  document.getElementById('weekday-label').textContent = days[date.getDay()];
}

function setupTimeSlider() {
  var slider = document.getElementById('time-slider');
  slider.innerHTML = '';
  for (var i = 0; i < 48; i++) {
    var seg = document.createElement('div');
    seg.className = 'time-segment';
    seg.dataset.bucket = i;
    if (i === currentBucket) seg.classList.add('selected');
    seg.addEventListener('click', function() {
      currentBucket = parseInt(this.dataset.bucket);
      document.querySelectorAll('.time-segment').forEach(function(s) { s.classList.remove('selected'); });
      this.classList.add('selected');
      updateTimeLabel();
      updateMap();
      updateAnomalyPanel();
      updateWeatherPanel();
      if (lastClickedTower) {
        debouncedLoadDetails(lastClickedTower);
      }
      debouncedUpdateArrows();
    });
    slider.appendChild(seg);
  }
  updateTimeLabel();
}

function updateTimeLabel() {
  var h = Math.floor(currentBucket / 2);
  var m = (currentBucket % 2) * 30;
  document.getElementById('time-label').textContent = String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
}

function setupMap() {
  map = L.map('map').setView([49.28, -123.16], 12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'OpenStreetMap', maxZoom: 18
  }).addTo(map);
  for (var i = 0; i < locations.length; i++) {
    var loc = locations[i];
    var marker = L.marker([loc.lat, loc.lng]).addTo(map);
    marker.on('click', function(name) { return function() { 
      if (lastClickedTower === name) {
        lastClickedTower = null;
        document.getElementById('details-content').innerHTML = '<div class="no-data-msg">Click a tower to view details</div>';
        clearArrows();
      } else {
        loadDetails(name);
      }
      updateTimeSliderHighlights();
      updateAnomalyPanel();
      updateWeatherPanel();
    }; }(loc.name));
    markers[loc.name] = marker;
  }
}

function getWeekday(dateStr) {
  var parts = dateStr.split('-');
  var date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
  return date.getDay() + 1;
}

function getAnomalyLevel(locName, weekday, bucket, count) {
  var currentStats = currentMode === 'cumulative' ? cumulativeStats : stats;
  var stat = currentStats[locName] && currentStats[locName][weekday] && currentStats[locName][weekday][bucket];
  if (!stat) return 0;
  if (count > stat.avg + 3 * stat.std) return 2;
  if (count > stat.avg + 2 * stat.std) return 1;
  return 0;
}

function isAnomalous(locName, weekday, bucket, count) {
  return getAnomalyLevel(locName, weekday, bucket, count) > 0;
}

function getMaxCount() {
  var max = 1;
  for (var i = 0; i < locations.length; i++) {
    var counts = currentCounts[locations[i].name] || {};
    for (var b in counts) {
      if (counts[b] > max) max = counts[b];
    }
  }
  return max;
}

function loadDateData() {
  var date = dates[currentDateIdx];
  var sourceCounts = currentMode === 'cumulative' ? cumulativeCounts : allCounts;
  currentCounts = {};
  for (var i = 0; i < locations.length; i++) {
    var locName = locations[i].name;
    var dateArr = (sourceCounts[locName] || {})[date];
    if (dateArr) {
      currentCounts[locName] = {};
      for (var b = 0; b < 48; b++) { currentCounts[locName][b] = dateArr[b]; }
    }
  }
  updateTimeSliderHighlights();
  updateMap();
  updateAnomalyPanel();
  updateWeatherPanel();
  loadWeatherForDate(date);
  if (lastClickedTower) debouncedLoadDetails(lastClickedTower);
  debouncedUpdateArrows();
}

function updateTimeSliderHighlights() {
  var weekday = getWeekday(dates[currentDateIdx]);
  var segments = document.querySelectorAll('.time-segment');
  segments.forEach(function(seg, i) {
    seg.classList.remove('anomalous', 'highly-anomalous');
    var maxLevel = 0;
    if (lastClickedTower) {
      var count = (currentCounts[lastClickedTower] || {})[i] || 0;
      if (count > 0) {
        maxLevel = getAnomalyLevel(lastClickedTower, weekday, i, count);
      }
    } else {
      for (var j = 0; j < locations.length; j++) {
        var count = (currentCounts[locations[j].name] || {})[i] || 0;
        if (count > 0) {
          var level = getAnomalyLevel(locations[j].name, weekday, i, count);
          if (level > maxLevel) maxLevel = level;
        }
      }
    }
    if (maxLevel === 2) seg.classList.add('highly-anomalous');
    else if (maxLevel === 1) seg.classList.add('anomalous');
  });
}

function updateMap() {
  var weekday = getWeekday(dates[currentDateIdx]);
  var maxCount = getMaxCount();
  for (var i = 0; i < locations.length; i++) {
    var loc = locations[i];
    var count = (currentCounts[loc.name] || {})[currentBucket] || 0;
    var level = getAnomalyLevel(loc.name, weekday, currentBucket, count);
    var marker = markers[loc.name];
    var minSize = 25, maxSize = 70;
    var ratio = maxCount > 0 ? count / maxCount : 0;
    var size = Math.max(minSize, minSize + ratio * (maxSize - minSize));
    var color, stroke;
    if (level === 2) { color = '#e74c3c'; stroke = '#c0392b'; }
    else if (level === 1) { color = '#f5d300'; stroke = '#9a7d00'; }
    else { color = '#4a90d9'; stroke = '#2c5f8a'; }
    var svg = '<div style="text-align:center;">' +
      '<svg viewBox="0 0 40 50" width="' + size + '" height="' + (size * 1.25) + '">' +
      '<circle cx="20" cy="6" r="5" fill="' + color + '" stroke="' + stroke + '" stroke-width="1.5"/>' +
      '<path d="M16 4 A6 6 0 0 1 24 4" fill="none" stroke="' + stroke + '" stroke-width="1.5"/>' +
      '<path d="M13 2 A10 10 0 0 1 27 2" fill="none" stroke="' + stroke + '" stroke-width="1.5"/>' +
      '<path d="M20 11 L10 38 L30 38 Z" fill="' + color + '" stroke="' + stroke + '" stroke-width="1.5" opacity="0.85"/>' +
      '<line x1="14" y1="30" x2="26" y2="30" stroke="' + stroke + '" stroke-width="1"/>' +
      '<line x1="12" y1="34" x2="28" y2="34" stroke="' + stroke + '" stroke-width="1"/>' +
      '<line x1="10" y1="38" x2="10" y2="44" stroke="' + stroke + '" stroke-width="2"/>' +
      '<line x1="30" y1="38" x2="30" y2="44" stroke="' + stroke + '" stroke-width="2"/>' +
      '</svg>' +
      '<div style="font-size:0.7em;font-weight:700;background:white;padding:1px 5px;border-radius:3px;border:1px solid #ccc;display:inline-block;margin-top:-2px;">' +
      count.toLocaleString() + '</div></div>';
    marker.setIcon(L.divIcon({
      html: svg, className: 'tower-marker',
      iconSize: [size, size * 1.4],
      iconAnchor: [size / 2, size * 1.35]
    }));
  }
}

function updateAnomalyPanel() {
  var weekday = getWeekday(dates[currentDateIdx]);
  var content = document.getElementById('anomaly-content');
  var html = '';
  var any = false;
  var locsToCheck = lastClickedTower ? [locations.find(function(l) { return l.name === lastClickedTower; })] : locations;
  for (var i = 0; i < locsToCheck.length; i++) {
    var loc = locsToCheck[i];
    if (!loc) continue;
    var count = (currentCounts[loc.name] || {})[currentBucket] || 0;
    var level = getAnomalyLevel(loc.name, weekday, currentBucket, count);
    if (count > 0 && level > 0) {
      var stat = stats[loc.name][weekday][currentBucket];
      any = true;
      var cls = level === 2 ? 'anomaly-alert high' : 'anomaly-alert';
      var icon = level === 2 ? '!! ' : '! ';
      html += '<div class="' + cls + '">' +
        '<div class="loc-name">' + icon + loc.name + '</div>' +
        '<div class="stat-row"><span class="stat-label">Expected (avg):</span><span class="stat-value">' + Math.round(stat.avg).toLocaleString() + '</span></div>' +
        '<div class="stat-row"><span class="stat-label">Actual:</span><span class="stat-value highlight">' + count.toLocaleString() + '</span></div>' +
        '</div>';
    }
  }
  content.innerHTML = any ? html : '<div class="no-data-msg">No anomalies at this time slot</div>';
  
  // Show/hide investigate button
  var invBtn = document.getElementById('investigate-btn');
  var invResult = document.getElementById('investigate-result');
  if (invBtn) {
    var showBtn = false;
    if (lastClickedTower) {
      var towerCount = (currentCounts[lastClickedTower] || {})[currentBucket] || 0;
      var towerLevel = getAnomalyLevel(lastClickedTower, weekday, currentBucket, towerCount);
      if (towerCount > 0 && towerLevel > 0) showBtn = true;
    }
    invBtn.style.display = showBtn ? 'block' : 'none';
    if (!showBtn && invResult) invResult.style.display = 'none';
  }
}

async function loadDetails(locName) {
  lastClickedTower = locName;
  debouncedUpdateArrows();
  var date = dates[currentDateIdx];
  var bucket = currentBucket;
  var requestId = ++detailsRequestId;
  var content = document.getElementById('details-content');
  content.innerHTML = '<div class="no-data-msg">Loading...</div>';
  try {
    var response = await fetch('/api/details?location=' + encodeURIComponent(locName) + '&date=' + date + '&bucket=' + bucket + '&mode=' + currentMode);
    if (requestId !== detailsRequestId) return;
    var data = await response.json();
    if (data.total_count === 0) {
      content.innerHTML = '<div class="no-data-msg">No visitors at this time slot</div>';
      return;
    }
    var maxOrigin = 1;
    for (var i = 0; i < data.origins.length; i++) {
      if (data.origins[i].count > maxOrigin) maxOrigin = data.origins[i].count;
    }
    var modeLabel = currentMode === 'cumulative' ? 'People Present' : 'New Arrivals';
    var chartTitle = currentMode === 'cumulative' ? 'Cumulative Occupancy Over Time' : 'New Arrivals Over Time';
    var html = '<div class="detail-header">' + locName + '</div>' +
      '<div class="detail-row"><span class="label">' + modeLabel + ':</span><span class="value">' + data.total_count.toLocaleString() + '</span></div>' +
      '<div class="detail-row"><span class="label">Avg Dwell Time:</span><span class="value">' + data.avg_dwell.toFixed(1) + ' min</span></div>' +
      '<div class="chart-container"><div class="chart-title">' + chartTitle + '</div><canvas id="time-chart"></canvas></div>' +
      '<div class="origin-section-title">Top 10 Origins</div>';
    for (var i = 0; i < data.origins.length; i++) {
      var o = data.origins[i];
      var pct = (o.count / maxOrigin) * 100;
      html += '<div class="origin-bar">' +
        '<span class="origin-rank">' + (i+1) + '</span>' +
        '<span class="origin-name" title="' + o.origin + '">' + o.origin + '</span>' +
        '<div class="bar-container"><div class="bar-fill" style="width:' + pct + '%;"></div></div>' +
        '<span class="origin-count">' + o.count.toLocaleString() + '</span>' +
        '</div>';
    }
    content.innerHTML = html;
    drawTimeChart(locName);
  } catch (err) {
    content.innerHTML = '<div class="no-data-msg">Error loading details</div>';
  }
}

function drawTimeChart(locName) {
  var canvas = document.getElementById('time-chart');
  if (!canvas) return;
  var ctx = canvas.getContext('2d');
  var width = canvas.offsetWidth;
  var height = canvas.offsetHeight;
  canvas.width = width;
  canvas.height = height;
  
  var counts = currentCounts[locName] || {};
  var values = [];
  var maxVal = 1;
  for (var i = 0; i < 48; i++) {
    var val = counts[i] || 0;
    values.push(val);
    if (val > maxVal) maxVal = val;
  }
  
  var padding = { top: 10, right: 10, bottom: 20, left: 40 };
  var chartWidth = width - padding.left - padding.right;
  var chartHeight = height - padding.top - padding.bottom;
  
  ctx.clearRect(0, 0, width, height);
  
  ctx.strokeStyle = '#e0e0e0';
  ctx.lineWidth = 1;
  for (var i = 0; i <= 4; i++) {
    var y = padding.top + (chartHeight / 4) * i;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }
  
  ctx.fillStyle = '#666';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'right';
  for (var i = 0; i <= 4; i++) {
    var val = Math.round(maxVal * (1 - i / 4));
    var y = padding.top + (chartHeight / 4) * i;
    ctx.fillText(val.toString(), padding.left - 5, y + 3);
  }
  
  ctx.textAlign = 'center';
  for (var i = 0; i <= 24; i += 6) {
    var x = padding.left + (chartWidth / 24) * i;
    var label = String(i).padStart(2, '0') + ':00';
    ctx.fillText(label, x, height - 5);
  }
  
  ctx.strokeStyle = '#4a90d9';
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (var i = 0; i < 48; i++) {
    var x = padding.left + (chartWidth / 47) * i;
    var y = padding.top + chartHeight - (values[i] / maxVal) * chartHeight;
    if (i === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.stroke();
  
  var currentX = padding.left + (chartWidth / 47) * currentBucket;
  var currentY = padding.top + chartHeight - (values[currentBucket] / maxVal) * chartHeight;
  
  ctx.strokeStyle = '#1a2937';
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 3]);
  ctx.beginPath();
  ctx.moveTo(currentX, padding.top);
  ctx.lineTo(currentX, height - padding.bottom);
  ctx.stroke();
  ctx.setLineDash([]);
  
  ctx.fillStyle = '#e74c3c';
  ctx.beginPath();
  ctx.arc(currentX, currentY, 4, 0, 2 * Math.PI);
  ctx.fill();
  ctx.strokeStyle = 'white';
  ctx.lineWidth = 2;
  ctx.stroke();
}

init();
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
