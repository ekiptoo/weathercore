#!/usr/bin/env python3
"""
WeatherCore — Flask REST API Server
Phase 3: Interface Integration

Serves all JSON endpoints consumed by the frontend dashboard.
Integrates with the backend scheduler so ingestion runs in the same process.

Run:
    python app.py
    → http://localhost:5000

Endpoints:
    GET  /api/stations                    — all active stations
    GET  /api/current                     — latest reading per station
    GET  /api/station/<id>/history        — ?hours=24
    GET  /api/station/<id>/summary        — ?days=30
    GET  /api/station/<id>/info           — station metadata
    GET  /api/alerts/active               — unresolved alerts
    POST /api/alerts/<id>/resolve         — mark alert resolved
    GET  /api/system/stats                — obs count, api health, jobs
    GET  /api/system/jobs                 — scheduler job info
    POST /api/system/fetch-now            — trigger immediate ingestion
    GET  /                                — serves frontend/index.html
"""

import os, json, atexit, logging
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

from backend import create_components

components = None

def get_components():
    global components
    if components is None:
        components = create_components()
    return components

# ─── App setup ────────────────────────────────────────────────────────────
log = logging.getLogger("weathercore.api")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "frontend")

app = Flask(__name__, static_folder=FRONTEND_DIR)
CORS(app, resources={r"/api/*": {"origins": "*"}})


# ─── Initialise backend components ────────────────────────────────────────
db, fetcher, alert_engine, summariser, scheduler = get_components()
scheduler.start(run_immediately=True)
atexit.register(scheduler.stop)


# ─── Utility: JSON serialiser for MySQL types ─────────────────────────────
def _serial(obj):
    """
    MySQL connector returns several non-JSON-native types.
    Handle all of them here so no endpoint ever crashes on serialisation.
      decimal.Decimal  → float   (DECIMAL/NUMERIC columns: lat, lon, temp…)
      datetime         → ISO str (DATETIME columns)
      date             → ISO str (DATE columns)
      timedelta        → total seconds (TIME columns, rare)
      bytes / bytearray→ hex str (BLOB columns, rare)
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, 'total_seconds'):        # timedelta
        return obj.total_seconds()
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    raise TypeError(f"Type {type(obj)} not serialisable when encoding {obj!r}")


def jsonify_rows(rows):
    """Serialise MySQL result dicts safely, handling all MySQL column types."""
    return app.response_class(
        json.dumps(rows, default=_serial),
        mimetype="application/json"
    )


# ═══════════════════════════════════════════════════════════════════════════
# STATIC — serve frontend
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory(FRONTEND_DIR, path)


# ═══════════════════════════════════════════════════════════════════════════
# STATIONS
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/stations")
def get_stations():
    """
    GET /api/stations
    Returns all active stations for map markers and dropdown menus.
    Response: [{station_id, station_code, name, city, country, lat, lon, elevation_m}]
    """
    stations = db.get_active_stations()
    return jsonify_rows(stations)


@app.route("/api/station/<int:station_id>/info")
def get_station_info(station_id):
    """
    GET /api/station/<id>/info
    Returns full metadata for one station.
    """
    station = db.get_station(station_id)
    if not station:
        abort(404, description=f"Station {station_id} not found")
    return jsonify_rows(station)


# ═══════════════════════════════════════════════════════════════════════════
# CURRENT CONDITIONS
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/current")
def get_current():
    """
    GET /api/current
    Returns latest observation for every active station.
    Used by dashboard hero cards and map tooltip popups.
    Response: [{station_id, station_name, city, country, lat, lon,
                temperature_c, humidity_pct, wind_speed_ms, ...}]
    """
    rows = db.get_latest_for_all_stations()
    return jsonify_rows(rows)


# ═══════════════════════════════════════════════════════════════════════════
# HISTORY
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/station/<int:station_id>/history")
def get_history(station_id):
    """
    GET /api/station/<id>/history?hours=24
    Returns time-series observations for charting.
    Query params:
      hours (int, default=24, max=168)
    Response: [{observed_at, temperature_c, humidity_pct,
                pressure_hpa, wind_speed_ms, precipitation_mm, ...}]
    """
    hours = min(int(request.args.get("hours", 24)), 168)  # cap at 7 days
    rows  = db.get_recent_observations(station_id, hours)
    if not rows:
        return jsonify_rows([])
    return jsonify_rows(rows)


# ═══════════════════════════════════════════════════════════════════════════
# DAILY SUMMARIES
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/station/<int:station_id>/summary")
def get_summary(station_id):
    """
    GET /api/station/<id>/summary?days=30
    Returns pre-aggregated daily summaries for trend charts.
    Query params:
      days (int, default=30, max=365)
    Response: [{summary_date, temp_min_c, temp_max_c, temp_avg_c,
                precip_total_mm, wind_max_ms, ...}]
    """
    days = min(int(request.args.get("days", 30)), 365)
    rows = db.get_daily_summaries(station_id, days)
    return jsonify_rows(rows)


# ═══════════════════════════════════════════════════════════════════════════
# ALERTS
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/alerts/active")
def get_active_alerts():
    """
    GET /api/alerts/active
    Returns all unresolved alerts, ordered by severity then time.
    Response: [{alert_id, alert_type, severity, message, triggered_at,
                station_name, city, country}]
    """
    alerts = db.get_active_alerts()
    return jsonify_rows(alerts)


@app.route("/api/alerts/<int:alert_id>/resolve", methods=["POST"])
def resolve_alert(alert_id):
    """
    POST /api/alerts/<id>/resolve
    Marks an alert as resolved (sets resolved_at = NOW()).
    Response: {success: bool, alert_id: int}
    """
    success = db.resolve_alert(alert_id)
    return jsonify({"success": success, "alert_id": alert_id})


# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM
# ═══════════════════════════════════════════════════════════════════════════
@app.route("/api/system/stats")
def get_stats():
    """
    GET /api/system/stats
    System-level statistics for the admin panel.
    Response: {station_count, observation_count, active_alerts,
               api_health: [...], scheduler_jobs: [...]}
    """
    stations   = db.get_active_stations()
    obs_count  = db.get_observation_count()
    alerts     = db.get_active_alerts()
    api_health = db.get_api_health()
    jobs       = scheduler.get_jobs()

    return jsonify({
        "station_count":    len(stations),
        "observation_count": obs_count,
        "active_alert_count": len(alerts),
        "api_health":       api_health,
        "scheduler_jobs":   jobs,
        "server_time_utc":  datetime.utcnow().isoformat(),
    })


@app.route("/api/system/jobs")
def get_jobs():
    """GET /api/system/jobs — Scheduler job status."""
    return jsonify(scheduler.get_jobs())


@app.route("/api/system/fetch-now", methods=["POST"])
def fetch_now():
    """
    POST /api/system/fetch-now
    Triggers an immediate ingestion cycle outside the schedule.
    Useful for manual refresh from the dashboard.
    """
    try:
        results = fetcher.ingest_all()
        inserted = sum(r.get("inserted", 0) for r in results)
        return jsonify({"success": True, "inserted": inserted, "detail": results})
    except Exception as e:
        log.error(f"/fetch-now error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# ERROR HANDLERS
# ═══════════════════════════════════════════════════════════════════════════
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": str(e)}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=os.getenv("FLASK_ENV") == "development",
        use_reloader=False,   # MUST be False — APScheduler + reloader = double jobs
    )