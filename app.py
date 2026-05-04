#!/usr/bin/env python3
"""
WeatherCore — Flask REST API Server (Production Safe Version)
Fixes:
- No DB connection at import time
- Safe lazy initialization
- Railway-compatible startup
"""

import os
import json
import atexit
import logging
from datetime import datetime, date
from decimal import Decimal

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

from backend import create_components


# ─────────────────────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────────────────────
log = logging.getLogger("weathercore.api")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "frontend")

app = Flask(__name__, static_folder=FRONTEND_DIR)
CORS(app, resources={r"/api/*": {"origins": "*"}})


# ─────────────────────────────────────────────────────────────
# LAZY BACKEND INITIALIZATION (IMPORTANT FIX)
# ─────────────────────────────────────────────────────────────
components = None

def get_components():
    """
    Creates backend components ONLY when first needed.
    Prevents Railway startup crash due to DB connection at import time.
    """
    global components
    if components is None:
        components = create_components()
    return components


# ─────────────────────────────────────────────────────────────
# SAFE SCHEDULER START (DO NOT RUN AT IMPORT TIME)
# ─────────────────────────────────────────────────────────────
@app.before_request
def start_scheduler_once():
    """
    Ensures scheduler starts only once after app is ready.
    """
    if not hasattr(app, "scheduler_started"):
        db, fetcher, alert_engine, summariser, scheduler = get_components()
        scheduler.start(run_immediately=True)
        app.scheduler_started = True


# ─────────────────────────────────────────────────────────────
# JSON SERIALISER (MySQL safe types)
# ─────────────────────────────────────────────────────────────
def _serial(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "total_seconds"):
        return obj.total_seconds()
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    raise TypeError(f"Type {type(obj)} not serialisable")


def jsonify_rows(rows):
    return app.response_class(
        json.dumps(rows, default=_serial),
        mimetype="application/json"
    )


# ─────────────────────────────────────────────────────────────
# STATIC FILES
# ─────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory(FRONTEND_DIR, path)


# ─────────────────────────────────────────────────────────────
# STATIONS
# ─────────────────────────────────────────────────────────────
@app.route("/api/stations")
def get_stations():
    db, *_ = get_components()
    return jsonify_rows(db.get_active_stations())


@app.route("/api/station/<int:station_id>/info")
def get_station_info(station_id):
    db, *_ = get_components()
    station = db.get_station(station_id)
    if not station:
        abort(404, description="Station not found")
    return jsonify_rows(station)


# ─────────────────────────────────────────────────────────────
# CURRENT DATA
# ─────────────────────────────────────────────────────────────
@app.route("/api/current")
def get_current():
    db, *_ = get_components()
    return jsonify_rows(db.get_latest_for_all_stations())


# ─────────────────────────────────────────────────────────────
# HISTORY
# ─────────────────────────────────────────────────────────────
@app.route("/api/station/<int:station_id>/history")
def get_history(station_id):
    db, *_ = get_components()
    hours = min(int(request.args.get("hours", 24)), 168)
    return jsonify_rows(db.get_recent_observations(station_id, hours))


# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
@app.route("/api/station/<int:station_id>/summary")
def get_summary(station_id):
    db, *_ = get_components()
    days = min(int(request.args.get("days", 30)), 365)
    return jsonify_rows(db.get_daily_summaries(station_id, days))


# ─────────────────────────────────────────────────────────────
# ALERTS
# ─────────────────────────────────────────────────────────────
@app.route("/api/alerts/active")
def get_alerts():
    db, *_ = get_components()
    return jsonify_rows(db.get_active_alerts())


@app.route("/api/alerts/<int:alert_id>/resolve", methods=["POST"])
def resolve_alert(alert_id):
    db, *_ = get_components()
    return jsonify({
        "success": db.resolve_alert(alert_id),
        "alert_id": alert_id
    })


# ─────────────────────────────────────────────────────────────
# SYSTEM
# ─────────────────────────────────────────────────────────────
@app.route("/api/system/stats")
def system_stats():
    db, fetcher, alert_engine, summariser, scheduler = get_components()

    return jsonify({
        "station_count": len(db.get_active_stations()),
        "observation_count": db.get_observation_count(),
        "active_alert_count": len(db.get_active_alerts()),
        "api_health": db.get_api_health(),
        "scheduler_jobs": scheduler.get_jobs(),
        "server_time_utc": datetime.utcnow().isoformat(),
    })


@app.route("/api/system/jobs")
def system_jobs():
    _, _, _, _, scheduler = get_components()
    return jsonify(scheduler.get_jobs())


@app.route("/api/system/fetch-now", methods=["POST"])
def fetch_now():
    _, fetcher, *_ = get_components()
    try:
        results = fetcher.ingest_all()
        inserted = sum(r.get("inserted", 0) for r in results)
        return jsonify({"success": True, "inserted": inserted})
    except Exception as e:
        log.error(f"fetch-now error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────
# ERROR HANDLERS
# ─────────────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": str(e)}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500


# ─────────────────────────────────────────────────────────────
# ENTRY POINT (LOCAL ONLY)
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=os.getenv("FLASK_ENV") == "development",
        use_reloader=False
    )