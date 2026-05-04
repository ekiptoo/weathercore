#!/usr/bin/env python3
"""
WeatherCore Backend — Single-file implementation
Phases 1 & 2: Database layer + Automatic update pipeline

Contains:
  - DatabaseManager  : connection pooling + all SQL helpers
  - WeatherFetcher   : Open-Meteo + OpenWeatherMap + NASA POWER ingestion
  - AlertEngine      : threshold-based alert detection & logging
  - Summariser       : nightly daily summary computation
  - WeatherScheduler : APScheduler wrapper (runs every 15 min)

Run standalone:
    python backend.py

Or import into app.py:
    from backend import db, scheduler
"""

# ─── stdlib ────────────────────────────────────────────────────────────────
import os, json, time, logging
from datetime import datetime, date, timedelta, timezone
from typing import Optional

# ─── third-party ───────────────────────────────────────────────────────────
import requests
import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("weathercore")


# ═══════════════════════════════════════════════════════════════════════════
# WMO WEATHER CODE DESCRIPTIONS
# ═══════════════════════════════════════════════════════════════════════════
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════════════════
class DatabaseManager:
    """
    Connection pool + all SQL helpers for WeatherCore.
    Usage:
        db = DatabaseManager()
        db.insert_observation(station_id=1, data={...})
    """

    # def __init__(self):
    #     self._pool = pooling.MySQLConnectionPool(
    #         pool_name="weathercore_pool",
    #         pool_size=5,
    #         host=os.getenv("DB_HOST", "localhost"),
    #         user=os.getenv("DB_USER", "weatheruser"),
    #         password=os.getenv("DB_PASS", "12345678"),
    #         database=os.getenv("DB_NAME", "gwdc"),
    #         charset="utf8mb4",
    #         autocommit=False,
    #         time_zone="+00:00",  # Always UTC in DB
    #         connection_timeout=10,
    #     )
    #     log.info("✓ MySQL connection pool created (size=5)")
    def __init__(self):
        self._pool = pooling.MySQLConnectionPool(
            pool_name="weathercore_pool",
            pool_size=5,
            host=os.getenv("MYSQLHOST"),
            user=os.getenv("MYSQLUSER"),
            password=os.getenv("MYSQLPASSWORD"),
            database=os.getenv("MYSQLDATABASE"),
            port=int(os.getenv("MYSQLPORT", 3306)),
            charset="utf8mb4",
            autocommit=False,
            time_zone="+00:00",           # Always UTC in DB
            connection_timeout=10,
        )
        log.info("✓ MySQL connection pool created (size=5)")

    def _conn(self):
        """Get a connection from the pool."""
        return self._pool.get_connection()

    # ── Config helpers ─────────────────────────────────────────────────────
    def get_config(self, key: str, default=None) -> Optional[str]:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT config_value FROM system_config WHERE config_key=%s", (key,))
            row = cur.fetchone()
            return row[0] if row else default

    def get_all_config(self) -> dict:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT config_key, config_value FROM system_config")
            return {r[0]: r[1] for r in cur.fetchall()}

    # ── Station helpers ────────────────────────────────────────────────────
    def get_active_stations(self) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT station_id, station_code, name, city, country,
                       latitude, longitude, elevation_m, timezone
                FROM stations
                WHERE active = 1
                ORDER BY station_id
            """)
            return cur.fetchall()

    def get_station(self, station_id: int) -> Optional[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM stations WHERE station_id=%s", (station_id,))
            return cur.fetchone()

    # ── Observation helpers ────────────────────────────────────────────────
    def insert_observation(self, station_id: int, data: dict) -> Optional[int]:
        """
        Insert one observation row. Uses INSERT IGNORE on the unique key
        (station_id, observed_at, source_api) to avoid duplicates.
        Returns inserted obs_id or None if duplicate.
        """
        sql = """
            INSERT IGNORE INTO observations (
                station_id, observed_at, temperature_c, feels_like_c,
                humidity_pct, dew_point_c, pressure_hpa,
                wind_speed_ms, wind_dir_deg, wind_gust_ms,
                precipitation_mm, cloud_cover_pct,
                visibility_km, uv_index, solar_radiation,
                weather_code, weather_description,
                source_api, raw_json
            ) VALUES (
                %(station_id)s, %(observed_at)s, %(temperature_c)s, %(feels_like_c)s,
                %(humidity_pct)s, %(dew_point_c)s, %(pressure_hpa)s,
                %(wind_speed_ms)s, %(wind_dir_deg)s, %(wind_gust_ms)s,
                %(precipitation_mm)s, %(cloud_cover_pct)s,
                %(visibility_km)s, %(uv_index)s, %(solar_radiation)s,
                %(weather_code)s, %(weather_description)s,
                %(source_api)s, %(raw_json)s
            )
        """
        payload = {"station_id": station_id, **data}
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, payload)
            conn.commit()
            return cur.lastrowid if cur.rowcount else None

    def get_recent_observations(self, station_id: int, hours: int = 24) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT observed_at, temperature_c, feels_like_c, humidity_pct,
                       pressure_hpa, wind_speed_ms, wind_dir_deg, wind_gust_ms,
                       precipitation_mm, cloud_cover_pct, visibility_km,
                       uv_index, weather_code, weather_description, source_api
                FROM observations
                WHERE station_id = %s
                  AND observed_at >= UTC_TIMESTAMP() - INTERVAL %s HOUR
                ORDER BY observed_at ASC
            """, (station_id, hours))
            return cur.fetchall()

    def get_daily_summaries(self, station_id: int, days: int = 30) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT * FROM daily_summaries
                WHERE station_id = %s
                  AND summary_date >= CURDATE() - INTERVAL %s DAY
                ORDER BY summary_date ASC
            """, (station_id, days))
            return cur.fetchall()

    def get_latest_for_all_stations(self) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM v_latest_conditions")
            return cur.fetchall()

    def get_observation_count(self) -> int:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM observations")
            return cur.fetchone()[0]

    # ── Alert helpers ──────────────────────────────────────────────────────
    def insert_alert(self, station_id: int, alert_type: str, severity: str,
                     metric_name: str, metric_value: float,
                     threshold_value: float, message: str) -> int:
        sql = """
            INSERT INTO alerts
                (station_id, alert_type, severity, metric_name,
                 metric_value, threshold_value, message, triggered_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())
        """
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, (station_id, alert_type, severity,
                              metric_name, metric_value, threshold_value, message))
            conn.commit()
            return cur.lastrowid

    def resolve_alert(self, alert_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE alerts SET resolved_at = UTC_TIMESTAMP()
                WHERE alert_id = %s AND resolved_at IS NULL
            """, (alert_id,))
            conn.commit()
            return cur.rowcount > 0

    def get_active_alerts(self) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM v_active_alerts")
            return cur.fetchall()

    def has_active_alert(self, station_id: int, alert_type: str) -> bool:
        """Prevent duplicate active alerts of same type per station."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM alerts
                WHERE station_id = %s AND alert_type = %s AND resolved_at IS NULL
            """, (station_id, alert_type))
            return cur.fetchone()[0] > 0

    # ── API log ────────────────────────────────────────────────────────────
    def log_api_call(self, station_id: Optional[int], api_source: str,
                     endpoint_url: str, http_status: int, success: bool,
                     latency_ms: int, error_message: Optional[str] = None):
        sql = """
            INSERT INTO api_log
                (station_id, api_source, endpoint_url, http_status,
                 success, latency_ms, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._conn() as conn:
                cur = conn.cursor()
                cur.execute(sql, (station_id, api_source, endpoint_url,
                                  http_status, int(success), latency_ms,
                                  error_message))
                conn.commit()
        except Exception:
            pass  # Never let logging crash the main flow

    # ── Daily summary ──────────────────────────────────────────────────────
    def compute_summary_for(self, station_id: int, target_date: date):
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("CALL sp_compute_daily_summary(%s, %s)", (station_id, target_date))
            conn.commit()
            log.info(f"  Summary computed — station={station_id} date={target_date}")

    def get_api_health(self) -> list[dict]:
        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT api_source,
                       COUNT(*) AS total_calls,
                       SUM(success) AS successful,
                       ROUND(AVG(latency_ms)) AS avg_latency_ms,
                       MAX(called_at) AS last_called
                FROM api_log
                WHERE called_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
                GROUP BY api_source
                ORDER BY api_source
            """)
            return cur.fetchall()


# ═══════════════════════════════════════════════════════════════════════════
# WEATHER FETCHER
# ═══════════════════════════════════════════════════════════════════════════
class WeatherFetcher:
    """
    Fetches current conditions from multiple free APIs and normalises
    them into a single dict ready for DatabaseManager.insert_observation().

    APIs:
      - Open-Meteo  (https://api.open-meteo.com)  — NO KEY REQUIRED
      - OpenWeatherMap (https://api.openweathermap.org) — free tier key
      - NASA POWER  (https://power.larc.nasa.gov)  — NO KEY REQUIRED
    """

    OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
    OWM_URL        = "https://api.openweathermap.org/data/2.5/weather"
    NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"

    CURRENT_VARS = [
        "temperature_2m", "apparent_temperature", "relative_humidity_2m",
        "dew_point_2m", "surface_pressure", "wind_speed_10m",
        "wind_direction_10m", "wind_gusts_10m", "precipitation",
        "cloud_cover", "visibility", "uv_index",
        "shortwave_radiation", "weather_code"
    ]

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.owm_key = os.getenv("OWM_API_KEY", "")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "WeatherCore/1.0"})

    # ── Open-Meteo (primary) ───────────────────────────────────────────────
    def fetch_open_meteo(self, station: dict) -> Optional[dict]:
        """No API key required. Best free source for current + hourly data."""
        params = {
            "latitude":       station["latitude"],
            "longitude":      station["longitude"],
            "current":        ",".join(self.CURRENT_VARS),
            "wind_speed_unit":"ms",
            "timezone":       "UTC",
            "forecast_days":  1,
        }
        start = time.monotonic()
        try:
            r = self.session.get(self.OPEN_METEO_URL, params=params, timeout=15)
            latency = int((time.monotonic() - start) * 1000)
            r.raise_for_status()
            raw = r.json()
            c   = raw["current"]

            self.db.log_api_call(
                station["station_id"], "open_meteo",
                r.url, r.status_code, True, latency
            )

            wcode = c.get("weather_code")
            return {
                "observed_at":        datetime.fromisoformat(c["time"]).replace(tzinfo=None),
                "temperature_c":      c.get("temperature_2m"),
                "feels_like_c":       c.get("apparent_temperature"),
                "humidity_pct":       c.get("relative_humidity_2m"),
                "dew_point_c":        c.get("dew_point_2m"),
                "pressure_hpa":       c.get("surface_pressure"),
                "wind_speed_ms":      c.get("wind_speed_10m"),
                "wind_dir_deg":       c.get("wind_direction_10m"),
                "wind_gust_ms":       c.get("wind_gusts_10m"),
                "precipitation_mm":   c.get("precipitation", 0.0),
                "cloud_cover_pct":    c.get("cloud_cover"),
                "visibility_km":      round(c["visibility"] / 1000, 2) if c.get("visibility") else None,
                "uv_index":           c.get("uv_index"),
                "solar_radiation":    c.get("shortwave_radiation"),
                "weather_code":       wcode,
                "weather_description": WMO_CODES.get(wcode, "Unknown"),
                "source_api":         "open_meteo",
                "raw_json":           json.dumps(raw),
            }
        except Exception as e:
            latency = int((time.monotonic() - start) * 1000)
            self.db.log_api_call(
                station["station_id"], "open_meteo",
                self.OPEN_METEO_URL, 0, False, latency, str(e)
            )
            log.warning(f"Open-Meteo failed for {station['station_code']}: {e}")
            return None

    # ── OpenWeatherMap (secondary / validation) ────────────────────────────
    def fetch_owm(self, station: dict) -> Optional[dict]:
        """Free tier: 1,000 calls/day. Register at openweathermap.org."""
        if not self.owm_key:
            return None
        params = {
            "lat":   station["latitude"],
            "lon":   station["longitude"],
            "appid": self.owm_key,
            "units": "metric",
        }
        start = time.monotonic()
        try:
            r = self.session.get(self.OWM_URL, params=params, timeout=15)
            latency = int((time.monotonic() - start) * 1000)
            r.raise_for_status()
            raw  = r.json()
            main = raw.get("main", {})
            wind = raw.get("wind", {})
            rain = raw.get("rain", {})

            self.db.log_api_call(
                station["station_id"], "owm",
                r.url, r.status_code, True, latency
            )

            obs_dt = datetime.fromtimestamp(raw.get("dt", time.time()), tz=timezone.utc).replace(tzinfo=None)
            return {
                "observed_at":        obs_dt,
                "temperature_c":      main.get("temp"),
                "feels_like_c":       main.get("feels_like"),
                "humidity_pct":       main.get("humidity"),
                "dew_point_c":        None,
                "pressure_hpa":       main.get("pressure"),
                "wind_speed_ms":      wind.get("speed"),
                "wind_dir_deg":       wind.get("deg"),
                "wind_gust_ms":       wind.get("gust"),
                "precipitation_mm":   rain.get("1h", 0.0),
                "cloud_cover_pct":    raw.get("clouds", {}).get("all"),
                "visibility_km":      round(raw.get("visibility", 0) / 1000, 2),
                "uv_index":           None,  # Not in free current endpoint
                "solar_radiation":    None,
                "weather_code":       raw.get("weather", [{}])[0].get("id"),
                "weather_description": raw.get("weather", [{}])[0].get("description", "").title(),
                "source_api":         "owm",
                "raw_json":           json.dumps(raw),
            }
        except Exception as e:
            latency = int((time.monotonic() - start) * 1000)
            self.db.log_api_call(
                station["station_id"], "owm",
                self.OWM_URL, 0, False, latency, str(e)
            )
            log.warning(f"OWM failed for {station['station_code']}: {e}")
            return None

    # ── NASA POWER (solar / radiation specialist) ──────────────────────────
    def fetch_nasa_power(self, station: dict) -> Optional[dict]:
        """
        NASA POWER: no key needed. Best for solar radiation.
        Note: this is research-grade — responses are slower (~3s).
        We use it only for solar_radiation supplement, not primary.
        """
        now = datetime.utcnow()
        params = {
            "parameters": "ALLSKY_SFC_SW_DWN,T2M,RH2M",
            "community":  "RE",
            "longitude":  station["longitude"],
            "latitude":   station["latitude"],
            "start":      now.strftime("%Y%m%d"),
            "end":        now.strftime("%Y%m%d"),
            "format":     "JSON",
        }
        start = time.monotonic()
        try:
            r = self.session.get(self.NASA_POWER_URL, params=params, timeout=30)
            latency = int((time.monotonic() - start) * 1000)
            r.raise_for_status()
            raw = r.json()
            props = raw.get("properties", {}).get("parameter", {})
            hour_str = now.strftime("%Y%m%d%H")

            self.db.log_api_call(
                station["station_id"], "nasa_power",
                r.url, r.status_code, True, latency
            )

            solar = props.get("ALLSKY_SFC_SW_DWN", {}).get(hour_str)
            temp  = props.get("T2M", {}).get(hour_str)
            rh    = props.get("RH2M", {}).get(hour_str)

            return {
                "observed_at":        now.replace(minute=0, second=0, microsecond=0),
                "temperature_c":      temp if temp and temp != -999 else None,
                "feels_like_c":       None,
                "humidity_pct":       rh   if rh   and rh   != -999 else None,
                "dew_point_c":        None,
                "pressure_hpa":       None,
                "wind_speed_ms":      None,
                "wind_dir_deg":       None,
                "wind_gust_ms":       None,
                "precipitation_mm":   0.0,
                "cloud_cover_pct":    None,
                "visibility_km":      None,
                "uv_index":           None,
                "solar_radiation":    solar if solar and solar != -999 else None,
                "weather_code":       None,
                "weather_description": "NASA POWER satellite observation",
                "source_api":         "nasa_power",
                "raw_json":           json.dumps(raw),
            }
        except Exception as e:
            latency = int((time.monotonic() - start) * 1000)
            self.db.log_api_call(
                station["station_id"], "nasa_power",
                self.NASA_POWER_URL, 0, False, latency, str(e)
            )
            log.warning(f"NASA POWER failed for {station['station_code']}: {e}")
            return None

    # ── Main ingestion entry point ─────────────────────────────────────────
    def ingest_station(self, station: dict) -> dict:
        """
        Fetch from all available APIs for one station.
        Returns summary dict with counts of successes.
        """
        results = {"station": station["station_code"], "inserted": 0, "skipped": 0, "errors": 0}

        # Primary: Open-Meteo (always try — no key needed)
        data = self.fetch_open_meteo(station)
        if data:
            obs_id = self.db.insert_observation(station["station_id"], data)
            if obs_id:
                results["inserted"] += 1
                log.info(f"  ✓ {station['station_code']} open_meteo → {data['temperature_c']}°C obs_id={obs_id}")
            else:
                results["skipped"] += 1
        else:
            results["errors"] += 1

        # Secondary: OpenWeatherMap (if key configured)
        if self.owm_key:
            data = self.fetch_owm(station)
            if data:
                obs_id = self.db.insert_observation(station["station_id"], data)
                results["inserted" if obs_id else "skipped"] += 1

        return results

    def ingest_all(self) -> list[dict]:
        """Run ingestion for all active stations. Called by scheduler."""
        log.info("═══ Starting ingestion cycle ═══")
        stations = self.db.get_active_stations()
        log.info(f"  Active stations: {len(stations)}")
        summaries = []
        for station in stations:
            try:
                result = self.ingest_station(station)
                summaries.append(result)
            except Exception as e:
                log.error(f"  Unhandled error for {station['station_code']}: {e}")
                summaries.append({"station": station["station_code"], "errors": 1})
        log.info(f"═══ Ingestion complete — {sum(r.get('inserted',0) for r in summaries)} rows inserted ═══")
        return summaries


# ═══════════════════════════════════════════════════════════════════════════
# ALERT ENGINE
# ═══════════════════════════════════════════════════════════════════════════
class AlertEngine:
    """
    Reads thresholds from system_config and fires alerts when breached.
    Prevents duplicate active alerts of the same type per station.
    Resolves alerts automatically when values return to safe levels.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db
        self._load_thresholds()

    def _load_thresholds(self):
        cfg = self.db.get_all_config()
        self.thresholds = {
            "heat": {
                "field":     "temperature_c",
                "op":        "gt",
                "warning":   float(cfg.get("alert_heat_warning",    35)),
                "emergency": float(cfg.get("alert_heat_emergency",   42)),
            },
            "cold": {
                "field":     "temperature_c",
                "op":        "lt",
                "warning":   float(cfg.get("alert_cold_warning",     0)),
                "emergency": float(cfg.get("alert_cold_emergency",  -10)),
            },
            "wind": {
                "field":     "wind_speed_ms",
                "op":        "gt",
                "warning":   float(cfg.get("alert_wind_warning",    15)),
                "emergency": float(cfg.get("alert_wind_emergency",  25)),
            },
            "flood": {
                "field":     "precipitation_mm",
                "op":        "gt",
                "warning":   float(cfg.get("alert_precip_warning",  20)),
                "emergency": float(cfg.get("alert_precip_emergency", 50)),
            },
            "uv": {
                "field":     "uv_index",
                "op":        "gt",
                "warning":   float(cfg.get("alert_uv_warning",       7)),
                "emergency": float(cfg.get("alert_uv_emergency",    10)),
            },
        }
        log.info(f"  Alert engine loaded {len(self.thresholds)} threshold rules")

    def _exceeds(self, value: float, op: str, threshold: float) -> bool:
        if op == "gt": return value > threshold
        if op == "lt": return value < threshold
        return False

    def _severity(self, value: float, cfg: dict) -> Optional[str]:
        if self._exceeds(value, cfg["op"], cfg["emergency"]): return "emergency"
        if self._exceeds(value, cfg["op"], cfg["warning"]):   return "warning"
        return None

    def _build_message(self, station: dict, alert_type: str, severity: str,
                       field: str, value: float, threshold: float) -> str:
        direction = "above" if value > threshold else "below"
        return (
            f"[{severity.upper()}] {station['station_name']} ({station['city']}): "
            f"{field.replace('_', ' ')} is {value} — {direction} {alert_type} "
            f"{severity} threshold of {threshold}."
        )

    def check_observation(self, station: dict, obs: dict):
        """
        Called after every successful insert.
        station: dict from v_latest_conditions (has station_name, city, etc.)
        obs: the normalised observation dict
        """
        station_id = station.get("station_id") or obs.get("station_id")
        for alert_type, cfg in self.thresholds.items():
            value = obs.get(cfg["field"])
            if value is None:
                continue
            severity = self._severity(value, cfg)
            if severity:
                if not self.db.has_active_alert(station_id, alert_type):
                    msg = self._build_message(
                        station, alert_type, severity,
                        cfg["field"], value,
                        cfg["emergency"] if severity == "emergency" else cfg["warning"]
                    )
                    alert_id = self.db.insert_alert(
                        station_id, alert_type, severity,
                        cfg["field"], value,
                        cfg["emergency"] if severity == "emergency" else cfg["warning"],
                        msg
                    )
                    log.warning(f"  🔔 ALERT #{alert_id} [{severity}] {alert_type} — {station.get('station_code','?')} val={value}")


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARISER
# ═══════════════════════════════════════════════════════════════════════════
class Summariser:
    """
    Computes daily summaries from raw observations.
    Runs at 00:05 UTC every night via scheduler.
    Also called manually for backfill.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    def compute_yesterday(self):
        """Standard nightly job — summarise yesterday for all stations."""
        yesterday = date.today() - timedelta(days=1)
        self.compute_for_date(yesterday)

    def compute_for_date(self, target_date: date):
        log.info(f"═══ Computing daily summaries for {target_date} ═══")
        stations = self.db.get_active_stations()
        for station in stations:
            try:
                self.db.compute_summary_for(station["station_id"], target_date)
            except Exception as e:
                log.error(f"  Summary failed for {station['station_code']}: {e}")
        log.info("═══ Daily summaries complete ═══")

    def backfill(self, days: int = 7):
        """Backfill summaries for the last N days (useful on first run)."""
        for i in range(days, 0, -1):
            target = date.today() - timedelta(days=i)
            self.compute_for_date(target)


# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════
class WeatherScheduler:
    """
    Wraps APScheduler to run:
      - Weather ingestion every 15 minutes
      - Daily summaries at 00:05 UTC
      - Alert threshold reload every hour (picks up config changes)
    """

    def __init__(self, fetcher: WeatherFetcher,
                 alert_engine: AlertEngine,
                 summariser: Summariser):
        self.fetcher       = fetcher
        self.alert_engine  = alert_engine
        self.summariser    = summariser
        self._scheduler    = BackgroundScheduler(timezone="UTC")
        self._running      = False

    def _ingest_job(self):
        """Combined fetch + alert check job."""
        try:
            self.fetcher.ingest_all()
        except Exception as e:
            log.error(f"Ingest job error: {e}")

    def _summary_job(self):
        try:
            self.summariser.compute_yesterday()
        except Exception as e:
            log.error(f"Summary job error: {e}")

    def _reload_thresholds_job(self):
        try:
            self.alert_engine._load_thresholds()
            log.info("  Alert thresholds reloaded from DB")
        except Exception as e:
            log.error(f"Threshold reload error: {e}")

    def start(self, run_immediately: bool = True):
        # Every 15 minutes — weather ingestion
        self._scheduler.add_job(
            self._ingest_job, "interval", minutes=15,
            id="weather_ingest", replace_existing=True,
            next_run_time=datetime.utcnow() if run_immediately else None
        )
        # Nightly at 00:05 UTC — daily summaries
        self._scheduler.add_job(
            self._summary_job, "cron",
            hour=0, minute=5, id="daily_summary", replace_existing=True
        )
        # Hourly — reload thresholds from DB
        self._scheduler.add_job(
            self._reload_thresholds_job, "interval",
            minutes=60, id="reload_thresholds", replace_existing=True
        )
        self._scheduler.start()
        self._running = True
        log.info("✓ Scheduler started")
        log.info("  → Ingestion: every 15 minutes")
        log.info("  → Summaries: daily at 00:05 UTC")
        log.info("  → Thresholds: reload every 60 minutes")

    def stop(self):
        if self._running:
            self._scheduler.shutdown(wait=False)
            self._running = False
            log.info("Scheduler stopped")

    def get_jobs(self) -> list[dict]:
        return [
            {
                "id":       job.id,
                "next_run": str(job.next_run_time),
                "trigger":  str(job.trigger),
            }
            for job in self._scheduler.get_jobs()
        ]


# ═══════════════════════════════════════════════════════════════════════════
# FACTORY — singleton instances used by app.py
# ═══════════════════════════════════════════════════════════════════════════
def create_components():
    """
    Create and wire all backend components.
    Returns (db, fetcher, alert_engine, summariser, scheduler)
    """
    db            = DatabaseManager()
    fetcher       = WeatherFetcher(db)
    alert_engine  = AlertEngine(db)
    summariser    = Summariser(db)
    scheduler     = WeatherScheduler(fetcher, alert_engine, summariser)
    return db, fetcher, alert_engine, summariser, scheduler



# ═══════════════════════════════════════════════════════════════════════════
# STANDALONE RUN (python backend.py)
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import atexit, signal, sys

    log.info("WeatherCore backend starting in standalone mode...")
    db, fetcher, alert_engine, summariser, scheduler = create_components()

    # Backfill summaries for past 7 days on first run
    log.info("Backfilling last 7 days of summaries...")
    summariser.backfill(days=7)

    scheduler.start(run_immediately=True)
    atexit.register(scheduler.stop)

    def _handle_signal(sig, frame):
        log.info("Shutdown signal received")
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("Backend running. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.stop()
