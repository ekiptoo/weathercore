-- =============================================================
--  WeatherCore — Full MySQL Schema
--  Phase 1: Database Foundation
--  Engine: InnoDB | Charset: utf8mb4 | MySQL 8.0+
--  Run: mysql -u weatheruser -p weathercore < schema.sql
-- =============================================================

SET FOREIGN_KEY_CHECKS = 0;
SET SQL_MODE = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -------------------------------------------------------------
-- Drop in reverse dependency order
-- -------------------------------------------------------------
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS daily_summaries;
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS api_log;
DROP TABLE IF EXISTS system_config;

SET FOREIGN_KEY_CHECKS = 1;

-- =============================================================
-- TABLE 1: stations
-- Master reference for all monitoring locations
-- =============================================================
CREATE TABLE stations (
    station_id      INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    station_code    VARCHAR(20)         NOT NULL COMMENT 'WMO/ICAO/custom code e.g. KE-001',
    name            VARCHAR(120)        NOT NULL COMMENT 'Human-readable station name',
    city            VARCHAR(80)         NOT NULL,
    country         CHAR(2)             NOT NULL COMMENT 'ISO 3166-1 alpha-2',
    latitude        DECIMAL(9,6)        NOT NULL COMMENT 'Decimal degrees, -90 to 90',
    longitude       DECIMAL(9,6)        NOT NULL COMMENT 'Decimal degrees, -180 to 180',
    elevation_m     SMALLINT            NOT NULL DEFAULT 0 COMMENT 'Metres above sea level',
    timezone        VARCHAR(40)         NOT NULL DEFAULT 'UTC' COMMENT 'IANA timezone string',
    active          TINYINT(1)          NOT NULL DEFAULT 1 COMMENT '1=poll this station',
    created_at      TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (station_id),
    UNIQUE KEY uq_station_code (station_code),
    KEY idx_country (country),
    KEY idx_active (active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Weather station master reference table';


-- =============================================================
-- TABLE 2: observations
-- Primary fact table — one row per station per API fetch
-- High-write table; composite index on (station_id, observed_at)
-- =============================================================
CREATE TABLE observations (
    obs_id              BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    station_id          INT UNSIGNED        NOT NULL,
    observed_at         DATETIME            NOT NULL COMMENT 'UTC timestamp of the reading',

    -- Atmospheric
    temperature_c       DECIMAL(5,2)                 COMMENT 'Air temperature at 2m, Celsius',
    feels_like_c        DECIMAL(5,2)                 COMMENT 'Apparent/feels-like temperature',
    humidity_pct        DECIMAL(5,2)                 COMMENT 'Relative humidity 0-100%',
    dew_point_c         DECIMAL(5,2)                 COMMENT 'Dew point temperature',
    pressure_hpa        DECIMAL(7,2)                 COMMENT 'Sea-level pressure, hPa',
    pressure_trend      ENUM('rising','steady','falling') COMMENT 'Pressure change direction',

    -- Wind
    wind_speed_ms       DECIMAL(5,2)                 COMMENT 'Wind speed at 10m, m/s',
    wind_dir_deg        SMALLINT UNSIGNED            COMMENT 'Wind direction 0-360 degrees',
    wind_gust_ms        DECIMAL(5,2)                 COMMENT 'Maximum gust speed, m/s',

    -- Precipitation & cloud
    precipitation_mm    DECIMAL(6,2)        NOT NULL DEFAULT 0.00 COMMENT 'Hourly accumulation, mm',
    snow_depth_cm       DECIMAL(6,2)                 COMMENT 'Snow depth on ground, cm',
    cloud_cover_pct     TINYINT UNSIGNED             COMMENT 'Total cloud cover 0-100%',

    -- Visibility & radiation
    visibility_km       DECIMAL(6,2)                 COMMENT 'Horizontal visibility, km',
    uv_index            DECIMAL(4,2)                 COMMENT 'UV index 0-12+',
    solar_radiation     DECIMAL(8,2)                 COMMENT 'Global horizontal irradiance, W/m2',

    -- WMO weather classification
    weather_code        SMALLINT                     COMMENT 'WMO weather interpretation code',
    weather_description VARCHAR(80)                  COMMENT 'Human-readable WMO description',

    -- Data provenance
    source_api          VARCHAR(30)         NOT NULL COMMENT 'open_meteo | owm | noaa | nasa',
    raw_json            JSON                         COMMENT 'Full API response snapshot for audit',
    created_at          TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (obs_id),
    CONSTRAINT fk_obs_station
        FOREIGN KEY (station_id) REFERENCES stations(station_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    -- Core query patterns: by station+time, by time only
    KEY idx_station_time    (station_id, observed_at),
    KEY idx_time            (observed_at),
    KEY idx_source          (source_api),

    -- Prevent duplicate records for same station+time+source
    UNIQUE KEY uq_station_time_source (station_id, observed_at, source_api)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Primary weather observation fact table';


-- =============================================================
-- TABLE 3: daily_summaries
-- Pre-aggregated daily stats — computed nightly by summariser.py
-- Avoids expensive GROUP BY queries on observations at runtime
-- =============================================================
CREATE TABLE daily_summaries (
    summary_id          INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    station_id          INT UNSIGNED        NOT NULL,
    summary_date        DATE                NOT NULL COMMENT 'Local date for this summary',

    -- Temperature stats
    temp_min_c          DECIMAL(5,2)                 COMMENT 'Daily minimum temperature',
    temp_max_c          DECIMAL(5,2)                 COMMENT 'Daily maximum temperature',
    temp_avg_c          DECIMAL(5,2)                 COMMENT 'Daily mean temperature',
    temp_range_c        DECIMAL(5,2)                 COMMENT 'temp_max - temp_min',

    -- Humidity
    humidity_avg_pct    DECIMAL(5,2),
    humidity_min_pct    DECIMAL(5,2),
    humidity_max_pct    DECIMAL(5,2),

    -- Wind
    wind_avg_ms         DECIMAL(5,2),
    wind_max_ms         DECIMAL(5,2)                 COMMENT 'Peak gust of the day',
    dominant_wind_dir   SMALLINT UNSIGNED            COMMENT 'Modal wind direction',

    -- Precipitation
    precip_total_mm     DECIMAL(6,2)        NOT NULL DEFAULT 0.00,
    precip_hours        TINYINT UNSIGNED             COMMENT 'Hours with precipitation > 0',
    snow_total_cm       DECIMAL(6,2)        NOT NULL DEFAULT 0.00,

    -- Radiation & UV
    uv_max              DECIMAL(4,2),
    solar_total_kwh     DECIMAL(8,3)                 COMMENT 'Total solar energy kWh/m2',

    -- Observation quality
    obs_count           SMALLINT UNSIGNED   NOT NULL DEFAULT 0 COMMENT 'Readings used for this day',
    computed_at         TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (summary_id),
    CONSTRAINT fk_summary_station
        FOREIGN KEY (station_id) REFERENCES stations(station_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE KEY uq_station_date (station_id, summary_date),
    KEY idx_summary_date (summary_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Pre-aggregated daily weather summaries per station';


-- =============================================================
-- TABLE 4: alerts
-- Threshold-breach events generated by alerts.py
-- resolved_at = NULL means the alert is still active
-- =============================================================
CREATE TABLE alerts (
    alert_id        INT UNSIGNED        NOT NULL AUTO_INCREMENT,
    station_id      INT UNSIGNED        NOT NULL,
    alert_type      ENUM(
                        'heat','cold','storm','flood',
                        'wind','uv','pressure','visibility'
                    )                   NOT NULL,
    severity        ENUM('watch','warning','emergency') NOT NULL,
    metric_name     VARCHAR(40)         NOT NULL COMMENT 'Field that breached threshold',
    metric_value    DECIMAL(10,3)       NOT NULL COMMENT 'Actual value at time of alert',
    threshold_value DECIMAL(10,3)       NOT NULL COMMENT 'Configured threshold breached',
    message         TEXT                NOT NULL COMMENT 'Auto-generated alert description',
    triggered_at    DATETIME            NOT NULL COMMENT 'When threshold was first breached',
    resolved_at     DATETIME                     COMMENT 'NULL = still active',
    notified        TINYINT(1)          NOT NULL DEFAULT 0 COMMENT '1 if notification sent',
    created_at      TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (alert_id),
    CONSTRAINT fk_alert_station
        FOREIGN KEY (station_id) REFERENCES stations(station_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    KEY idx_alert_station   (station_id),
    KEY idx_alert_active    (resolved_at),
    KEY idx_alert_type      (alert_type, severity),
    KEY idx_alert_time      (triggered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Weather threshold breach alert log';


-- =============================================================
-- TABLE 5: api_log
-- Audit trail for every outbound API call
-- Track success/failure rates and latency per source
-- =============================================================
CREATE TABLE api_log (
    log_id          BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    station_id      INT UNSIGNED                 COMMENT 'NULL for non-station calls',
    api_source      VARCHAR(30)         NOT NULL COMMENT 'open_meteo | owm | noaa | nasa',
    endpoint_url    VARCHAR(500)        NOT NULL,
    http_status     SMALLINT UNSIGNED            COMMENT 'HTTP response code',
    success         TINYINT(1)          NOT NULL DEFAULT 0,
    latency_ms      INT UNSIGNED                 COMMENT 'Round-trip time in ms',
    error_message   TEXT                         COMMENT 'NULL on success',
    called_at       TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (log_id),
    KEY idx_apilog_source  (api_source, called_at),
    KEY idx_apilog_success (success, called_at),
    KEY idx_apilog_station (station_id, called_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Outbound API call audit log';


-- =============================================================
-- TABLE 6: system_config
-- Runtime key-value configuration store
-- Editable via admin without code changes
-- =============================================================
CREATE TABLE system_config (
    config_key      VARCHAR(60)         NOT NULL,
    config_value    TEXT                NOT NULL,
    description     VARCHAR(200),
    updated_at      TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Runtime system configuration key-value store';


-- =============================================================
-- STORED PROCEDURE: sp_compute_daily_summary
-- Called by summariser.py for a given station + date
-- =============================================================
DELIMITER $$

CREATE PROCEDURE sp_compute_daily_summary(
    IN p_station_id INT UNSIGNED,
    IN p_date       DATE
)
BEGIN
    INSERT INTO daily_summaries (
        station_id, summary_date,
        temp_min_c, temp_max_c, temp_avg_c, temp_range_c,
        humidity_avg_pct, humidity_min_pct, humidity_max_pct,
        wind_avg_ms, wind_max_ms,
        precip_total_mm, precip_hours,
        uv_max, obs_count
    )
    SELECT
        p_station_id,
        p_date,
        MIN(temperature_c),
        MAX(temperature_c),
        ROUND(AVG(temperature_c), 2),
        ROUND(MAX(temperature_c) - MIN(temperature_c), 2),
        ROUND(AVG(humidity_pct), 2),
        MIN(humidity_pct),
        MAX(humidity_pct),
        ROUND(AVG(wind_speed_ms), 2),
        MAX(wind_gust_ms),
        ROUND(SUM(precipitation_mm), 2),
        SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END),
        MAX(uv_index),
        COUNT(*)
    FROM observations
    WHERE station_id   = p_station_id
      AND DATE(observed_at) = p_date
    ON DUPLICATE KEY UPDATE
        temp_min_c       = VALUES(temp_min_c),
        temp_max_c       = VALUES(temp_max_c),
        temp_avg_c       = VALUES(temp_avg_c),
        temp_range_c     = VALUES(temp_range_c),
        humidity_avg_pct = VALUES(humidity_avg_pct),
        wind_avg_ms      = VALUES(wind_avg_ms),
        wind_max_ms      = VALUES(wind_max_ms),
        precip_total_mm  = VALUES(precip_total_mm),
        precip_hours     = VALUES(precip_hours),
        uv_max           = VALUES(uv_max),
        obs_count        = VALUES(obs_count),
        computed_at      = CURRENT_TIMESTAMP;
END$$

DELIMITER ;


-- =============================================================
-- VIEW: v_latest_conditions
-- Fast access to the most recent reading per active station
-- Used by /api/current Flask endpoint
-- =============================================================
CREATE OR REPLACE VIEW v_latest_conditions AS
SELECT
    s.station_id,
    s.station_code,
    s.name            AS station_name,
    s.city,
    s.country,
    s.latitude,
    s.longitude,
    s.elevation_m,
    o.obs_id,
    o.observed_at,
    o.temperature_c,
    o.feels_like_c,
    o.humidity_pct,
    o.pressure_hpa,
    o.wind_speed_ms,
    o.wind_dir_deg,
    o.wind_gust_ms,
    o.precipitation_mm,
    o.cloud_cover_pct,
    o.visibility_km,
    o.uv_index,
    o.weather_code,
    o.weather_description,
    o.source_api
FROM stations s
JOIN observations o ON o.obs_id = (
    SELECT obs_id
    FROM   observations
    WHERE  station_id = s.station_id
    ORDER  BY observed_at DESC
    LIMIT  1
)
WHERE s.active = 1;


-- =============================================================
-- VIEW: v_active_alerts
-- All unresolved alerts with station context
-- =============================================================
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT
    a.alert_id,
    a.alert_type,
    a.severity,
    a.metric_name,
    a.metric_value,
    a.threshold_value,
    a.message,
    a.triggered_at,
    s.station_id,
    s.station_code,
    s.name   AS station_name,
    s.city,
    s.country
FROM alerts a
JOIN stations s ON s.station_id = a.station_id
WHERE a.resolved_at IS NULL
ORDER BY
    FIELD(a.severity, 'emergency','warning','watch'),
    a.triggered_at DESC;


-- =============================================================
-- SEED DATA: default system config
-- =============================================================
INSERT INTO system_config (config_key, config_value, description) VALUES
('fetch_interval_minutes',  '15',   'How often to poll all active stations'),
('alert_heat_warning',      '35',   'Heat warning threshold °C'),
('alert_heat_emergency',    '42',   'Heat emergency threshold °C'),
('alert_cold_warning',      '0',    'Cold warning threshold °C'),
('alert_cold_emergency',    '-10',  'Cold emergency threshold °C'),
('alert_wind_warning',      '15',   'Wind warning threshold m/s'),
('alert_wind_emergency',    '25',   'Wind emergency threshold m/s'),
('alert_precip_warning',    '20',   'Precipitation warning threshold mm/hr'),
('alert_precip_emergency',  '50',   'Precipitation emergency threshold mm/hr'),
('alert_uv_warning',        '7',    'UV index warning threshold'),
('alert_uv_emergency',      '10',   'UV index emergency threshold'),
('retention_days_raw',      '90',   'Days to retain raw observations'),
('retention_days_summary',  '730',  'Days to retain daily summaries'),
('primary_api',             'open_meteo', 'Primary weather data source'),
('secondary_api',           'owm',  'Fallback API source');


-- =============================================================
-- SEED DATA: initial stations (East Africa + global examples)
-- =============================================================
INSERT INTO stations (station_code, name, city, country, latitude, longitude, elevation_m, timezone, active) VALUES
('KE-NBO-001', 'Nairobi JKIA',          'Nairobi',      'KE', -1.319167,  36.927500, 1624, 'Africa/Nairobi',   1),
('KE-MBA-001', 'Mombasa Moi Airport',   'Mombasa',      'KE', -4.034300,  39.594200,   55, 'Africa/Nairobi',   1),
('KE-KIS-001', 'Kisumu Met Station',    'Kisumu',       'KE', -0.091700,  34.767900, 1131, 'Africa/Nairobi',   1),
('KE-NKR-001', 'Nakuru Met',            'Nakuru',       'KE', -0.303000,  36.080000, 1850, 'Africa/Nairobi',   1),
('UG-KLA-001', 'Kampala Entebbe Intl',  'Kampala',      'UG',  0.042400,  32.443500, 1155, 'Africa/Kampala',   1),
('TZ-DAR-001', 'Dar es Salaam Julius N','Dar es Salaam','TZ', -6.878100,  39.202500,   53, 'Africa/Dar_es_Salaam', 1),
('ZA-JNB-001', 'Johannesburg OR Tambo', 'Johannesburg', 'ZA', -26.133700, 28.242000, 1694, 'Africa/Johannesburg', 1),
('NG-LOS-001', 'Lagos Murtala Intl',    'Lagos',        'NG',  6.577400,   3.321200,   38, 'Africa/Lagos',     1);


-- =============================================================
-- VERIFICATION QUERIES (comment out before running in prod)
-- =============================================================
-- SHOW TABLES;
-- DESCRIBE observations;
-- SELECT * FROM stations;
-- SELECT * FROM system_config;
-- SHOW CREATE VIEW v_latest_conditions;

SELECT
    CONCAT('✓ Schema created: ',
           (SELECT COUNT(*) FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()), ' tables/views') AS status,
    (SELECT COUNT(*) FROM stations) AS stations_seeded,
    (SELECT COUNT(*) FROM system_config) AS config_keys;
