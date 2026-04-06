
import curses


def _dedx_field(v):
    if v is None:
        return ""
    v = str(v).strip()
    return v

def _dedx_info_line(label, info):
    if not info:
        return f"{label}:"
    call = _dedx_field(info.get("callsign") or info.get("call"))
    name = _dedx_field(info.get("name"))
    city = _dedx_field(info.get("city"))
    prov = _dedx_field(info.get("prov") or info.get("state") or info.get("province"))
    country = _dedx_field(info.get("country"))
    source = _dedx_field(info.get("source") or info.get("database"))

    parts = [p for p in [call, name, city, prov, country] if p]
    line1 = f"{label}: " + " | ".join(parts) if parts else f"{label}:"
    line2 = f"Src: {source}" if source else "Src:"
    return line1 + "\n" + line2

def format_dedx_static_panel(de_info, dx_info):
    # Fixed non-scrolling layout:
    # DE info (2 lines), one blank line, DX info (2 lines)
    de_block = _dedx_info_line("DE", de_info)
    dx_block = _dedx_info_line("DX", dx_info)
    return de_block + "\n\n" + dx_block
import os
import io
import contextlib
import importlib.util
import time
import threading
import queue
import socket
import ssl
import re
import json
import sqlite3

try:
    import pymysql as _mysql_driver
    _MYSQL_DRIVER_NAME = "pymysql"
except Exception:
    try:
        import mysql.connector as _mysql_driver
        _MYSQL_DRIVER_NAME = "mysql.connector"
    except Exception:
        try:
            import mariadb as _mysql_driver
            _MYSQL_DRIVER_NAME = "mariadb"
        except Exception:
            _mysql_driver = None
            _MYSQL_DRIVER_NAME = ""
import sys
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

try:
    from PIL import Image
except Exception:
    Image = None


def _app_base_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.getcwd()

def _app_file_path(filename: str) -> str:
    return os.path.join(_app_base_dir(), filename)


LOG_FILE = _app_file_path("hamclock.log")
LOGGING_ENABLED = True

def set_logging_enabled(enabled: bool) -> None:
    global LOGGING_ENABLED
    LOGGING_ENABLED = bool(enabled)

def _truncate_for_log(value: str, limit: int = 1500) -> str:
    value = "" if value is None else str(value)
    return value if len(value) <= limit else value[:limit] + f"... [truncated {len(value)-limit} chars]"

def _mask_sensitive_text(value: str) -> str:
    text = "" if value is None else str(value)
    return re.sub(r'((?:password|passwd|pwd)=)([^&;\s]+)', r'\1***', text, flags=re.IGNORECASE)

def _debug_log(message: str) -> None:
    if not LOGGING_ENABLED:
        return
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}\n")
    except Exception:
        pass

def _log_exception(context: str, exc: Exception) -> None:
    try:
        _debug_log(f"{context}: {exc.__class__.__name__}: {_mask_sensitive_text(str(exc))}")
        tb = traceback.format_exc().strip()
        if tb and tb != "NoneType: None":
            _debug_log(_truncate_for_log(tb, 4000))
    except Exception:
        pass



@dataclass
class AppConfig:
    dx_host: str = ""
    dx_port: int = 7300
    dx_user: str = ""
    dx_pass: str = ""

    dx_filter: str = ""

    wx_location_name: str = ""
    map_image_path: str = "world_map.jpg"

    refresh_seconds: float = 1800.0
    map_refresh_seconds: float = 300.0  # 5 minutes

    time_refresh_seconds: float = 1.0
    enable_logging: bool = True

    online_lookup_website: str = "hamdb.org"
    online_lookup_username: str = ""
    online_lookup_password: str = ""

    database_type: str = "sqlite"
    sqlite_file_name: str = "hamcall.sqlite"
    logbook_default_station_callsign: str = ""

    mysql_enabled: bool = False
    mysql_host: str = ""
    mysql_port: int = 3306
    mysql_username: str = ""
    mysql_password: str = ""
    mysql_database: str = ""

    world_map_refresh_sec: float = 300.0
    space_weather_refresh_sec: float = 1800.0
    latitude_decimal: Optional[float] = None
    longitude_decimal: Optional[float] = None
    grid_square: str = ""
    local_weather_refresh_sec: float = 1800.0
    time_zone: str = "America/Edmonton"
    dx_cluster_host: str = ""
    dx_cluster_port: int = 7300
    dx_cluster_username: str = ""
    dx_cluster_password: str = ""
    rig_control_enabled: bool = False
    rigctld_host: str = "127.0.0.1"
    rigctld_port: int = 4532
    rigctld_poll_ms: int = 1000


@dataclass
class AppState:
    running: bool = True
    paused: bool = False

    menu_visible: bool = False
    file_menu_open: bool = False
    menu_selected_idx: int = 0  # 0 Settings, 1 MySQL, 2 Online Lookup, 3 Callsign Lookup, 4 DX Command, 5 Quit

    menu_win: Optional['curses.window'] = None
    menu_h: int = 0
    menu_w: int = 0
    menu_y: int = 0
    menu_x: int = 0

    space_weather_lines: List[str] = field(default_factory=list)
    dx_lines: List[str] = field(default_factory=list)
    wx_static_lines: List[str] = field(default_factory=list)   # non-time lines
    wx_time_lines: List[str] = field(default_factory=list)     # local/utc only
    dedx_lines: List[str] = field(default_factory=list)
    dedx_lookup_cache: dict = field(default_factory=dict)
    dedx_lookup_lock: "threading.Lock" = field(default_factory=threading.Lock)

    status_line: str = ""
    dx_status_line: str = "DX: idle"
    dx_cluster_ready: bool = False
    dx_spots: List[dict] = field(default_factory=list)
    dx_selected_idx: int = -1
    dx_auto_follow: bool = True
    dx_lock: "threading.Lock" = field(default_factory=threading.Lock)
    dirty_menu: bool = True
    dirty_space: bool = True
    dirty_dx: bool = True
    dirty_dedx: bool = True
    dx_cmd_queue: "queue.Queue[str]" = field(default_factory=queue.Queue)
    dx_spot_pairs: List[Tuple[Tuple[float, float], Tuple[float, float]]] = field(default_factory=list)  # last N (spotter, spotted) pairs
    dx_blink_on: bool = True
    dx_last_blink: float = 0.0
    dx_points_lock: "threading.Lock" = field(default_factory=threading.Lock)
    dx_points_file: str = ""
    lookup_provider_display: str = "hamdb.org"
    lookup_session_id_display: str = "-"
    online_lookup_status_line: str = "Lookup: hamdb.org sid=-"
    online_lookup_history: List[str] = field(default_factory=list)
    online_lookup_lock: "threading.Lock" = field(default_factory=threading.Lock)
    dirty_wx_static: bool = True
    dirty_wx_time: bool = True
    dirty_map: bool = True
    dirty_status: bool = True
    logging_mode: bool = False
    selected_station_callsign_id: Optional[int] = None
    selected_station_callsign: str = ""
    logbook_lines: List[str] = field(default_factory=list)
    dirty_logbook: bool = True
    logbook_selected_idx: int = 0
    logbook_recent_rows: List[dict] = field(default_factory=list)
    rig_frequency: str = "-"
    rig_mode: str = "-"
    rig_comp: bool = False
    rig_preamp: str = "Off"
    rig_agc: str = "-"
    rig_nb: bool = False
    rig_nr: bool = False
    rig_status_line: str = "Radio: disabled"
    dirty_rig: bool = True




# ---- config persistence ----
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hamclock_settings.json")

def load_config(path: str = CONFIG_FILE) -> AppConfig:
    """Load persisted configuration from JSON.

    Returns an AppConfig populated with defaults, overridden by any values found
    in the JSON file. Unknown keys in the file are ignored. Legacy weather/timezone
    keys are mapped into the single standardized field set.
    """
    cfg = AppConfig()

    if not os.path.exists(path):
        return cfg

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        # Corrupt/unreadable config: fall back to defaults
        return cfg

    legacy_map = {
        "wx_lat": "latitude_decimal",
        "wx_lon": "longitude_decimal",
        "wx_grid": "grid_square",
        "tz_label": "time_zone",
    }
    for old_key, new_key in legacy_map.items():
        if new_key not in data and old_key in data:
            data[new_key] = data.get(old_key)

    # Only accept known fields (avoid crashing on extra keys)
    for k in getattr(cfg, "__dataclass_fields__", {}).keys():
        if k in data:
            try:
                setattr(cfg, k, data[k])
            except Exception:
                pass

    normalize_config(cfg)
    set_logging_enabled(getattr(cfg, "enable_logging", True))
    return cfg

def save_config(cfg: AppConfig, path: str = CONFIG_FILE) -> None:
    normalize_config(cfg)
    set_logging_enabled(getattr(cfg, "enable_logging", True))
    data = {k: getattr(cfg, k) for k in cfg.__dataclass_fields__.keys()}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

def normalize_config(cfg: AppConfig) -> AppConfig:
    """Normalize menu-facing settings into the runtime fields still used elsewhere."""
    try:
        if not getattr(cfg, "dx_host", ""):
            cfg.dx_host = getattr(cfg, "dx_cluster_host", "") or getattr(cfg, "dx_host", "")
        if not getattr(cfg, "dx_user", ""):
            cfg.dx_user = getattr(cfg, "dx_cluster_username", "") or getattr(cfg, "dx_user", "")
        if not getattr(cfg, "dx_pass", ""):
            cfg.dx_pass = getattr(cfg, "dx_cluster_password", "") or getattr(cfg, "dx_pass", "")
        if not getattr(cfg, "dx_port", None):
            cfg.dx_port = int(getattr(cfg, "dx_cluster_port", 7300) or 7300)
    except Exception:
        pass

    try:
        cfg.dx_cluster_host = getattr(cfg, "dx_cluster_host", "") or getattr(cfg, "dx_host", "")
        cfg.dx_cluster_port = int(getattr(cfg, "dx_cluster_port", 0) or getattr(cfg, "dx_port", 7300) or 7300)
        cfg.dx_cluster_username = getattr(cfg, "dx_cluster_username", "") or getattr(cfg, "dx_user", "")
        cfg.dx_cluster_password = getattr(cfg, "dx_cluster_password", "") or getattr(cfg, "dx_pass", "")
    except Exception:
        pass

    try:
        cfg.refresh_seconds = float(getattr(cfg, "space_weather_refresh_sec", getattr(cfg, "refresh_seconds", 1800.0)) or 1800.0)
    except Exception:
        cfg.refresh_seconds = 1800.0
    try:
        cfg.map_refresh_seconds = float(getattr(cfg, "world_map_refresh_sec", getattr(cfg, "map_refresh_seconds", 300.0)) or 300.0)
    except Exception:
        cfg.map_refresh_seconds = 300.0
    try:
        cfg.local_weather_refresh_sec = float(getattr(cfg, "local_weather_refresh_sec", 1800.0) or 1800.0)
    except Exception:
        cfg.local_weather_refresh_sec = 1800.0

    cfg.grid_square = str(getattr(cfg, "grid_square", "") or "").strip()
    if not getattr(cfg, "time_zone", ""):
        cfg.time_zone = "America/Edmonton"

    db_type = str(getattr(cfg, "database_type", "sqlite") or "sqlite").strip().lower()
    cfg.database_type = "mysql" if db_type == "mysql" else "sqlite"
    cfg.mysql_enabled = (cfg.database_type == "mysql")

    if not getattr(cfg, "sqlite_file_name", ""):
        cfg.sqlite_file_name = CALLSIGN_DB_NAME

    if not getattr(cfg, "online_lookup_website", ""):
        cfg.online_lookup_website = "hamdb.org"

    return cfg

def _callsign_db_path_cfg(cfg: AppConfig) -> str:
    filename = getattr(cfg, "sqlite_file_name", "") or CALLSIGN_DB_NAME
    if os.path.isabs(filename):
        return filename
    return os.path.join(_script_dir(), filename)


def _db_is_mysql_cfg(cfg: AppConfig) -> bool:
    try:
        normalize_config(cfg)
    except Exception:
        pass
    return str(getattr(cfg, "database_type", "sqlite")).strip().lower() == "mysql"


def _db_param() -> str:
    return "%s"


def _db_connect_cfg(cfg: AppConfig):
    normalize_config(cfg)
    if _db_is_mysql_cfg(cfg):
        return _mysql_connect_cfg(cfg)
    return sqlite3.connect(_callsign_db_path_cfg(cfg))


def _db_close(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _db_backend_label(cfg: AppConfig) -> str:
    return "MySQL" if _db_is_mysql_cfg(cfg) else "SQLite"


def _db_target_desc(cfg: AppConfig) -> str:
    normalize_config(cfg)
    if _db_is_mysql_cfg(cfg):
        host = _clean_lookup_value(getattr(cfg, "mysql_host", "")) or "(host not set)"
        port = int(getattr(cfg, "mysql_port", 3306) or 3306)
        database = _clean_lookup_value(getattr(cfg, "mysql_database", "")) or "(database not set)"
        return f"MySQL {host}:{port}/{database}"
    return f"SQLite {_callsign_db_path_cfg(cfg)}"


def _logbook_table_exists(cur, cfg: AppConfig, table_name: str) -> bool:
    if _db_is_mysql_cfg(cfg):
        ph = _db_param()
        cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = {ph} AND table_name = {ph}", (getattr(cfg, "mysql_database", ""), table_name))
        row = cur.fetchone()
        return bool(row and int(row[0] or 0) > 0)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cur.fetchone() is not None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_today_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def utc_now_time() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def ensure_logbook_schema(cfg: AppConfig) -> None:
    backend_desc = _db_target_desc(cfg)
    conn = None
    try:
        conn = _db_connect_cfg(cfg)
        cur = conn.cursor()
        if _db_is_mysql_cfg(cfg):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS station_callsigns (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    callsign VARCHAR(32) NOT NULL,
                    display_name VARCHAR(64) NOT NULL DEFAULT '',
                    is_default INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_utc VARCHAR(32) NOT NULL,
                    updated_utc VARCHAR(32) NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qso_log (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    station_callsign_id INTEGER NOT NULL,
                    operator_callsign VARCHAR(32) NOT NULL,
                    qso_date VARCHAR(10) NOT NULL,
                    qso_time VARCHAR(8) NOT NULL,
                    worked_callsign VARCHAR(32) NOT NULL,
                    band VARCHAR(16) NOT NULL DEFAULT '',
                    frequency_khz REAL DEFAULT NULL,
                    mode VARCHAR(16) NOT NULL DEFAULT '',
                    submode VARCHAR(16) NOT NULL DEFAULT '',
                    rst_sent VARCHAR(8) NOT NULL DEFAULT '',
                    rst_recv VARCHAR(8) NOT NULL DEFAULT '',
                    their_name VARCHAR(64) NOT NULL DEFAULT '',
                    their_qth VARCHAR(128) NOT NULL DEFAULT '',
                    their_grid VARCHAR(16) NOT NULL DEFAULT '',
                    their_state_province VARCHAR(64) NOT NULL DEFAULT '',
                    their_country VARCHAR(64) NOT NULL DEFAULT '',
                    tx_power_w REAL DEFAULT NULL,
                    remarks TEXT,
                    created_utc VARCHAR(32) NOT NULL,
                    updated_utc VARCHAR(32) NOT NULL
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS station_callsigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    callsign TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    is_default INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qso_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_callsign_id INTEGER NOT NULL,
                    operator_callsign TEXT NOT NULL,
                    qso_date TEXT NOT NULL,
                    qso_time TEXT NOT NULL,
                    worked_callsign TEXT NOT NULL,
                    band TEXT NOT NULL DEFAULT '',
                    frequency_khz REAL DEFAULT NULL,
                    mode TEXT NOT NULL DEFAULT '',
                    submode TEXT NOT NULL DEFAULT '',
                    rst_sent TEXT NOT NULL DEFAULT '',
                    rst_recv TEXT NOT NULL DEFAULT '',
                    their_name TEXT NOT NULL DEFAULT '',
                    their_qth TEXT NOT NULL DEFAULT '',
                    their_grid TEXT NOT NULL DEFAULT '',
                    their_state_province TEXT NOT NULL DEFAULT '',
                    their_country TEXT NOT NULL DEFAULT '',
                    tx_power_w REAL DEFAULT NULL,
                    remarks TEXT,
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                )
            """)

        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_station_callsigns_callsign ON station_callsigns (callsign)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qso_log_station_callsign_id ON qso_log (station_callsign_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qso_log_qso_date_time ON qso_log (qso_date, qso_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qso_log_worked_callsign ON qso_log (worked_callsign)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qso_log_band ON qso_log (band)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qso_log_mode ON qso_log (mode)")
        conn.commit()

        missing = [name for name in ("station_callsigns", "qso_log") if not _logbook_table_exists(cur, cfg, name)]
        if missing:
            raise RuntimeError(f"Logbook schema verification failed on {backend_desc}; missing table(s): {', '.join(missing)}")
    except Exception as e:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        raise RuntimeError(f"Unable to create logbook tables on {backend_desc}: {e}") from e
    finally:
        if conn is not None:
            _db_close(conn)


def ensure_station_callsign(cfg: AppConfig, callsign: str, display_name: str = "", make_default: bool = False) -> Optional[int]:
    call = _normalize_callsign(callsign)
    if not call:
        return None

    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        now = utc_now_iso()
        ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"

        if make_default:
            cur.execute("UPDATE station_callsigns SET is_default = 0")

        cur.execute(f"SELECT id FROM station_callsigns WHERE callsign = {ph}", (call,))
        row = cur.fetchone()

        if row:
            sid = int(row[0])
            cur.execute(
                f"UPDATE station_callsigns SET display_name = {ph}, is_active = 1, updated_utc = {ph} WHERE id = {ph}",
                (display_name or "", now, sid),
            )
            if make_default:
                cur.execute(f"UPDATE station_callsigns SET is_default = 1 WHERE id = {ph}", (sid,))
            conn.commit()
            return sid

        cur.execute(
            f"INSERT INTO station_callsigns (callsign, display_name, is_default, is_active, created_utc, updated_utc) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            (call, display_name or "", 1 if make_default else 0, 1, now, now),
        )
        conn.commit()
        return int(getattr(cur, "lastrowid", 0) or 0)
    finally:
        _db_close(conn)


def get_station_callsigns(cfg: AppConfig) -> List[dict]:
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, callsign, display_name, is_default, is_active
            FROM station_callsigns
            WHERE is_active = 1
            ORDER BY is_default DESC, callsign ASC
        """)
        rows = cur.fetchall() or []
        return [
            {
                "id": int(r[0]),
                "callsign": str(r[1] or ""),
                "display_name": str(r[2] or ""),
                "is_default": bool(r[3]),
                "is_active": bool(r[4]),
            }
            for r in rows
        ]
    finally:
        _db_close(conn)


def select_default_station_callsign(cfg: AppConfig, state: AppState) -> None:
    preferred = _normalize_callsign(getattr(cfg, "logbook_default_station_callsign", "") or "")
    if preferred:
        try:
            ensure_station_callsign(cfg, preferred, make_default=True)
        except Exception:
            pass

    calls = get_station_callsigns(cfg)
    if not calls:
        state.selected_station_callsign_id = None
        state.selected_station_callsign = ""
        return

    chosen = None
    if preferred:
        for item in calls:
            if item["callsign"] == preferred:
                chosen = item
                break

    if chosen is None:
        for item in calls:
            if item["is_default"]:
                chosen = item
                break

    if chosen is None:
        chosen = calls[0]

    state.selected_station_callsign_id = chosen["id"]
    state.selected_station_callsign = chosen["callsign"]


def get_last_qsos(cfg: AppConfig, station_callsign_id: Optional[int] = None, limit: int = 10) -> List[dict]:
    limit = max(1, min(int(limit), 100))
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
        sql = """
            SELECT id, operator_callsign, qso_date, qso_time, worked_callsign,
                   band, frequency_khz, mode, rst_sent, rst_recv, their_grid,
                   their_country, remarks
            FROM qso_log
        """
        params = []
        if station_callsign_id:
            sql += f" WHERE station_callsign_id = {ph}"
            params.append(station_callsign_id)
        sql += " ORDER BY qso_date DESC, qso_time DESC, id DESC"
        sql += f" LIMIT {ph}"
        params.append(limit)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
        return [
            {
                "id": int(r[0]),
                "operator_callsign": str(r[1] or ""),
                "qso_date": str(r[2] or ""),
                "qso_time": str(r[3] or ""),
                "worked_callsign": str(r[4] or ""),
                "band": str(r[5] or ""),
                "frequency_khz": r[6],
                "mode": str(r[7] or ""),
                "rst_sent": str(r[8] or ""),
                "rst_recv": str(r[9] or ""),
                "their_grid": str(r[10] or ""),
                "their_country": str(r[11] or ""),
                "remarks": str(r[12] or ""),
            }
            for r in rows
        ]
    finally:
        _db_close(conn)


def refresh_logbook_lines(cfg: AppConfig, state: AppState) -> None:
    if not state.selected_station_callsign_id:
        state.logbook_recent_rows = []
        state.logbook_selected_idx = 0
        state.logbook_lines = [
            "Logbook mode",
            "",
            "No station callsign selected.",
            "Press C to add/select a station callsign.",
            "",
            "Keys: L=Exit  C=Callsigns  N=New QSO",
        ]
        state.dirty_logbook = True
        return

    rows = get_last_qsos(cfg, state.selected_station_callsign_id, limit=10)
    state.logbook_recent_rows = rows
    if not rows:
        state.logbook_selected_idx = 0
    else:
        state.logbook_selected_idx = max(0, min(int(getattr(state, "logbook_selected_idx", 0) or 0), len(rows) - 1))

    lines = [
        f"Logging for: {state.selected_station_callsign}",
        "Keys: L=Exit  N=New  E=Edit  D=Delete  C=Callsigns  Up/Down=Select",
        "",
    ]

    if not rows:
        lines.append("No contacts logged yet.")
    else:
        lines.append("Recent QSOs (last 10):")
        for i, r in enumerate(rows):
            freq = ""
            if r["frequency_khz"] not in (None, ""):
                try:
                    freq = f"{float(r['frequency_khz']):.1f}k"
                except Exception:
                    freq = str(r["frequency_khz"])
            prefix = ">" if i == state.logbook_selected_idx else " "
            lines.append(
                f"{prefix} {r['qso_date']} {r['qso_time'][:5]} "
                f"{r['worked_callsign']:<12.12} {r['band']:<5.5} "
                f"{r['mode']:<6.6} {freq:<10.10}"
            )
        sel = rows[state.logbook_selected_idx]
        lines.extend([
            "",
            f"Selected: #{sel['id']}  {sel['worked_callsign']}  {sel['qso_date']} {sel['qso_time']}",
            f"RST S/R: {sel['rst_sent'] or '-'} / {sel['rst_recv'] or '-'}    Grid: {sel['their_grid'] or '-'}    Country: {sel['their_country'] or '-'}",
            f"Remarks: {(sel['remarks'] or '-')[:80]}",
        ])

    state.logbook_lines = lines
    state.dirty_logbook = True



def _format_rig_display_frequency_from_hz(freq_hz) -> str:
    try:
        hz = int(float(freq_hz))
    except Exception:
        return "-"
    if hz < 0:
        return "-"
    mhz = hz // 1_000_000
    rem = hz % 1_000_000
    khz = rem // 1_000
    hz2 = (rem % 1_000) // 10
    return f"{mhz}.{khz:03d}.{hz2:02d}"

def _rig_frequency_to_khz_value(freq_text: str) -> str:
    raw = str(freq_text or "").strip()
    if not raw or raw == "-":
        return ""
    s = raw.replace(",", "").strip()
    # Handle display formats like 7.200.00 -> 7200.00
    if s.count(".") >= 2:
        parts = [p for p in s.split(".") if p != ""]
        if len(parts) >= 2:
            try:
                mhz = int(parts[0])
                khz = int(parts[1])
                hz = int(parts[2]) if len(parts) >= 3 else 0
                return f"{mhz * 1000 + khz}.{hz:02d}"
            except Exception:
                pass
    try:
        val = float(s)
        # rig panel displays MHz, QSO form expects kHz
        return f"{val * 1000.0:.2f}"
    except Exception:
        return ""

def _prefill_new_qso_from_rig(fields: list, state: AppState) -> None:
    fmap = _field_map(fields)
    freq_khz = _rig_frequency_to_khz_value(getattr(state, "rig_frequency", ""))
    mode = str(getattr(state, "rig_mode", "") or "").strip().upper()

    if freq_khz and "frequency_khz" in fmap and not str(fmap["frequency_khz"].get("value", "") or "").strip():
        fmap["frequency_khz"]["value"] = freq_khz

    if mode and mode != "-" and "mode" in fmap and not str(fmap["mode"].get("value", "") or "").strip():
        fmap["mode"]["value"] = mode

    if "frequency_khz" in fmap and "band" in fmap:
        guessed = _freq_to_band_name(fmap["frequency_khz"].get("value", ""))
        if guessed and not str(fmap["band"].get("value", "") or "").strip():
            fmap["band"]["value"] = guessed

def get_qso_by_id(cfg: AppConfig, qso_id: int) -> Optional[dict]:
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
        cur.execute(f"""
            SELECT id, station_callsign_id, operator_callsign, qso_date, qso_time,
                   worked_callsign, band, frequency_khz, mode, submode,
                   rst_sent, rst_recv, their_name, their_qth, their_grid,
                   their_state_province, their_country, tx_power_w, remarks,
                   created_utc, updated_utc
            FROM qso_log
            WHERE id = {ph}
        """, (qso_id,))
        r = cur.fetchone()
        if not r:
            return None
        return {
            "id": int(r[0]),
            "station_callsign_id": int(r[1]),
            "operator_callsign": str(r[2] or ""),
            "qso_date": str(r[3] or ""),
            "qso_time": str(r[4] or ""),
            "worked_callsign": str(r[5] or ""),
            "band": str(r[6] or ""),
            "frequency_khz": r[7],
            "mode": str(r[8] or ""),
            "submode": str(r[9] or ""),
            "rst_sent": str(r[10] or ""),
            "rst_recv": str(r[11] or ""),
            "their_name": str(r[12] or ""),
            "their_qth": str(r[13] or ""),
            "their_grid": str(r[14] or ""),
            "their_state_province": str(r[15] or ""),
            "their_country": str(r[16] or ""),
            "tx_power_w": r[17],
            "remarks": str(r[18] or ""),
            "created_utc": str(r[19] or ""),
            "updated_utc": str(r[20] or ""),
        }
    finally:
        _db_close(conn)


def insert_qso_log(cfg: AppConfig, state: AppState, entry: dict) -> int:
    if not state.selected_station_callsign_id or not state.selected_station_callsign:
        raise ValueError("No station callsign selected")
    worked_callsign = _normalize_callsign(entry.get("worked_callsign", ""))
    if not worked_callsign:
        raise ValueError("Worked callsign is required")

    now = utc_now_iso()
    qso_date = str(entry.get("qso_date") or utc_today_date())
    qso_time = str(entry.get("qso_time") or utc_now_time())[:8]
    freq_raw = str(entry.get("frequency_khz", "")).strip()
    pwr_raw = str(entry.get("tx_power_w", "")).strip()
    frequency_khz = _optional_float(entry.get("frequency_khz", ""))
    tx_power_w = _optional_float(entry.get("tx_power_w", ""))

    params = (
        state.selected_station_callsign_id,
        state.selected_station_callsign,
        qso_date,
        qso_time,
        worked_callsign,
        str(entry.get("band", "") or "").strip(),
        frequency_khz,
        str(entry.get("mode", "") or "").strip().upper(),
        str(entry.get("submode", "") or "").strip().upper(),
        str(entry.get("rst_sent", "") or "").strip(),
        str(entry.get("rst_recv", "") or "").strip(),
        str(entry.get("their_name", "") or "").strip(),
        str(entry.get("their_qth", "") or "").strip(),
        str(entry.get("their_grid", "") or "").strip().upper(),
        str(entry.get("their_state_province", "") or "").strip(),
        str(entry.get("their_country", "") or "").strip(),
        tx_power_w,
        str(entry.get("remarks", "") or "").strip(),
        now,
        now,
    )

    ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
    sql = f"""
        INSERT INTO qso_log (
            station_callsign_id, operator_callsign, qso_date, qso_time,
            worked_callsign, band, frequency_khz, mode, submode,
            rst_sent, rst_recv, their_name, their_qth, their_grid,
            their_state_province, their_country, tx_power_w, remarks,
            created_utc, updated_utc
        ) VALUES ({", ".join([ph] * 20)})
    """
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return int(cur.lastrowid)
    finally:
        _db_close(conn)


def update_qso_log(cfg: AppConfig, qso_id: int, entry: dict) -> None:
    worked_callsign = _normalize_callsign(entry.get("worked_callsign", ""))
    if not worked_callsign:
        raise ValueError("Worked callsign is required")

    now = utc_now_iso()
    qso_date = str(entry.get("qso_date") or utc_today_date())
    qso_time = str(entry.get("qso_time") or utc_now_time())[:8]
    freq_raw = str(entry.get("frequency_khz", "")).strip()
    pwr_raw = str(entry.get("tx_power_w", "")).strip()
    frequency_khz = _optional_float(entry.get("frequency_khz", ""))
    tx_power_w = _optional_float(entry.get("tx_power_w", ""))

    ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
    sql = f"""
        UPDATE qso_log SET
            qso_date = {ph},
            qso_time = {ph},
            worked_callsign = {ph},
            band = {ph},
            frequency_khz = {ph},
            mode = {ph},
            submode = {ph},
            rst_sent = {ph},
            rst_recv = {ph},
            their_name = {ph},
            their_qth = {ph},
            their_grid = {ph},
            their_state_province = {ph},
            their_country = {ph},
            tx_power_w = {ph},
            remarks = {ph},
            updated_utc = {ph}
        WHERE id = {ph}
    """
    params = (
        qso_date,
        qso_time,
        worked_callsign,
        str(entry.get("band", "") or "").strip(),
        frequency_khz,
        str(entry.get("mode", "") or "").strip().upper(),
        str(entry.get("submode", "") or "").strip().upper(),
        str(entry.get("rst_sent", "") or "").strip(),
        str(entry.get("rst_recv", "") or "").strip(),
        str(entry.get("their_name", "") or "").strip(),
        str(entry.get("their_qth", "") or "").strip(),
        str(entry.get("their_grid", "") or "").strip().upper(),
        str(entry.get("their_state_province", "") or "").strip(),
        str(entry.get("their_country", "") or "").strip(),
        tx_power_w,
        str(entry.get("remarks", "") or "").strip(),
        now,
        qso_id,
    )
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
    finally:
        _db_close(conn)


def delete_qso_log(cfg: AppConfig, qso_id: int) -> None:
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
        cur.execute(f"DELETE FROM qso_log WHERE id = {ph}", (qso_id,))
        conn.commit()
    finally:
        _db_close(conn)


def set_default_station_callsign(cfg: AppConfig, station_id: int) -> None:
    conn = _db_connect_cfg(cfg)
    try:
        cur = conn.cursor()
        ph = _db_param() if _db_is_mysql_cfg(cfg) else "?"
        cur.execute("UPDATE station_callsigns SET is_default = 0")
        cur.execute(f"UPDATE station_callsigns SET is_default = 1 WHERE id = {ph}", (station_id,))
        conn.commit()
    finally:
        _db_close(conn)


def _move_logbook_selection(state: AppState, delta: int) -> None:
    rows = list(getattr(state, "logbook_recent_rows", []) or [])
    if not rows:
        state.logbook_selected_idx = 0
        return
    idx = int(getattr(state, "logbook_selected_idx", 0) or 0) + int(delta)
    idx = max(0, min(len(rows) - 1, idx))
    state.logbook_selected_idx = idx
    state.dirty_logbook = True
    state.dirty_map = True


def _refresh_logbook_view(cfg: AppConfig, state: AppState) -> None:
    refresh_logbook_lines(cfg, state)


def _popup_menu(stdscr, title: str, items: List[str], _line: str = "Enter=Select Esc=Cancel") -> int:
    if not items:
        return -1
    idx = 0
    while True:
        maxy, maxx = stdscr.getmaxyx()
        inner_w = min(maxx - 6, max(36, min(100, max(len(title) + 4, max(len(i) for i in items) + 6, len(help_line) + 4))))
        h = min(maxy - 4, max(8, len(items) + 4))
        y0 = max(1, (maxy - h) // 2)
        x0 = max(1, (maxx - inner_w) // 2)
        win = curses.newwin(h, inner_w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, f" {title} ", curses.A_BOLD)
        visible = max(1, h - 3)
        top = 0
        if idx >= visible:
            top = idx - visible + 1
        for i in range(top, min(len(items), top + visible)):
            attr = curses.A_REVERSE if i == idx else 0
            safe_addstr(win, 1 + (i - top), 2, items[i][: max(1, inner_w - 4)], attr)
        safe_addstr(win, h - 2, 2, help_line[: max(1, inner_w - 4)], curses.A_DIM)
        win.refresh()
        k = win.getch()
        if k in (27, ord('q'), ord('Q')):
            return -1
        if k in (curses.KEY_UP, ord('k')):
            idx = max(0, idx - 1)
        elif k in (curses.KEY_DOWN, ord('j')):
            idx = min(len(items) - 1, idx + 1)
        elif k in (10, 13, curses.KEY_ENTER):
            return idx


def _confirm_dialog(stdscr, title: str, message: str) -> bool:
    lines = [line for line in str(message).splitlines()] or [""]
    lines.append("")
    lines.append("Y = Yes    N/Esc = No")
    maxy, maxx = stdscr.getmaxyx()
    inner_w = min(maxx - 6, max(40, min(100, max(len(title) + 4, *(len(line) + 4 for line in lines)))))
    h = min(maxy - 4, max(7, len(lines) + 3))
    y0 = max(1, (maxy - h) // 2)
    x0 = max(1, (maxx - inner_w) // 2)
    win = curses.newwin(h, inner_w, y0, x0)
    win.keypad(True)
    while True:
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, f" {title} ", curses.A_BOLD)
        for i, line in enumerate(lines[: max(1, h - 2)]):
            safe_addstr(win, 1 + i, 2, line[: max(1, inner_w - 4)])
        win.refresh()
        k = win.getch()
        if k in (ord('y'), ord('Y')):
            return True
        if k in (ord('n'), ord('N'), 27):
            return False


def _normalize_time_hms(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return utc_now_time()
    if re.fullmatch(r"\d{4}", s):
        return f"{s[:2]}:{s[2:4]}:00"
    if re.fullmatch(r"\d{2}:\d{2}", s):
        return s + ":00"
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", s):
        return s
    return s[:8]



def _freq_to_band_name(freq_khz) -> str:
    try:
        f = float(freq_khz)
    except Exception:
        return ""
    bands = [
        (135.7, 137.8, "2200m"),
        (472.0, 479.0, "630m"),
        (1800.0, 2000.0, "160m"),
        (3500.0, 4000.0, "80m"),
        (5330.0, 5406.5, "60m"),
        (7000.0, 7300.0, "40m"),
        (10100.0, 10150.0, "30m"),
        (14000.0, 14350.0, "20m"),
        (18068.0, 18168.0, "17m"),
        (21000.0, 21450.0, "15m"),
        (24890.0, 24990.0, "12m"),
        (28000.0, 29700.0, "10m"),
        (50000.0, 54000.0, "6m"),
        (70000.0, 71000.0, "4m"),
        (144000.0, 148000.0, "2m"),
        (222000.0, 225000.0, "1.25m"),
        (420000.0, 450000.0, "70cm"),
        (902000.0, 928000.0, "33cm"),
        (1240000.0, 1300000.0, "23cm"),
    ]
    for lo, hi, name in bands:
        if lo <= f <= hi:
            return name
    return ""


def _optional_float(value):
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "none":
        return None
    return float(s)

def _draw_qso_form(win, title, fields, idx, message=""):
    win.erase()
    win.box()
    h, w = win.getmaxyx()
    safe_addstr(win, 0, 2, f" {title} ", curses.A_BOLD)
    safe_addstr(win, 1, 2, "Type to edit  Tab/Shift-Tab move  Enter/F2 save  Esc cancel"[: max(1, w - 4)], curses.A_DIM)
    safe_addstr(win, 2, 2, "Band auto-fills from Frequency. UTC date/time set when saved."[: max(1, w - 4)], curses.A_DIM)

    top_y = 4
    label_w = 20
    val_x = 2 + label_w
    val_w = max(12, w - val_x - 3)
    visible_rows = max(1, h - 8)

    scroll = 0
    if idx >= visible_rows:
        scroll = idx - visible_rows + 1

    visible = fields[scroll: scroll + visible_rows]
    for row, field in enumerate(visible):
        y = top_y + row
        is_sel = (scroll + row) == idx
        style = curses.A_REVERSE if is_sel else curses.A_NORMAL
        label = str(field.get("label", ""))[: label_w - 1]
        value = str(field.get("value", "") or "")
        if is_sel and not field.get("readonly"):
            value = value + "_"
        if field.get("readonly"):
            value_style = style | curses.A_DIM
        else:
            value_style = style
        safe_addstr(win, y, 2, f"{label:<{label_w-1}}", style)
        safe_addstr(win, y, val_x, value[:val_w], value_style)
        field["_screen_y"] = y
        field["_screen_x"] = val_x

    if message:
        safe_addstr(win, h - 3, 2, message[: max(1, w - 4)], curses.A_BOLD)
    safe_addstr(win, h - 2, 2, "Tab move/lookup call  Enter/F2 save  F5 force lookup"[: max(1, w - 4)], curses.A_DIM)
    win.refresh()

def _qso_fields_from_existing(existing: dict) -> list:
    return [
        {"key": "qso_date", "label": "UTC Date", "value": existing.get("qso_date", "") if existing else "", "readonly": True},
        {"key": "qso_time", "label": "UTC Time", "value": existing.get("qso_time", "") if existing else "", "readonly": True},
        {"key": "worked_callsign", "label": "Worked Callsign", "value": existing.get("worked_callsign", "") if existing else ""},
        {"key": "frequency_khz", "label": "Frequency kHz", "value": "" if not existing or existing.get("frequency_khz") in (None, "") else str(existing.get("frequency_khz"))},
        {"key": "band", "label": "Band", "value": existing.get("band", "") if existing else ""},
        {"key": "mode", "label": "Mode", "value": existing.get("mode", "") if existing else ""},
        {"key": "submode", "label": "Submode", "value": existing.get("submode", "") if existing else ""},
        {"key": "rst_sent", "label": "RST Sent", "value": existing.get("rst_sent", "59") if existing else "59"},
        {"key": "rst_recv", "label": "RST Recv", "value": existing.get("rst_recv", "59") if existing else "59"},
        {"key": "their_name", "label": "Their Name", "value": existing.get("their_name", "") if existing else ""},
        {"key": "their_qth", "label": "Their QTH", "value": existing.get("their_qth", "") if existing else ""},
        {"key": "their_grid", "label": "Their Grid", "value": existing.get("their_grid", "") if existing else ""},
        {"key": "their_state_province", "label": "State/Province", "value": existing.get("their_state_province", "") if existing else ""},
        {"key": "their_country", "label": "Country", "value": existing.get("their_country", "") if existing else ""},
        {"key": "tx_power_w", "label": "TX Power W", "value": "" if not existing or existing.get("tx_power_w") in (None, "") else str(existing.get("tx_power_w"))},
        {"key": "remarks", "label": "Remarks", "value": existing.get("remarks", "") if existing else ""},
    ]



def _field_map(fields: list) -> dict:
    return {str(f.get("key", "")): f for f in fields}


def _coalesce_lookup_value(*values) -> str:
    for value in values:
        cleaned = _clean_lookup_value(value)
        if cleaned:
            return cleaned
    return ""


def _extract_qso_autofill_from_payload(payload: dict, callsign: str) -> dict:
    if not isinstance(payload, dict):
        payload = {}

    source_payload = None
    source_name = "calls"
    for candidate in ("hamdb", "hamqth", "qrz"):
        cand = payload.get(candidate)
        if isinstance(cand, dict) and cand:
            source_payload = cand
            source_name = candidate
            break
    if source_payload is None:
        source_payload = payload

    first = _coalesce_lookup_value(source_payload.get("fname"), source_payload.get("first_name"), source_payload.get("first"))
    middle = _coalesce_lookup_value(source_payload.get("mi"), source_payload.get("middle"), source_payload.get("middle_name"))
    last = _coalesce_lookup_value(source_payload.get("name"), source_payload.get("last_name"), source_payload.get("surname"))
    full_name = _format_lookup_name(first, middle, last) or _coalesce_lookup_value(source_payload.get("adr_name"))

    city = _coalesce_lookup_value(source_payload.get("addr2"), source_payload.get("adr_city"), source_payload.get("city"))
    prov = _coalesce_lookup_value(
        source_payload.get("state"),
        source_payload.get("prov_state"),
        source_payload.get("district"),
        source_payload.get("us_state"),
        source_payload.get("province"),
    )
    country = _coalesce_lookup_value(
        source_payload.get("country"),
        source_payload.get("mailing_country"),
        source_payload.get("prefix_country"),
        source_payload.get("dxcc_name"),
        source_payload.get("adr_country"),
    )
    street = _coalesce_lookup_value(source_payload.get("street"), source_payload.get("address"))
    postal = _coalesce_lookup_value(source_payload.get("zip"), source_payload.get("postal_code"))
    grid = _coalesce_lookup_value(source_payload.get("grid"), source_payload.get("grid_square"), source_payload.get("gridsquare"))

    qth_parts = [p for p in (street, city, prov, postal, country) if p]
    qth = ", ".join(qth_parts)

    return {
        "callsign": _normalize_callsign(source_payload.get("call") or source_payload.get("callsign") or callsign),
        "their_name": full_name,
        "their_qth": qth,
        "their_grid": grid.upper(),
        "their_state_province": prov,
        "their_country": country,
        "source": source_name,
    }


def _lookup_qso_autofill_local_db(conn, callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {}

    ph = _param_placeholder_for_conn(conn)

    # Prefer richer payload from calls table if available.
    try:
        cols = _table_columns_generic(conn, "calls")
        if "callsign" in cols and "payload_json" in cols:
            cur = conn.cursor()
            cur.execute(f"SELECT payload_json FROM calls WHERE UPPER(callsign)=UPPER({ph}) LIMIT 1", (callsign,))
            row = cur.fetchone()
            if row and row[0]:
                try:
                    payload = json.loads(row[0] or "{}")
                except Exception:
                    payload = {}
                result = _extract_qso_autofill_from_payload(payload, callsign)
                if any(result.get(k) for k in ("their_name", "their_qth", "their_grid", "their_state_province", "their_country")):
                    return result
    except Exception:
        pass

    # Fall back to hamcall_calls columns when available.
    try:
        cols = _table_columns_generic(conn, "hamcall_calls")
        if "callsign" in cols:
            wanted = [c for c in (
                "callsign", "first_name", "middle", "last_name", "street", "city",
                "state_province", "postal_code", "grid", "mailing_country", "prefix_country", "dxcc_name"
            ) if c in cols]
            if wanted:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT {', '.join(wanted)} FROM hamcall_calls WHERE UPPER(callsign)=UPPER({ph}) LIMIT 1",
                    (callsign,),
                )
                row = cur.fetchone()
                if row:
                    data = dict(zip(wanted, row))
                    full_name = _format_lookup_name(data.get("first_name"), data.get("middle"), data.get("last_name"))
                    city = _clean_lookup_value(data.get("city"))
                    prov = _clean_lookup_value(data.get("state_province"))
                    country = _coalesce_lookup_value(data.get("mailing_country"), data.get("prefix_country"), data.get("dxcc_name"))
                    street = _clean_lookup_value(data.get("street"))
                    postal = _clean_lookup_value(data.get("postal_code"))
                    qth = ", ".join([p for p in (street, city, prov, postal, country) if p])
                    return {
                        "callsign": _normalize_callsign(data.get("callsign") or callsign),
                        "their_name": full_name,
                        "their_qth": qth,
                        "their_grid": _clean_lookup_value(data.get("grid")).upper(),
                        "their_state_province": prov,
                        "their_country": country,
                        "source": "hamcall_calls",
                    }
    except Exception:
        pass

    return {}


def lookup_qso_autofill_info(cfg: AppConfig, state: AppState, callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {
            "callsign": "",
            "their_name": "",
            "their_qth": "",
            "their_grid": "",
            "their_state_province": "",
            "their_country": "",
            "source": "not found",
        }

    base = get_cached_callsign_info(cfg, state, callsign)
    details = {}
    try:
        conn = _db_connect_cfg(cfg)
        try:
            details = _lookup_qso_autofill_local_db(conn, callsign)
        finally:
            _db_close(conn)
    except Exception:
        details = {}

    their_name = _coalesce_lookup_value(details.get("their_name"), base.get("name"))
    their_qth = _coalesce_lookup_value(details.get("their_qth"))
    if not their_qth:
        their_qth = ", ".join([p for p in (
            _clean_lookup_value(base.get("city")),
            _clean_lookup_value(base.get("prov_state")),
            _clean_lookup_value(base.get("country")),
        ) if p])

    result = {
        "callsign": callsign,
        "their_name": their_name,
        "their_qth": their_qth,
        "their_grid": _coalesce_lookup_value(details.get("their_grid")).upper(),
        "their_state_province": _coalesce_lookup_value(details.get("their_state_province"), base.get("prov_state")),
        "their_country": _coalesce_lookup_value(details.get("their_country"), base.get("country")),
        "source": _coalesce_lookup_value(details.get("source"), base.get("source")) or "not found",
    }

    if result["source"] == "not found" and not result["their_name"]:
        result["their_name"] = "Not Found"

    return result


def _apply_qso_lookup_to_fields(fields: list, lookup: dict, overwrite: bool = False) -> None:
    fmap = _field_map(fields)
    if "worked_callsign" in fmap and lookup.get("callsign"):
        fmap["worked_callsign"]["value"] = _normalize_callsign(lookup.get("callsign"))

    mapping = {
        "their_name": lookup.get("their_name", ""),
        "their_qth": lookup.get("their_qth", ""),
        "their_grid": _clean_lookup_value(lookup.get("their_grid")).upper(),
        "their_state_province": lookup.get("their_state_province", ""),
        "their_country": lookup.get("their_country", ""),
    }
    for key, new_value in mapping.items():
        if key not in fmap:
            continue
        cur = _clean_lookup_value(fmap[key].get("value"))
        val = _clean_lookup_value(new_value)
        if not val:
            continue
        if overwrite or not cur or cur == "Not Found":
            fmap[key]["value"] = val


def _lookup_and_fill_qso_fields(cfg: AppConfig, state: AppState, fields: list, overwrite: bool = False) -> tuple:
    fmap = _field_map(fields)
    worked = _normalize_callsign(fmap.get("worked_callsign", {}).get("value", ""))
    if not worked:
        return False, ""
    info = lookup_qso_autofill_info(cfg, state, worked)
    _apply_qso_lookup_to_fields(fields, info, overwrite=overwrite)
    src = _clean_lookup_value(info.get("source")) or "not found"
    return True, f"Lookup {worked}: {src}"


def qso_entry_dialog(stdscr, cfg: AppConfig, state: AppState, qso_id: Optional[int] = None) -> bool:
    existing = get_qso_by_id(cfg, qso_id) if qso_id else {}
    fields = _qso_fields_from_existing(existing)
    if not qso_id:
        _prefill_new_qso_from_rig(fields, state)
    title = "Edit QSO" if qso_id else "New QSO"
    idx = 2
    message = ""
    last_lookup_callsign = _normalize_callsign(existing.get("worked_callsign", "")) if existing else ""
    curses.curs_set(1)
    stdscr.nodelay(False)

    def _clear_qso_lookup_fields() -> None:
        fmap = _field_map(fields)
        for key in ("their_name", "their_qth", "their_grid", "their_state_province", "their_country"):
            if key in fmap:
                fmap[key]["value"] = ""

    def _lookup_on_tab_from_worked_callsign() -> None:
        nonlocal message, last_lookup_callsign
        if qso_id:
            return
        fmap = _field_map(fields)
        current_call = _normalize_callsign(fmap.get("worked_callsign", {}).get("value", ""))
        _clear_qso_lookup_fields()
        if not current_call:
            last_lookup_callsign = ""
            message = ""
            return
        try:
            ok, status = _lookup_and_fill_qso_fields(cfg, state, fields, overwrite=True)
            last_lookup_callsign = current_call
            message = status if ok else f"Lookup {current_call}: not found"
        except Exception as e:
            last_lookup_callsign = current_call
            fmap = _field_map(fields)
            if "their_name" in fmap and not _clean_lookup_value(fmap["their_name"].get("value")):
                fmap["their_name"]["value"] = "Not Found"
            message = f"Lookup failed: {e}"

    def _force_lookup_current_call() -> None:
        nonlocal message, last_lookup_callsign
        fmap = _field_map(fields)
        current_call = _normalize_callsign(fmap.get("worked_callsign", {}).get("value", ""))
        _clear_qso_lookup_fields()
        if not current_call:
            last_lookup_callsign = ""
            message = ""
            return
        try:
            ok, status = _lookup_and_fill_qso_fields(cfg, state, fields, overwrite=True)
            last_lookup_callsign = current_call
            message = status if ok else f"Lookup {current_call}: not found"
        except Exception as e:
            last_lookup_callsign = current_call
            fmap = _field_map(fields)
            if "their_name" in fmap and not _clean_lookup_value(fmap["their_name"].get("value")):
                fmap["their_name"]["value"] = "Not Found"
            message = f"Lookup failed: {e}"

    def _save_current_form():
        nonlocal message

        values = {f["key"]: str(f.get("value", "") or "") for f in fields}
        values["worked_callsign"] = _normalize_callsign(values.get("worked_callsign", ""))
        values["mode"] = str(values.get("mode", "")).strip().upper()
        values["submode"] = str(values.get("submode", "")).strip().upper()
        values["their_grid"] = str(values.get("their_grid", "")).strip().upper()

        if not values["worked_callsign"]:
            message = "Worked callsign is required."
            return False

        values["qso_date"] = utc_today_date()
        values["qso_time"] = utc_now_time()

        try:
            values["frequency_khz"] = _optional_float(values.get("frequency_khz", ""))
        except Exception:
            message = "Frequency must be numeric."
            return False

        try:
            values["tx_power_w"] = _optional_float(values.get("tx_power_w", ""))
        except Exception:
            message = "TX power must be numeric."
            return False

        guessed = _freq_to_band_name(values.get("frequency_khz"))
        if guessed:
            values["band"] = guessed

        try:
            if qso_id:
                update_qso_log(cfg, qso_id, values)
                state.status_line = f"Updated QSO #{qso_id}"
            else:
                new_id = insert_qso_log(cfg, state, values)
                state.status_line = f"Logged QSO #{new_id} with {values['worked_callsign']}"
            _refresh_logbook_view(cfg, state)
            state.dirty_status = True
            curses.curs_set(0)
            return True
        except Exception as e:
            message = f"Save failed: {e}"
            return False

    def _apply_field_normalization(field):
        key = field["key"]
        if key == "worked_callsign":
            field["value"] = _normalize_callsign(field.get("value", ""))
        elif key in ("mode", "submode"):
            field["value"] = str(field.get("value", "") or "").strip().upper()
        elif key == "their_grid":
            field["value"] = str(field.get("value", "") or "").strip().upper()
        elif key == "frequency_khz":
            guessed = _freq_to_band_name(field.get("value", ""))
            if guessed:
                for f in fields:
                    if f["key"] == "band":
                        f["value"] = guessed
                        break

    try:
        while True:
            maxy, maxx = stdscr.getmaxyx()
            h = min(maxy - 2, max(18, len(fields) + 8))
            w = min(maxx - 4, max(76, min(110, maxx - 4)))
            y0 = max(1, (maxy - h) // 2)
            x0 = max(1, (maxx - w) // 2)
            win = curses.newwin(h, w, y0, x0)
            win.keypad(True)

            fields[0]["value"] = utc_today_date()
            fields[1]["value"] = utc_now_time()

            freq_field = next((f for f in fields if f["key"] == "frequency_khz"), None)
            band_field = next((f for f in fields if f["key"] == "band"), None)
            if freq_field and band_field:
                guessed = _freq_to_band_name(freq_field.get("value", ""))
                if guessed:
                    band_field["value"] = guessed

            _draw_qso_form(win, title, fields, idx, message)
            k = win.getch()
            prev_idx = idx
            message = ""

            if k in (27,):
                return False
            elif k in (9, getattr(curses, 'KEY_TAB', -1)):
                if fields[idx]["key"] == "worked_callsign":
                    _lookup_on_tab_from_worked_callsign()
                idx = (idx + 1) % len(fields)
            elif k in (getattr(curses, 'KEY_BTAB', 353), 353):
                idx = (idx - 1) % len(fields)
            elif k in (curses.KEY_UP, ord('k')):
                idx = max(0, idx - 1)
            elif k in (curses.KEY_DOWN, ord('j')):
                idx = min(len(fields) - 1, idx + 1)
            elif k == curses.KEY_F5:
                _force_lookup_current_call()
            elif k == curses.KEY_F2:
                if _save_current_form():
                    return True
            elif k in (10, 13, curses.KEY_ENTER):
                if _save_current_form():
                    return True
            else:
                field = fields[idx]
                if field.get("readonly"):
                    continue

                current = str(field.get("value", "") or "")

                if k in (curses.KEY_BACKSPACE, 127, 8):
                    field["value"] = current[:-1]
                    _apply_field_normalization(field)
                    if field["key"] == "worked_callsign":
                        last_lookup_callsign = ""
                elif k == 21:  # Ctrl+U
                    field["value"] = ""
                    _apply_field_normalization(field)
                    if field["key"] == "worked_callsign":
                        last_lookup_callsign = ""
                elif 32 <= k <= 126:
                    field["value"] = current + chr(k)
                    _apply_field_normalization(field)
                    if field["key"] == "worked_callsign":
                        last_lookup_callsign = ""

            if idx != prev_idx and fields[prev_idx]["key"] == "worked_callsign":
                # already handled above, but keep behavior if movement keys are expanded later
                pass
    finally:
        try:
            curses.curs_set(0)
        except Exception:
            pass
        try:
            stdscr.timeout(100)
        except Exception:
            pass

def callsign_dialog(stdscr, cfg: AppConfig, state: AppState) -> bool:
    while True:
        calls = get_station_callsigns(cfg)
        items = [f"Use: {c['callsign']}{' [default]' if c['is_default'] else ''}" for c in calls]
        items += ["Add new callsign", "Set selected callsign as default", "Cancel"]
        idx = _popup_menu(stdscr, "Station Callsigns", items)
        if idx < 0 or idx == len(items) - 1:
            return False
        if idx < len(calls):
            chosen = calls[idx]
            state.selected_station_callsign_id = chosen["id"]
            state.selected_station_callsign = chosen["callsign"]
            cfg.logbook_default_station_callsign = chosen["callsign"]
            _refresh_logbook_view(cfg, state)
            state.status_line = f"Logging for {chosen['callsign']}"
            state.dirty_status = True
            return True
        if idx == len(calls):
            maxy, maxx = stdscr.getmaxyx()
            w = min(60, maxx - 4)
            y0 = max(1, maxy // 2 - 2)
            x0 = max(1, (maxx - w) // 2)
            call = _input_box(stdscr, y0, x0, w, "New Callsign: ", "")
            if not call:
                continue
            display = _input_box(stdscr, y0 + 3, x0, w, "Display Name: ", "")
            make_default = _confirm_dialog(stdscr, "Default Callsign", f"Make {_normalize_callsign(call)} the default callsign?")
            try:
                sid = ensure_station_callsign(cfg, call, display_name=display, make_default=make_default)
                select_default_station_callsign(cfg, state)
                if sid:
                    calls2 = get_station_callsigns(cfg)
                    for c in calls2:
                        if c["id"] == sid:
                            state.selected_station_callsign_id = c["id"]
                            state.selected_station_callsign = c["callsign"]
                            cfg.logbook_default_station_callsign = c["callsign"]
                            break
                _refresh_logbook_view(cfg, state)
                state.status_line = f"Added callsign {state.selected_station_callsign or _normalize_callsign(call)}"
                state.dirty_status = True
                return True
            except Exception as e:
                _simple_message_popup(stdscr, "Callsign", f"Unable to add callsign:\n{e}")
                state.status_line = f"Callsign add failed: {e}"
                state.dirty_status = True
                continue
        if idx == len(calls) + 1:
            if not state.selected_station_callsign_id:
                _simple_message_popup(stdscr, "Callsign", "No callsign is currently selected.")
                continue
            try:
                set_default_station_callsign(cfg, state.selected_station_callsign_id)
                cfg.logbook_default_station_callsign = state.selected_station_callsign
                _refresh_logbook_view(cfg, state)
                state.status_line = f"Default callsign set to {state.selected_station_callsign}"
                state.dirty_status = True
                return True
            except Exception as e:
                _simple_message_popup(stdscr, "Callsign", f"Unable to set default:\n{e}")
                state.status_line = f"Default callsign failed: {e}"
                state.dirty_status = True
                continue


def delete_selected_qso_dialog(stdscr, cfg: AppConfig, state: AppState) -> bool:
    rows = list(getattr(state, "logbook_recent_rows", []) or [])
    idx = int(getattr(state, "logbook_selected_idx", 0) or 0)
    if not rows or idx < 0 or idx >= len(rows):
        _simple_message_popup(stdscr, "Delete QSO", "No recent QSO is selected.")
        return False
    row = rows[idx]
    msg = f"Delete QSO #{row['id']}?\n{row['qso_date']} {row['qso_time']}  {row['worked_callsign']}  {row['band']} {row['mode']}"
    if not _confirm_dialog(stdscr, "Delete QSO", msg):
        return False
    try:
        delete_qso_log(cfg, int(row["id"]))
        _refresh_logbook_view(cfg, state)
        if state.logbook_recent_rows:
            state.logbook_selected_idx = min(idx, len(state.logbook_recent_rows) - 1)
        else:
            state.logbook_selected_idx = 0
        state.status_line = f"Deleted QSO #{row['id']}"
        state.dirty_status = True
        state.dirty_logbook = True
        state.dirty_map = True
        return True
    except Exception as e:
        _simple_message_popup(stdscr, "Delete QSO", f"Delete failed:\n{e}")
        state.status_line = f"Delete failed: {e}"
        state.dirty_status = True
        return False


def apply_config_runtime(cfg: AppConfig, state: AppState) -> None:
    normalize_config(cfg)
    try:
        initialize_online_lookup_session(cfg, state)
    except Exception:
        pass
    try:
        with state.dedx_lookup_lock:
            state.dedx_lookup_cache.clear()
    except Exception:
        pass
    state.dirty_space = True
    state.dirty_dx = True
    state.dirty_wx_static = True
    state.dirty_wx_time = True
    state.dirty_rig = True
    state.dirty_dedx = True
    state.dirty_map = True
    state.dirty_status = True


def _test_sqlite_connection_cfg(cfg: AppConfig):
    db_path = _callsign_db_path_cfg(cfg)
    try:
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            conn.close()
        return True, f"Connected to sqlite database: {db_path}"
    except Exception as e:
        return False, str(e)


def _test_database_connection_cfg(cfg: AppConfig):
    normalize_config(cfg)
    if getattr(cfg, "database_type", "sqlite") == "mysql":
        return _test_mysql_connection_cfg(cfg)
    return _test_sqlite_connection_cfg(cfg)


def _input_box(stdscr, y0, x0, w, prompt, initial=""):
    win = curses.newwin(3, w, y0, x0)
    win.erase()
    win.box()
    safe_addstr(win, 1, 2, prompt[: max(1, w - 4)])
    if initial:
        safe_addstr(win, 1, min(w - 3, 2 + len(prompt)), str(initial)[: max(0, w - (4 + len(prompt)))])
    win.refresh()
    curses.echo()
    try:
        raw = win.getstr(1, 2 + len(prompt), max(1, w - (4 + len(prompt))))
        return raw.decode("utf-8", errors="ignore") if raw is not None else ""
    except Exception:
        return initial
    finally:
        curses.noecho()




def _rig_format_freq_hz(hz_value) -> str:
    try:
        hz = int(float(str(hz_value).strip()))
        if hz <= 0:
            return "-"
        return f"{hz / 1_000_000:.5f}"
    except Exception:
        return "-"


def _rigctld_command(host: str, port: int, cmd: str, timeout: float = 1.5) -> str:
    import socket
    with socket.create_connection((host, int(port)), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall((cmd.rstrip("\n") + "\n").encode("ascii", errors="ignore"))
        data = s.recv(4096)
    return data.decode("utf-8", errors="replace").strip()




def _rigctld_bool_token(value: str) -> bool:
    v = str(value or "").strip().upper()
    return v in ("1", "ON", "TRUE", "T", "YES", "Y")

def _rig_parse_preamp(value: str) -> str:
    try:
        n = int(float(str(value).strip()))
        if n <= 0:
            return "Off"
        return str(n)
    except Exception:
        return "Off"

def _rig_parse_agc(value: str) -> str:
    raw = str(value or "").strip()
    up = raw.upper()
    if up in ("SLOW", "MED", "MEDIUM", "FAST", "OFF"):
        return {"MEDIUM": "Med"}.get(up, up.title() if up != "OFF" else "Off")
    try:
        n = int(float(raw))
    except Exception:
        return "-"
    # Common Hamlib/rigctld numeric mapping fallback
    if n <= 0:
        return "Off"
    if n == 1:
        return "Slow"
    if n == 2:
        return "Med"
    if n >= 3:
        return "Fast"
    return "-"


def poll_rigctld_once(cfg: AppConfig, state: AppState) -> None:
    enabled = bool(getattr(cfg, "rig_control_enabled", False))
    if not enabled:
        state.rig_frequency = "-"
        state.rig_mode = "-"
        state.rig_status_line = "Radio: disabled"
        state.dirty_rig = True
        state.dirty_status = True
        return

    host = str(getattr(cfg, "rigctld_host", "127.0.0.1") or "127.0.0.1")
    port = int(getattr(cfg, "rigctld_port", 4532) or 4532)
    try:
        freq_raw = _rigctld_command(host, port, "f")
        mode_raw = _rigctld_command(host, port, "m")
        mode = (mode_raw.splitlines()[0].split()[0].strip() if mode_raw.strip() else "-")
        state.rig_frequency = _rig_format_freq_hz(freq_raw)
        state.rig_mode = mode or "-"
        state.rig_status_line = f"Radio: {host}:{port}"
    except Exception as e:
        state.rig_frequency = "-"
        state.rig_mode = "-"
        state.rig_status_line = f"Radio: {type(e).__name__}"
    state.dirty_rig = True
    state.dirty_status = True


def edit_rig_control_dialog(stdscr, cfg: AppConfig, state: AppState):
    fields = [
        ("rig_control_enabled", "Enable Rig Control", "bool"),
        ("rigctld_host", "rigctld Host", "str"),
        ("rigctld_port", "rigctld Port", "int"),
        ("rigctld_poll_ms", "Poll Interval ms", "int"),
    ]
    idx = 0
    curses.curs_set(0)
    stdscr.nodelay(False)

    while True:
        maxy, maxx = stdscr.getmaxyx()
        w = min(64, max(42, maxx - 4))
        h = len(fields) + 7
        y0 = max(1, (maxy - h) // 2)
        x0 = max(1, (maxx - w) // 2)

        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, " Rig Control ", curses.A_BOLD)
        safe_addstr(win, 1, 2, "Enter=edit  S=save  Esc=cancel"[: w - 4])

        for row, (key, label, kind) in enumerate(fields):
            style = curses.A_REVERSE if row == idx else curses.A_NORMAL
            value = getattr(cfg, key, "")
            if kind == "bool":
                shown = "Yes" if bool(value) else "No"
            else:
                shown = str(value)
            line = f"{label}: {shown}"
            safe_addstr(win, row + 3, 2, line[: w - 4], style)

        win.refresh()
        k = win.getch()

        if k in (27,):
            try:
                win.erase()
                win.noutrefresh()
            except Exception:
                pass
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k in (ord("s"), ord("S")):
            save_config(cfg)
            poll_rigctld_once(cfg, state)
            try:
                win.erase()
                win.noutrefresh()
            except Exception:
                pass
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            key, label, kind = fields[idx]
            if kind == "bool":
                setattr(cfg, key, not bool(getattr(cfg, key, False)))
            else:
                initial = str(getattr(cfg, key, ""))
                answer = _input_box(stdscr, y0 + h + 1 if (y0 + h + 4) < maxy else max(1, y0 - 4), x0, min(w, maxx - x0 - 1), f"{label}: ", initial=initial)
                if answer is not None and str(answer).strip() != "":
                    try:
                        if kind == "int":
                            setattr(cfg, key, int(str(answer).strip()))
                        else:
                            setattr(cfg, key, str(answer).strip())
                    except Exception:
                        pass
def file_menu_dialog(stdscr, cfg: AppConfig, state: AppState):
    items = ["Settings", "Rig Control", "DX Cluster", "Local Database", "Online lookup", "Look up call sign", "Quit"]
    idx = 0
    curses.curs_set(0)
    stdscr.nodelay(False)

    while True:
        maxy, maxx = stdscr.getmaxyx()
        w = min(34, max(24, maxx - 4))
        h = len(items) + 4
        y0 = 1
        x0 = 1

        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, " File ", curses.A_BOLD)

        for row, item in enumerate(items):
            style = curses.A_REVERSE if row == idx else curses.A_NORMAL
            safe_addstr(win, row + 2, 2, item[: w - 4], style)

        win.refresh()
        k = win.getch()
        if k in (27,):
            try:
                win.erase()
                win.noutrefresh()
            except Exception:
                pass
            state.dirty_space = True
            state.dirty_dx = True
            state.dirty_wx_static = True
            state.dirty_wx_time = True
            state.dirty_rig = True
            state.dirty_dedx = True
            state.dirty_map = True
            state.dirty_status = True
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(items) - 1, idx + 1)
        elif k in (curses.KEY_ENTER, 10, 13):
            choice = items[idx]
            try:
                win.erase()
                win.noutrefresh()
            except Exception:
                pass
            if choice == "Settings":
                edit_settings_dialog(stdscr, cfg, state)
            elif choice == "Rig Control":
                edit_rig_control_dialog(stdscr, cfg, state)
            elif choice == "DX Cluster":
                edit_dx_cluster_dialog(stdscr, cfg, state)
            elif choice == "Local Database":
                edit_local_database_dialog(stdscr, cfg, state)
            elif choice == "Online lookup":
                edit_online_lookup_dialog(stdscr, cfg, state)
            elif choice == "Look up call sign":
                callsign_lookup_dialog(stdscr, cfg, state)
            elif choice == "Quit":
                state.running = False
                curses.curs_set(0)
                stdscr.timeout(100)
                return


def edit_dx_cluster_dialog(stdscr, cfg: AppConfig, state: AppState):
    fields = [
        ("DX Cluster Host", "dx_cluster_host"),
        ("Port", "dx_cluster_port"),
        ("Username", "dx_cluster_username"),
        ("Password", "dx_cluster_password"),
        ("DX Filter", "dx_filter"),
    ]
    values = {attr: str(getattr(cfg, attr, "") or "") for _, attr in fields}
    idx = 0
    curses.curs_set(1)
    stdscr.nodelay(False)

    while True:
        maxy, maxx = stdscr.getmaxyx()
        w = min(76, max(44, maxx - 4))
        h = 12
        y0 = max(1, (maxy - h) // 2)
        x0 = max(0, (maxx - w) // 2)

        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, " DX Cluster ", curses.A_BOLD)

        for row, (label, attr) in enumerate(fields):
            style = curses.A_REVERSE if row == idx else curses.A_NORMAL
            val = values.get(attr, "")
            disp = "*" * len(val) if attr == "dx_cluster_password" and val else val
            safe_addstr(win, row + 2, 2, f"{label}:"[: w - 4], style)
            safe_addstr(win, row + 2, 28, disp[: max(1, w - 30)], style)

        safe_addstr(win, h - 2, 2, "Enter edits. F2 saves and applies. ESC cancels.", curses.A_DIM)
        win.refresh()
        k = win.getch()

        if k in (27,):
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k == curses.KEY_F2:
            try:
                cfg.dx_cluster_host = values["dx_cluster_host"].strip()
                cfg.dx_cluster_port = int(values["dx_cluster_port"] or "7300")
                cfg.dx_cluster_username = values["dx_cluster_username"].strip()
                cfg.dx_cluster_password = values["dx_cluster_password"]
                cfg.dx_filter = values["dx_filter"].strip()
                cfg.dx_host = cfg.dx_cluster_host
                cfg.dx_port = cfg.dx_cluster_port
                cfg.dx_user = cfg.dx_cluster_username
                cfg.dx_pass = cfg.dx_cluster_password
                save_config(cfg)
                apply_config_runtime(cfg, state)
                state.status_line = "DX Cluster settings saved and applied"
            except Exception as e:
                state.status_line = f"DX Cluster save failed: {e}"
            state.dirty_status = True
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            s = _input_box(stdscr, y0 + h - 3, x0, w, f"{label}: ", values.get(attr, ""))
            if s is not None:
                values[attr] = s


def edit_local_database_dialog(stdscr, cfg: AppConfig, state: AppState):
    idx = 0
    values = {
        "database_type": str(getattr(cfg, "database_type", "mysql" if getattr(cfg, "mysql_enabled", False) else "sqlite") or "sqlite"),
        "sqlite_file_name": str(getattr(cfg, "sqlite_file_name", CALLSIGN_DB_NAME) or CALLSIGN_DB_NAME),
        "mysql_host": str(getattr(cfg, "mysql_host", "") or ""),
        "mysql_port": str(getattr(cfg, "mysql_port", 3306) or 3306),
        "mysql_username": str(getattr(cfg, "mysql_username", "") or ""),
        "mysql_password": str(getattr(cfg, "mysql_password", "") or ""),
        "mysql_database": str(getattr(cfg, "mysql_database", "") or ""),
    }
    curses.curs_set(1)
    stdscr.nodelay(False)

    while True:
        db_type = "mysql" if str(values.get("database_type", "sqlite")).lower() == "mysql" else "sqlite"
        fields = [("Type", "database_type")]
        if db_type == "sqlite":
            fields.append(("File Name", "sqlite_file_name"))
        else:
            fields.extend([
                ("Host", "mysql_host"),
                ("Port", "mysql_port"),
                ("Username", "mysql_username"),
                ("Password", "mysql_password"),
                ("Database", "mysql_database"),
            ])
        maxy, maxx = stdscr.getmaxyx()
        w = min(80, max(48, maxx - 4))
        h = len(fields) + 7
        y0 = max(1, (maxy - h) // 2)
        x0 = max(0, (maxx - w) // 2)

        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        safe_addstr(win, 0, 2, " Local Database ", curses.A_BOLD)

        for row, (label, attr) in enumerate(fields):
            style = curses.A_REVERSE if row == idx else curses.A_NORMAL
            val = values.get(attr, "")
            if attr == "mysql_password" and val:
                val = "*" * len(val)
            safe_addstr(win, row + 2, 2, f"{label}:"[: w - 4], style)
            safe_addstr(win, row + 2, 24, str(val)[: max(1, w - 26)], style)

        safe_addstr(win, h - 3, 2, "Left/Right or Enter on Type toggles sqlite/MySQL.", curses.A_DIM)
        safe_addstr(win, h - 2, 2, "F2 saves/applies. F5 tests selected database connection.", curses.A_DIM)
        win.refresh()
        k = win.getch()

        if k in (27,):
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k in (curses.KEY_LEFT, curses.KEY_RIGHT) and fields[idx][1] == "database_type":
            values["database_type"] = "mysql" if db_type == "sqlite" else "sqlite"
            idx = 0
        elif k == curses.KEY_F5:
            test_cfg = cfg
            try:
                test_cfg.database_type = values["database_type"]
                test_cfg.sqlite_file_name = values.get("sqlite_file_name", CALLSIGN_DB_NAME)
                test_cfg.mysql_host = values.get("mysql_host", "")
                test_cfg.mysql_port = int(values.get("mysql_port", "3306") or "3306")
                test_cfg.mysql_username = values.get("mysql_username", "")
                test_cfg.mysql_password = values.get("mysql_password", "")
                test_cfg.mysql_database = values.get("mysql_database", "")
                test_cfg.mysql_enabled = (str(test_cfg.database_type).lower() == "mysql")
            except Exception:
                pass
            ok, msg = _test_database_connection_cfg(test_cfg)
            _simple_message_popup(stdscr, "Database Connection Test", ("SUCCESS: " if ok else "FAILED: ") + str(msg))
        elif k == curses.KEY_F2:
            try:
                cfg.database_type = "mysql" if db_type == "mysql" else "sqlite"
                cfg.sqlite_file_name = values.get("sqlite_file_name", CALLSIGN_DB_NAME).strip() or CALLSIGN_DB_NAME
                cfg.mysql_host = values.get("mysql_host", "").strip()
                cfg.mysql_port = int(values.get("mysql_port", "3306") or "3306")
                cfg.mysql_username = values.get("mysql_username", "").strip()
                cfg.mysql_password = values.get("mysql_password", "")
                cfg.mysql_database = values.get("mysql_database", "").strip()
                cfg.mysql_enabled = (cfg.database_type == "mysql")
                save_config(cfg)
                apply_config_runtime(cfg, state)
                state.status_line = "Local database settings saved and applied"
            except Exception as e:
                state.status_line = f"Local database save failed: {e}"
            state.dirty_status = True
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            if attr == "database_type":
                values["database_type"] = "mysql" if db_type == "sqlite" else "sqlite"
                idx = 0
                continue
            s = _input_box(stdscr, y0 + h - 3, x0, w, f"{label}: ", values.get(attr, ""))
            if s is not None:
                values[attr] = s


def edit_settings_dialog(stdscr, cfg: AppConfig, state: AppState):
    fields = [
        ("World Map Refresh (sec)", "world_map_refresh_sec"),
        ("Space Weather Refresh (sec)", "space_weather_refresh_sec"),
        ("Latitude (Decimal)", "latitude_decimal"),
        ("Longitude (Decimal)", "longitude_decimal"),
        ("Grid Square", "grid_square"),
        ("Local Weather Refresh (sec)", "local_weather_refresh_sec"),
        ("Time Zone", "time_zone"),
    ]

    normalize_config(cfg)
    if not getattr(cfg, "time_zone", ""):
        cfg.time_zone = "America/Edmonton"

    values = {}
    for _, attr in fields:
        v = getattr(cfg, attr, "")
        values[attr] = "" if v is None else str(v)

    idx = 0
    curses.curs_set(1)
    stdscr.nodelay(False)

    while True:
        maxy, maxx = stdscr.getmaxyx()
        h = min(maxy - 4, len(fields) + 6)
        w = min(maxx - 4, max(60, min(100, maxx - 4)))
        y0 = (maxy - h) // 2
        x0 = (maxx - w) // 2
        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        title = " Settings (Enter=edit, F2=Save, ESC=Cancel) "
        safe_addstr(win, 0, max(2, (w - len(title)) // 2), title, curses.A_BOLD)

        for row, (label, attr) in enumerate(fields):
            y = 2 + row
            is_sel = row == idx
            style = curses.A_REVERSE if is_sel else curses.A_NORMAL
            val = values.get(attr, "")
            safe_addstr(win, y, 2, f"{label}:"[: w - 4], style)
            safe_addstr(win, y, 32, val[: max(1, w - 34)], style)

        safe_addstr(win, h - 2, 2, "Up/Down select. Enter edits. F2 saves and applies to the running app.", curses.A_DIM)
        win.refresh()

        k = win.getch()
        if k in (27,):
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k == curses.KEY_F2:
            try:
                cfg.world_map_refresh_sec = float(values.get("world_map_refresh_sec", "300") or "300")
                cfg.space_weather_refresh_sec = float(values.get("space_weather_refresh_sec", "1800") or "1800")
                cfg.latitude_decimal = None if str(values.get("latitude_decimal", "")).strip() == "" else float(values.get("latitude_decimal"))
                cfg.longitude_decimal = None if str(values.get("longitude_decimal", "")).strip() == "" else float(values.get("longitude_decimal"))
                cfg.grid_square = values.get("grid_square", "").strip()
                cfg.local_weather_refresh_sec = float(values.get("local_weather_refresh_sec", "1800") or "1800")
                cfg.time_zone = values.get("time_zone", "").strip() or "America/Edmonton"
                save_config(cfg)
                apply_config_runtime(cfg, state)
                state.status_line = "Settings saved and applied"
            except Exception as e:
                state.status_line = f"Settings save failed: {e}"
            state.dirty_status = True
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            s = _input_box(stdscr, y0 + h - 3, x0, w, f"{label}: ", values.get(attr, ""))
            if s is not None:
                values[attr] = s

def _load_asciiworld_module():
    here = os.path.dirname(os.path.abspath(__file__))
    aw_path = os.path.join(here, "asciiworld.py")
    if not os.path.exists(aw_path):
        raise FileNotFoundError(f"asciiworld.py not found next to this script: {aw_path}")

    spec = importlib.util.spec_from_file_location("asciiworld", aw_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load asciiworld.py from: {aw_path}")

    mod = importlib.util.module_from_spec(spec)
    import sys as _sys
    _sys.modules["asciiworld"] = mod
    spec.loader.exec_module(mod)
    return mod


def asciiworld_to_lines(inner_w: int, inner_h: int, map_shp_path: Optional[str] = None, locations_path: Optional[str] = None) -> List[str]:
    """Render asciiworld for a curses panel interior.

    - Uses 256 colors output (ANSI escapes) so curses can display via addstr.
    - Enables sun shading (-s) with civil twilight (-d civil).
    - Disables trailing newline (-T) so we don't waste the last row in curses.
    """
    mod = _load_asciiworld_module()

    # Ensure positive geometry.
    w = max(1, int(inner_w))
    h = max(1, int(inner_h))

    # Default shapefile path: look next to this script.
    if not map_shp_path:
        here = os.path.dirname(os.path.abspath(__file__))
        map_shp_path = os.path.join(here, "ne_110m_land.shp")

    argv = ["-w", str(w), "-h", str(h), "-m", map_shp_path, "-c", "256", "-s", "-d", "civil", "-T"]
    if locations_path and os.path.exists(locations_path):
        argv += ["-l", locations_path]

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = mod.main(argv)
    if rc != 0:
        return [f"asciiworld failed (rc={rc})"]

    # Keep EXACT line count = inner_h (pad/crop), and never introduce a trailing newline.
    s = buf.getvalue()
    lines = s.split("\n")
    if len(lines) < h:
        lines += [""] * (h - len(lines))
    return lines[:h]

def safe_addstr(win, y, x, s, attr=0):
    try:
        win.addstr(y, x, s, attr)
    except curses.error:
        pass

# ---- ANSI (SGR) to curses helpers ----
_ANSI_SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")

def _nearest_basic_color_from_rgb(r, g, b):
    # Map to 8 basic colors (0-7) using squared distance.
    basic = [
        (0,   0,   0),   # black
        (205, 0,   0),   # red
        (0,   205, 0),   # green
        (205, 205, 0),   # yellow
        (0,   0,   238), # blue
        (205, 0,   205), # magenta
        (0,   205, 205), # cyan
        (229, 229, 229), # white
    ]
    best_i = 7
    best_d = 10**18
    for i, (br, bg, bb) in enumerate(basic):
        d = (r-br)*(r-br) + (g-bg)*(g-bg) + (b-bb)*(b-bb)
        if d < best_d:
            best_d = d
            best_i = i
    return best_i

def _xterm256_to_rgb(n: int):
    n = int(n)
    if n < 16:
        # Standard ANSI colors (approx, used only for fallback mapping)
        table = [
            (0,0,0),(205,0,0),(0,205,0),(205,205,0),
            (0,0,238),(205,0,205),(0,205,205),(229,229,229),
            (127,127,127),(255,0,0),(0,255,0),(255,255,0),
            (92,92,255),(255,0,255),(0,255,255),(255,255,255)
        ]
        return table[n]
    if 16 <= n <= 231:
        n -= 16
        r = n // 36
        g = (n % 36) // 6
        b = n % 6
        steps = [0, 95, 135, 175, 215, 255]
        return (steps[r], steps[g], steps[b])
    # 232-255 grayscale
    gray = 8 + (n - 232) * 10
    return (gray, gray, gray)

def _color_index_to_curses(n: int) -> int:
    """Return a curses color index for an xterm 256-color index.

    If the terminal/ncurses supports >=256 colors, use n directly.
    Otherwise approximate to the 8 basic colors.
    """
    try:
        if getattr(curses, "COLORS", 0) >= 256:
            return int(n)
    except Exception:
        pass
    r, g, b = _xterm256_to_rgb(int(n))
    return _nearest_basic_color_from_rgb(r, g, b)

# Cache for (fg, bg) -> pair_number
_COLOR_PAIR_CACHE = {}
_NEXT_PAIR = 1  # 0 is default

def _get_color_attr(fg_idx: Optional[int], bg_idx: Optional[int], bold: bool = False) -> int:
    global _NEXT_PAIR
    attr = curses.A_BOLD if bold else 0

    if not curses.has_colors():
        return attr

    # -1 means default in ncurses; keep if use_default_colors() succeeded.
    fg = -1 if fg_idx is None else _color_index_to_curses(fg_idx)
    bg = -1 if bg_idx is None else _color_index_to_curses(bg_idx)

    key = (fg, bg)
    pair = _COLOR_PAIR_CACHE.get(key)
    if pair is None:
        # Allocate a new pair if possible; otherwise fall back to default.
        try:
            if _NEXT_PAIR < curses.COLOR_PAIRS:
                pair = _NEXT_PAIR
                curses.init_pair(pair, fg, bg)
                _COLOR_PAIR_CACHE[key] = pair
                _NEXT_PAIR += 1
            else:
                pair = 0
        except Exception:
            pair = 0
    return attr | curses.color_pair(pair)

def ansi_to_curses_runs(s: str):
    """Parse a string with ANSI SGR sequences into (text, curses_attr) runs."""
    runs = []
    i = 0
    fg = None
    bg = None
    bold = False
    cur_attr = _get_color_attr(fg, bg, bold)

    while i < len(s):
        esc = s.find("\x1b", i)
        if esc == -1:
            if i < len(s):
                runs.append((s[i:], cur_attr))
            break

        # Add preceding text
        if esc > i:
            runs.append((s[i:esc], cur_attr))

        # Try parse CSI ... m; if incomplete/unknown, drop the ESC to avoid printing ^[
        if esc + 1 >= len(s) or s[esc+1] != '[':
            i = esc + 1
            continue

        m = _ANSI_SGR_RE.match(s, esc)
        if not m:
            # Incomplete sequence like ESC[ at end of line; stop processing rest
            i = esc + 1
            continue

        params = m.group(1)
        codes = [0] if params == "" else [int(p) if p.isdigit() else 0 for p in params.split(';')]

        # Interpret common SGR codes and xterm 256-color extensions
        j = 0
        while j < len(codes):
            c = codes[j]
            if c == 0:
                fg = None
                bg = None
                bold = False
            elif c == 1:
                bold = True
            elif c == 22:
                bold = False
            elif c == 39:
                fg = None
            elif c == 49:
                bg = None
            elif 30 <= c <= 37:
                fg = c - 30
            elif 40 <= c <= 47:
                bg = c - 40
            elif c == 38 and j + 2 < len(codes) and codes[j+1] == 5:
                fg = codes[j+2]
                j += 2
            elif c == 48 and j + 2 < len(codes) and codes[j+1] == 5:
                bg = codes[j+2]
                j += 2
            j += 1

        cur_attr = _get_color_attr(fg, bg, bold)
        i = m.end()

    return runs

def add_ansi_str(win, y: int, x: int, s: str, max_width: int):
    """Write ANSI-colored string to a curses window, truncating by visible width."""
    if max_width <= 0:
        return

    col = 0
    for text, attr in ansi_to_curses_runs(s):
        if not text:
            continue
        # Truncate this run if needed
        if col >= max_width:
            break
        avail = max_width - col
        chunk = text[:avail]
        safe_addstr(win, y, x + col, chunk, attr)
        col += len(chunk)




def box_title(win, title: str):
    win.box()
    safe_addstr(win, 0, 2, f" {title} ")


def clear_interior(win):
    h, w = win.getmaxyx()
    blank = " " * max(1, w - 2)
    for y in range(1, h - 1):
        safe_addstr(win, y, 1, blank)


def _fetch_url(url: str, timeout: float = 10.0) -> str:
    """Fetch a URL with sensible TLS handling.

    Primary attempt uses the system trust store (normal certificate verification).
    If verification fails, we can optionally fall back to an unverified context to
    keep the clock running.

    Control:
      - Set environment variable MYHAMCLOCK_INSECURE_SSL=1 to always skip TLS verification.
      - Otherwise, we only fall back to unverified on common TLS verification failures.

    Note: Skipping verification is less secure; prefer fixing Windows/Python CA certs.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "MyHamClock/1.0"})

    def _read(context=None) -> str:
        if context is None:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
        with urllib.request.urlopen(req, timeout=timeout, context=context) as r:
            return r.read().decode("utf-8", errors="replace")

    insecure = os.environ.get("MYHAMCLOCK_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes")
    if insecure and url.lower().startswith("https://"):
        return _read(context=ssl._create_unverified_context())

    try:
        return _read()
    except Exception as e:
        # urllib wraps SSL errors in URLError, but sometimes raw SSLError bubbles up.
        reason = getattr(e, "reason", None)

        tls_error = isinstance(e, ssl.SSLError) or isinstance(reason, ssl.SSLError)
        if not tls_error:
            raise

        # Only fall back for verification / chain / hostname issues (common on Windows when CA bundle is missing/outdated).
        msg = str(reason or e).lower()
        fallback_markers = (
            "certificate verify failed",
            "certIFICATE_VERIFY_FAILED".lower(),
            "self signed",
            "unknown ca",
            "unable to get local issuer",
            "hostname",
        )
        if any(s in msg for s in fallback_markers) and url.lower().startswith("https://"):
            ctx = ssl._create_unverified_context()
            return _read(context=ctx)

        raise




def update_space_weather(state: AppState, panel_inner_w: int = 0):
    """Populate state.space_weather_lines with live solar data and HamQSL HF band conditions."""
    try:
        xml = _fetch_url("https://www.hamqsl.com/solarxml.php", timeout=10.0)

        def get(tag: str) -> str:
            m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.IGNORECASE | re.DOTALL)
            return m.group(1).strip() if m else ""

        def _norm_cond(val: str) -> str:
            v = str(val or "").strip()
            if not v:
                return "—"
            lookup = {
                "good": "Good",
                "fair": "Fair",
                "poor": "Poor",
                "open": "Open",
                "closed": "Closed",
            }
            return lookup.get(v.lower(), v)

        def _parse_hamqsl_band_conditions(xml_text: str):
            """
            Parse HamQSL solarxml calculatedconditions/band entries.

            Expected structure includes entries like:
              <calculatedconditions>
                <band name="30m-20m" time="day">Good</band>
                <band name="30m-20m" time="night">Good</band>
              </calculatedconditions>
            """
            out = {}
            m_calc = re.search(
                r"<calculatedconditions\b[^>]*>(.*?)</calculatedconditions>",
                xml_text,
                re.IGNORECASE | re.DOTALL,
            )
            scope = m_calc.group(1) if m_calc else xml_text

            for m_band in re.finditer(
                r"<band\b([^>]*)>(.*?)</band>",
                scope,
                re.IGNORECASE | re.DOTALL,
            ):
                attrs_text = m_band.group(1) or ""
                value = _norm_cond(re.sub(r"<.*?>", "", m_band.group(2) or "").strip())
                attrs = dict(
                    (k.lower(), v)
                    for k, v in re.findall(r'(\w+)\s*=\s*"([^"]*)"', attrs_text)
                )
                band_name = str(attrs.get("name", "")).strip().lower()
                time_name = str(attrs.get("time", "")).strip().lower()
                if not band_name or time_name not in ("day", "night"):
                    continue

                band_name = band_name.replace(" ", "")
                band_name = band_name.replace("meters", "m").replace("meter", "m")
                band_name = band_name.replace("mhz", "m")
                band_name = band_name.replace("m-", "-").replace("-m", "-")
                band_name = band_name.replace("m", "")
                band_name = band_name.strip("-")
                if band_name not in ("80-40", "30-20", "17-15", "12-10"):
                    continue

                out.setdefault(band_name, {})
                out[band_name][time_name] = value

            return out

        sfi = get("solarflux")
        ssn = get("sunspots")
        aindex = get("aindex")
        kindex = get("kindex")
        xray = get("xray")
        aurora = get("aurora")
        muf = get("muf")
        solarwind = get("solarwind")
        bz = get("bz")
        updated = get("updated")

        left = [
            f"SFI: {sfi or '—'}   SSN: {ssn or '—'}",
            f"Kp:  {kindex or '—'}   A:   {aindex or '—'}",
            f"X-ray: {xray or '—'}  Aurora: {aurora or '—'}",
            f"Wind: {solarwind or '—'}  Bz: {bz or '—'}",
            f"MUF:  {muf or '—'}",
        ]
        if updated:
            left.append(f"Upd: {updated}")

        def _band_condition(sfi_val: str, kp_val: str, mhz: float) -> str:
            try:
                s = float(sfi_val) if sfi_val else 0.0
            except Exception:
                s = 0.0
            try:
                k = float(kp_val) if kp_val else 0.0
            except Exception:
                k = 0.0

            if mhz >= 21:
                if s >= 140 and k <= 4:
                    return "Good"
                if s >= 110 and k <= 5:
                    return "Fair"
                return "Poor"
            if mhz >= 14:
                if s >= 120 and k <= 5:
                    return "Good"
                if s >= 100 and k <= 6:
                    return "Fair"
                return "Poor"
            if mhz >= 7:
                if k >= 7:
                    return "Poor"
                return "Good" if s >= 70 else "Fair"
            return "Poor" if k >= 7 else "Good"

        def _worsen_one_step(cond: str) -> str:
            order = ["Good", "Fair", "Poor", "Closed"]
            try:
                i = order.index(cond)
            except ValueError:
                return cond or "—"
            return order[min(i + 1, len(order) - 1)]

        def _fallback_band_group_lines(sfi_val: str, kp_val: str):
            groups = [
                ("80-40", (3.5, 7.0)),
                ("30-20", (10.0, 14.0)),
                ("17-15", (18.0, 21.0)),
                ("12-10", (24.0, 28.0)),
            ]
            out = ["     Day  Night"]
            order = ["Good", "Fair", "Poor", "Closed"]
            for label, (f1, f2) in groups:
                c1 = _band_condition(sfi_val, kp_val, f1)
                c2 = _band_condition(sfi_val, kp_val, f2)
                day = c1 if order.index(c1) <= order.index(c2) else c2
                night = _worsen_one_step(day) if f2 >= 14.0 else day
                out.append(f"{label:>5} {day:<5} {night:<5}")
            return out

        parsed_bands = _parse_hamqsl_band_conditions(xml)
        if parsed_bands:
            right = ["     Day  Night"]
            for label in ("80-40", "30-20", "17-15", "12-10"):
                band = parsed_bands.get(label, {})
                day = _norm_cond(band.get("day", "—"))
                night = _norm_cond(band.get("night", "—"))
                right.append(f"{label:>5} {day:<5} {night:<5}")
        else:
            right = _fallback_band_group_lines(sfi, kindex)

        w = int(panel_inner_w or 0)
        if w >= 34:
            col_gap = 2
            col_w = (w - col_gap) // 2
            rows = max(len(left), len(right))
            lines_out = []
            for i in range(rows):
                l = left[i] if i < len(left) else ""
                r = right[i] if i < len(right) else ""
                lines_out.append(f"{l:<{col_w}.{col_w}}{' ' * col_gap}{r:>{col_w}.{col_w}}")
            state.space_weather_lines = lines_out
        else:
            state.space_weather_lines = left + [""] + ["Bands:"] + right

        state.dirty_space = True
    except Exception as e:
        state.space_weather_lines = [
            "Space wx: (error)",
            f"Reason: {e}",
            "Tip: install/refresh ca-certificates",
        ]
        state.dirty_space = True

def maidenhead_to_latlon(grid: str):
    """Convert Maidenhead grid (4, 6, or 8 chars) to (lat, lon) center point."""
    g = re.sub(r"\s+", "", grid or "").upper()
    if len(g) < 4:
        raise ValueError("Grid square must be at least 4 characters")
    if not (g[0].isalpha() and g[1].isalpha() and g[2].isdigit() and g[3].isdigit()):
        raise ValueError("Invalid Maidenhead grid format")

    lon = -180.0 + (ord(g[0]) - ord('A')) * 20.0
    lat =  -90.0 + (ord(g[1]) - ord('A')) * 10.0
    lon += int(g[2]) * 2.0
    lat += int(g[3]) * 1.0

    lon_size = 2.0
    lat_size = 1.0

    if len(g) >= 6:
        if not (g[4].isalpha() and g[5].isalpha()):
            raise ValueError("Invalid Maidenhead grid format")
        lon += (ord(g[4]) - ord('A')) * (5.0 / 60.0)
        lat += (ord(g[5]) - ord('A')) * (2.5 / 60.0)
        lon_size = 5.0 / 60.0
        lat_size = 2.5 / 60.0

    if len(g) >= 8:
        if not (g[6].isdigit() and g[7].isdigit()):
            raise ValueError("Invalid Maidenhead grid format")
        lon += int(g[6]) * (0.5 / 60.0)
        lat += int(g[7]) * (0.25 / 60.0)
        lon_size = 0.5 / 60.0
        lat_size = 0.25 / 60.0

    # return center of the square
    return (lat + lat_size / 2.0, lon + lon_size / 2.0)


def resolve_wx_latlon(cfg: AppConfig):
    """Return (lat, lon) from the standardized settings: grid_square first, then latitude/longitude."""
    grid = str(getattr(cfg, "grid_square", "") or "").strip()
    if grid:
        try:
            return maidenhead_to_latlon(grid)
        except Exception:
            pass
    lat = getattr(cfg, "latitude_decimal", None)
    lon = getattr(cfg, "longitude_decimal", None)
    if lat is not None and lon is not None:
        return float(lat), float(lon)
    return None, None

def fetch_open_meteo_current(lat: float, lon: float, timeout: float = 10.0):
    """Fetch current conditions from Open-Meteo for a given lat/lon."""
    params = {
        "latitude": f"{lat:.5f}",
        "longitude": f"{lon:.5f}",
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "weather_code",
        ]),
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "timezone": "UTC",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "MyHamClock/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    return data


def wmo_weather_code_to_text(code: Optional[int]) -> str:
    # Minimal mapping; expand as needed.
    mapping = {
        0: "Clear",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Rime fog",
        51: "Light drizzle",
        53: "Drizzle",
        55: "Heavy drizzle",
        61: "Light rain",
        63: "Rain",
        65: "Heavy rain",
        71: "Light snow",
        73: "Snow",
        75: "Heavy snow",
        80: "Rain showers",
        81: "Rain showers",
        82: "Violent showers",
        95: "Thunderstorm",
        96: "T-storm w/ hail",
        99: "T-storm w/ hail",
    }
    return mapping.get(code, f"Code {code}" if code is not None else "N/A")


def wind_deg_to_compass(deg: Optional[float]) -> str:
    """Convert meteorological wind direction in degrees to nearest 16-point compass direction.

    Open-Meteo returns wind_direction_10m as degrees (0..360) where 0/360 = North, 90 = East.
    """
    if deg is None:
        return "N/A"
    try:
        d = float(deg) % 360.0
    except Exception:
        return "N/A"

    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((d + 11.25) // 22.5) % 16
    return dirs[idx]




def update_weather_open_meteo(cfg: AppConfig, state: AppState):
    lat, lon = resolve_wx_latlon(cfg)
    if lat is None or lon is None:
        state.wx_static_lines = [
            (cfg.wx_location_name or "Weather"),
            "Temp:  N/A (set Grid Square or Latitude/Longitude)",
            "Wind:  N/A",
            "Sky:   N/A",
        ]
        state.dirty_wx_static = True
        return

    try:
        data = fetch_open_meteo_current(lat, lon)
        cur = data.get("current", {}) or {}

        temp = cur.get("temperature_2m")
        rh = cur.get("relative_humidity_2m")
        app = cur.get("apparent_temperature")
        ws = cur.get("wind_speed_10m")
        wd = cur.get("wind_direction_10m")
        gust = cur.get("wind_gusts_10m")
        wcode = cur.get("weather_code")

        sky = wmo_weather_code_to_text(wcode if isinstance(wcode, int) else None)

        loc = cfg.wx_location_name or (cfg.grid_square if getattr(cfg, "grid_square", "") else f"{lat:.3f},{lon:.3f}")
        state.wx_static_lines = [
            loc,
            f"Temp:  {temp:.1f}°C (feels {app:.1f}°C)  RH {rh:.0f}%" if temp is not None and app is not None and rh is not None else "Temp:  N/A",
            f"Wind:  {ws:.0f} km/h @ {wd:.0f}° ({wind_deg_to_compass(wd)})  Gust {gust:.0f}" if ws is not None and wd is not None and gust is not None else "Wind:  N/A",
            f"Sky:   {sky}",
        ]
    except Exception as e:
        state.wx_static_lines = [
            (cfg.wx_location_name or "Weather"),
            "Temp:  N/A",
            "Wind:  N/A",
            f"Sky:   error: {e}",
        ]

    state.dirty_wx_static = True



def update_time_lines(state: AppState):
    # 24-hour clock only, time-of-day (no date)
    local = time.strftime("%H:%M:%S")
    utc = time.strftime("%H:%M:%S", time.gmtime())
    state.wx_time_lines = [
        f"Local:  {local}",
        f"UTC:    {utc}",
    ]
    state.dirty_wx_time = True


def strip_dx_prefix(line: str) -> str:
    # Remove leading "DX de" (case-insensitive) with variable spacing.
    return re.sub(r'^\s*DX\s+de\s+', '', line, flags=re.IGNORECASE)


def extract_dx_mode(comment: str) -> str:
    """
    Extract an operating mode from the DX cluster comment field.

    Preferred explicit modes:
      SSTV, FT8, FT4, FT2, CW, LSB, USB, SSB, DATA

    If no explicit mode is present, common digital-mode keywords are mapped to DATA.
    Returns "" when no mode can be determined.
    """
    txt = (comment or "").upper()

    explicit_modes = ["SSTV", "FT8", "FT4", "FT2", "CW", "LSB", "USB", "SSB", "DATA", "RTTY", "PSK", "OLIVIA", "JS8", "JT65", "JT9", "JT", "MFSK",        "PACKET", "VARA", "WINLINK", "DIGI", "DIGITAL", "FSQ", "HELL"]
    for mode in explicit_modes:
        if re.search(rf'(?<![A-Z0-9]){re.escape(mode)}(?![A-Z0-9])', txt):
            return mode

    return ""


def format_dx_spot(line: str) -> str:
    """
    Parse common DX cluster spot lines and reformat to fixed-width columns:
        <spotter> <freq> <spotted> <mode>

    The mode is extracted from the comment field when present.
    Non-spot lines are returned in a cleaned form for display.
    """
    clean = strip_dx_prefix(line).strip()

    # Expect: '<spotter>: <rest>'
    m = re.match(r'^(\S+):\s*(.+)$', clean)
    if not m:
        return clean

    spotter = m.group(1)
    rest = m.group(2).strip()

    # Parse the structured part of a DX spot line.
    # Typical format:
    #   <freq> <spotted> <comment up to 30 chars> [grid] <HHMM>Z [grid]
    m2 = re.match(
        r'^(\d+(?:\.\d+)?)\s+([A-Z0-9/+-]+)\s*(.*?)\s*(?:([A-Ra-r]{2}\d{2}))?\s*(\d{4}Z)(?:\s+([A-Ra-r]{2}\d{2}))?\s*$',
        rest,
        re.IGNORECASE
    )
    if not m2:
        parts = rest.split()
        if len(parts) < 2:
            return clean
        freq = parts[0]
        spotted = parts[1]
        mode = extract_dx_mode(" ".join(parts[2:])) or "-"
    else:
        freq = m2.group(1)
        spotted = m2.group(2)
        comment = (m2.group(3) or "").strip()
        mode = extract_dx_mode(comment) or "-"

    # Fixed-width columns.
    spotter_col = f"{spotter:<10.10}"
    freq_col = f"{freq:>8}"
    spotted_col = f"{spotted:<10.10}"
    mode_col = f"{mode:<5.5}"

    return f"{spotter_col} {freq_col} {spotted_col} {mode_col}"


# ---- DX spot -> map point helpers ----
_GRID4_RE = re.compile(r"\b([A-Ra-r]{2}\d{2})\b")

def extract_grid4_from_spot(formatted_spot: str) -> Optional[str]:
    """Extract a 4-character Maidenhead (e.g., DO42) from the formatted DX spot line."""
    m = _GRID4_RE.search(formatted_spot)
    if not m:
        return None
    return m.group(1).upper()


def parse_raw_dx_spot(line: str):
    return None

def _extract_grid4(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    m = re.search(r'([A-Ra-r]{2}\d{2})', value.strip())
    return m.group(1).upper() if m else None

def maidenhead4_to_latlon(grid4: str) -> Tuple[float, float]:
    """Convert 4-char Maidenhead locator (AA00) to lat/lon (center of the square)."""
    g = grid4.strip().upper()
    if not re.fullmatch(r"[A-R]{2}\d{2}", g):
        raise ValueError(f"Invalid Maidenhead-4: {grid4!r}")

    A = ord('A')
    lon = (ord(g[0]) - A) * 20 - 180
    lat = (ord(g[1]) - A) * 10 - 90
    lon += int(g[2]) * 2
    lat += int(g[3]) * 1

    # center of the 2°x1° square
    lon += 1.0
    lat += 0.5
    return (lat, lon)



def _write_dx_marker(f, lat: float, lon: float, label: str, color: str = "R"):
    """
    Write one asciiworld location entry with a text marker.
    color: R=red, Y=yellow
    """
    f.write(f"{lat:.5f} {lon:.5f} {label} {color}\n")



def write_dx_points_file(state: AppState):
    """
    DX spot overlay disabled.

    Keep the world map itself intact, but remove any dx_points.txt overlay file
    so no DX spot markers are shown.
    """
    if not state.dx_points_file:
        here = os.path.dirname(os.path.abspath(__file__))
        state.dx_points_file = os.path.join(here, "dx_points.txt")
    with state.dx_points_lock:
        try:
            os.remove(state.dx_points_file)
        except FileNotFoundError:
            pass




# ---- callsign lookup / DE-DX panel helpers ----
CALLSIGN_DB_NAME = "hamcall.sqlite"
CALLSIGN_LOOKUP_MODULE_NAME = "callsign_lookup"
HAMQTH_URL = "https://www.hamqth.com/xml.php"
HAMQTH_PROGRAM = "hamclock"
HAMQTH_CACHE_FILE = "hamqth_session.json"
HAMQTH_CACHE_MAX_AGE = 3600
HAMQTH_NS = {"hq": "https://www.hamqth.com"}
QRZ_URL = "https://xmldata.qrz.com/xml/current/"
QRZ_XML_FILE = _app_file_path("qrz.xml")
QRZ_RUNTIME_SESSION_ID = ""
QRZ_RUNTIME_SESSION_SOURCE = "-"

QRZ_CACHE_FILE = _app_file_path("qrz_session.txt")
QRZ_CACHE_MAX_AGE = 86400

_callsign_lookup_module = None


def _clean_lookup_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_callsign(value) -> str:
    call = _clean_lookup_value(value).upper()
    if not call:
        return ""

    call = re.sub(r'\s+', '', call)
    parts = [p for p in call.split('/') if p]
    if not parts:
        return ""

    if len(parts) == 1:
        return parts[0]

    def _looks_like_base_callsign(part: str) -> bool:
        if not part or len(part) < 3:
            return False
        if not re.search(r'[A-Z]', part):
            return False
        if not re.search(r'\d', part):
            return False
        return bool(re.fullmatch(r'[A-Z0-9]+', part))

    preferred = [p for p in parts if _looks_like_base_callsign(p)]
    if preferred:
        preferred.sort(key=lambda p: (len(p), p.count('-'), p))
        return preferred[0]

    longest = max(parts, key=len)
    return longest


def _format_lookup_name(first, middle, last) -> str:
    parts = [_clean_lookup_value(first), _clean_lookup_value(middle), _clean_lookup_value(last)]
    return " ".join(p for p in parts if p)


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _callsign_db_path() -> str:
    cfg2 = load_config()
    normalize_config(cfg2)
    primary = _callsign_db_path_cfg(cfg2)
    if os.path.exists(primary):
        return primary
    sample = os.path.join(_script_dir(), "sample.sqlite")
    if os.path.exists(sample):
        return sample
    return primary


def _load_callsign_lookup_module():
    global _callsign_lookup_module
    if _callsign_lookup_module is not None:
        return _callsign_lookup_module

    candidate = os.path.join(_script_dir(), f"{CALLSIGN_LOOKUP_MODULE_NAME}.py")
    if not os.path.exists(candidate):
        _callsign_lookup_module = False
        return None

    try:
        spec = importlib.util.spec_from_file_location(CALLSIGN_LOOKUP_MODULE_NAME, candidate)
        if spec is None or spec.loader is None:
            _callsign_lookup_module = False
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault(CALLSIGN_LOOKUP_MODULE_NAME, module)
        spec.loader.exec_module(module)
        _callsign_lookup_module = module
        return module
    except Exception:
        _callsign_lookup_module = False
        return None


def _normalize_lookup_result(result: Optional[dict], source_name: str, callsign: str) -> Optional[dict]:
    if not result:
        return None

    country = (
        _clean_lookup_value(result.get("country"))
        or _clean_lookup_value(result.get("mailing_country"))
        or _clean_lookup_value(result.get("prefix_country"))
        or _clean_lookup_value(result.get("dxcc_name"))
    )

    return {
        "callsign": _normalize_callsign(result.get("callsign")) or _normalize_callsign(callsign),
        "name": _clean_lookup_value(result.get("name")),
        "city": _clean_lookup_value(result.get("city")),
        "prov_state": _clean_lookup_value(result.get("prov_state")),
        "country": country,
        "source": source_name,
    }



def _db_is_mysql_cfg(cfg: Optional[AppConfig] = None) -> bool:
    if cfg is None:
        try:
            cfg = load_config()
        except Exception:
            cfg = None
    return bool(cfg and getattr(cfg, "mysql_enabled", False))


def _mysql_connect_cfg(cfg: AppConfig):
    if _mysql_driver is None:
        raise RuntimeError("No MySQL/MariaDB driver installed. Install pymysql, mysql-connector-python, or mariadb.")

    host = _clean_lookup_value(getattr(cfg, "mysql_host", ""))
    port = int(getattr(cfg, "mysql_port", 3306) or 3306)
    user = _clean_lookup_value(getattr(cfg, "mysql_username", ""))
    password = getattr(cfg, "mysql_password", "") or ""
    database = _clean_lookup_value(getattr(cfg, "mysql_database", ""))

    if not host:
        raise RuntimeError("MySQL host/IP is required")
    if not user:
        raise RuntimeError("MySQL username is required")
    if not database:
        raise RuntimeError("MySQL database is required")

    if _MYSQL_DRIVER_NAME == "pymysql":
        return _mysql_driver.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=False,
            charset="utf8mb4",
            cursorclass=_mysql_driver.cursors.Cursor,
        )

    if _MYSQL_DRIVER_NAME == "mysql.connector":
        return _mysql_driver.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )

    if _MYSQL_DRIVER_NAME == "mariadb":
        return _mysql_driver.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=False,
        )

    raise RuntimeError("Unsupported MySQL/MariaDB driver")


def _test_mysql_connection_cfg(cfg: AppConfig) -> Tuple[bool, str]:
    try:
        conn = _mysql_connect_cfg(cfg)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return True, f"Connection OK via {_MYSQL_DRIVER_NAME}"
    except Exception as e:
        return False, str(e)


def _param_placeholder_for_conn(conn) -> str:
    return "%s" if conn.__class__.__module__.split(".")[0] in ("pymysql", "mysql", "mariadb") else "?"


def _table_columns_generic(conn, table_name: str) -> set:
    try:
        mod = conn.__class__.__module__.split(".")[0]
        cur = conn.cursor()
        if mod in ("pymysql", "mysql", "mariadb"):
            cur.execute(f"DESCRIBE {table_name}")
            rows = cur.fetchall()
            return {str(row[0]).lower() for row in rows}
        cur.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1]).lower() for row in cur.fetchall()}
    except Exception:
        return set()


def _calls_table_has_columns_generic(conn, wanted_cols) -> bool:
    cols = _table_columns_generic(conn, "calls")
    return all(col.lower() in cols for col in wanted_cols)


def _lookup_hamcall_calls_db(conn, callsign: str) -> Optional[dict]:
    cur = conn.cursor()
    ph = _param_placeholder_for_conn(conn)
    cur.execute(f"""
        SELECT callsign, first_name, middle, last_name, city, state_province,
               mailing_country, prefix_country, dxcc_name
        FROM hamcall_calls
        WHERE UPPER(callsign) = UPPER({ph})
        LIMIT 1
    """, (callsign,))
    row = cur.fetchone()
    if not row:
        return None
    return {
        "callsign": _normalize_callsign(row[0]),
        "name": _format_lookup_name(row[1], row[2], row[3]),
        "city": row[4],
        "prov_state": row[5],
        "country": row[6] or row[7] or row[8] or "",
    }


def _lookup_calls_db(conn, callsign: str) -> Optional[dict]:
    cur = conn.cursor()
    ph = _param_placeholder_for_conn(conn)
    cur.execute(f"""
        SELECT callsign, payload_json
        FROM calls
        WHERE UPPER(callsign) = UPPER({ph})
        LIMIT 1
    """, (callsign,))
    row = cur.fetchone()
    if not row:
        return None

    payload = {}
    try:
        payload = json.loads(row[1] or "{}")
    except Exception as e:
        _debug_log(f"_lookup_calls_db: payload_json decode failed callsign={callsign!r}: {e}")
        payload = {}

    source_payload = None
    source_name = "calls"
    for candidate in ("hamdb", "hamqth", "qrz"):
        candidate_payload = payload.get(candidate)
        if isinstance(candidate_payload, dict) and candidate_payload:
            source_payload = candidate_payload
            source_name = candidate
            break

    if source_payload is None:
        source_payload = payload if isinstance(payload, dict) else {}

    first = _clean_lookup_value(
        source_payload.get("fname")
        or source_payload.get("first_name")
        or source_payload.get("first")
    )
    middle = _clean_lookup_value(
        source_payload.get("mi")
        or source_payload.get("middle")
        or source_payload.get("middle_name")
    )
    last = _clean_lookup_value(
        source_payload.get("name")
        or source_payload.get("last_name")
        or source_payload.get("surname")
    )

    return {
        "callsign": row[0] or source_payload.get("call") or source_payload.get("callsign") or callsign.upper(),
        "name": _format_lookup_name(first, middle, last) or _clean_lookup_value(source_payload.get("adr_name")),
        "city": (
            source_payload.get("addr2")
            or source_payload.get("adr_city")
            or source_payload.get("city")
            or ""
        ),
        "prov_state": (
            source_payload.get("state")
            or source_payload.get("prov_state")
            or source_payload.get("district")
            or source_payload.get("us_state")
            or source_payload.get("province")
            or ""
        ),
        "country": (
            source_payload.get("country")
            or source_payload.get("mailing_country")
            or source_payload.get("prefix_country")
            or source_payload.get("dxcc_name")
            or source_payload.get("adr_country")
            or ""
        ),
        "source": source_name,
    }


def _mysql_insert_or_update_calls(conn, callsign: str, grid: str, name: str, payload_json: str) -> None:
    cur = conn.cursor()
    ph = _param_placeholder_for_conn(conn)
    cols = _table_columns_generic(conn, "calls")

    cur.execute(f"SELECT payload_json FROM calls WHERE UPPER(callsign)=UPPER({ph}) LIMIT 1", (callsign,))
    row = cur.fetchone()
    if row:
        cur.execute(f"UPDATE calls SET payload_json={ph} WHERE UPPER(callsign)=UPPER({ph})", (payload_json, callsign))
        conn.commit()
        return

    if all(c in cols for c in ("callsign", "grid", "name", "payload_json")):
        cur.execute(
            f"INSERT INTO calls (callsign, grid, name, payload_json) VALUES ({ph}, {ph}, {ph}, {ph})",
            (callsign, grid, name, payload_json),
        )
        conn.commit()
        return

    if all(c in cols for c in ("callsign", "payload_json")):
        cur.execute(
            f"INSERT INTO calls (callsign, payload_json) VALUES ({ph}, {ph})",
            (callsign, payload_json),
        )
        conn.commit()


def _mysql_upsert_hamcall_calls_from_qrz(conn, info: dict) -> None:
    if not info:
        return

    cols = _table_columns_generic(conn, "hamcall_calls")
    if not cols:
        return

    callsign = _normalize_callsign(info.get("callsign"))
    if not callsign:
        return

    row = {
        "callsign": callsign,
        "class": _clean_lookup_value(info.get("class")),
        "bmcode": _clean_lookup_value(info.get("bmcode")),
        "first_name": _clean_lookup_value(info.get("first_name")),
        "middle": _clean_lookup_value(info.get("middle")),
        "last_name": _clean_lookup_value(info.get("last_name")),
        "suffix": _clean_lookup_value(info.get("suffix")),
        "street": _clean_lookup_value(info.get("street")),
        "prefix_country": _clean_lookup_value(info.get("prefix_country")),
        "po_box": _clean_lookup_value(info.get("po_box")),
        "city": _clean_lookup_value(info.get("city")),
        "state_province": _clean_lookup_value(info.get("state_province") or info.get("prov_state")),
        "postal_code": _clean_lookup_value(info.get("postal_code")),
        "birthdate": _clean_lookup_value(info.get("birthdate")),
        "date_first_issue": _clean_lookup_value(info.get("date_first_issue")),
        "expiration_date": _clean_lookup_value(info.get("expiration_date")),
        "process_date": _clean_lookup_value(info.get("process_date")),
        "county": _clean_lookup_value(info.get("county")),
        "gmt_offset": _clean_lookup_value(info.get("gmt_offset")),
        "latitude": _clean_lookup_value(info.get("latitude")),
        "longitude": _clean_lookup_value(info.get("longitude")),
        "grid": _clean_lookup_value(info.get("grid")),
        "area_code": _clean_lookup_value(info.get("area_code")),
        "previous_call": _clean_lookup_value(info.get("previous_call")),
        "previous_class": _clean_lookup_value(info.get("previous_class")),
        "transaction_type": _clean_lookup_value(info.get("transaction_type")),
        "email": _clean_lookup_value(info.get("email")),
        "qsl_manager": _clean_lookup_value(info.get("qsl_manager")),
        "mailing_country": _clean_lookup_value(info.get("mailing_country") or info.get("country")),
        "url": _clean_lookup_value(info.get("url")),
        "vanity_flag": _clean_lookup_value(info.get("vanity_flag")),
        "fax": _clean_lookup_value(info.get("fax")),
        "interest_profile": _clean_lookup_value(info.get("interest_profile")),
        "phone": _clean_lookup_value(info.get("phone")),
        "fcc_licensee_id": _clean_lookup_value(info.get("fcc_licensee_id")),
        "ten_ten_number": _clean_lookup_value(info.get("ten_ten_number")),
        "fcc_frn": _clean_lookup_value(info.get("fcc_frn")),
        "iota": _clean_lookup_value(info.get("iota")),
        "fists": _clean_lookup_value(info.get("fists")),
        "qcwa": _clean_lookup_value(info.get("qcwa")),
        "ootc": _clean_lookup_value(info.get("ootc")),
        "fcc_license_key": _clean_lookup_value(info.get("fcc_license_key")),
        "naqcc": _clean_lookup_value(info.get("naqcc")),
        "skcc": _clean_lookup_value(info.get("skcc")),
        "dxcc_number": _clean_lookup_value(info.get("dxcc_number")),
        "dxcc_name": _clean_lookup_value(info.get("dxcc_name") or info.get("country")),
        "last_lookup_utc": _clean_lookup_value(info.get("last_lookup_utc") or _utc_now_iso()),
        "raw": _clean_lookup_value(info.get("raw")),
        "data_json": _clean_lookup_value(info.get("data_json") or json.dumps(info, ensure_ascii=False, sort_keys=True)),
    }

    insertable = {k: row.get(k, "") for k in row if k.lower() in cols}
    if "callsign" not in insertable:
        return

    ph = _param_placeholder_for_conn(conn)
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM hamcall_calls WHERE UPPER(callsign)=UPPER({ph}) LIMIT 1", (callsign,))
    exists = cur.fetchone() is not None

    ordered_cols = list(insertable.keys())
    if exists:
        set_cols = [c for c in ordered_cols if c.lower() != "callsign"]
        if set_cols:
            sql = f"UPDATE hamcall_calls SET " + ", ".join(f"{c}={ph}" for c in set_cols) + f" WHERE UPPER(callsign)=UPPER({ph})"
            params = [insertable[c] for c in set_cols] + [callsign]
            cur.execute(sql, params)
            conn.commit()
    else:
        sql = f"INSERT INTO hamcall_calls ({', '.join(ordered_cols)}) VALUES (" + ", ".join([ph] * len(ordered_cols)) + ")"
        cur.execute(sql, [insertable[c] for c in ordered_cols])
        conn.commit()


def _lookup_hamcall_calls_local(conn: sqlite3.Connection, callsign: str) -> Optional[dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT callsign, first_name, middle, last_name, city, state_province,
               mailing_country, prefix_country, dxcc_name
        FROM hamcall_calls
        WHERE UPPER(callsign) = UPPER(?)
        LIMIT 1
    """, (callsign,))
    row = cur.fetchone()
    if not row:
        return None

    return {
        "callsign": _normalize_callsign(row[0]),
        "name": _format_lookup_name(row[1], row[2], row[3]),
        "city": row[4],
        "prov_state": row[5],
        "country": row[6] or row[7] or row[8] or "",
    }



def _lookup_calls_local(conn: sqlite3.Connection, callsign: str) -> Optional[dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT callsign, payload_json
        FROM calls
        WHERE UPPER(callsign) = UPPER(?)
        LIMIT 1
    """, (callsign,))
    row = cur.fetchone()
    if not row:
        return None

    payload = {}
    try:
        payload = json.loads(row[1] or "{}")
    except Exception as e:
        _debug_log(f"_lookup_calls_local: payload_json decode failed callsign={callsign!r}: {e}")
        payload = {}

    source_payload = None
    source_name = "calls"
    for candidate in ("hamdb", "hamqth", "qrz"):
        candidate_payload = payload.get(candidate)
        if isinstance(candidate_payload, dict) and candidate_payload:
            source_payload = candidate_payload
            source_name = candidate
            break

    if source_payload is None:
        source_payload = payload if isinstance(payload, dict) else {}

    first = _clean_lookup_value(
        source_payload.get("fname")
        or source_payload.get("first_name")
        or source_payload.get("first")
    )
    middle = _clean_lookup_value(
        source_payload.get("mi")
        or source_payload.get("middle")
        or source_payload.get("middle_name")
    )
    last = _clean_lookup_value(
        source_payload.get("name")
        or source_payload.get("last_name")
        or source_payload.get("surname")
    )

    result = {
        "callsign": row[0] or source_payload.get("call") or source_payload.get("callsign") or callsign.upper(),
        "name": _format_lookup_name(first, middle, last) or _clean_lookup_value(source_payload.get("adr_name")),
        "city": (
            source_payload.get("addr2")
            or source_payload.get("adr_city")
            or source_payload.get("city")
            or ""
        ),
        "prov_state": (
            source_payload.get("state")
            or source_payload.get("prov_state")
            or source_payload.get("district")
            or source_payload.get("us_state")
            or source_payload.get("province")
            or ""
        ),
        "country": (
            source_payload.get("country")
            or source_payload.get("mailing_country")
            or source_payload.get("prefix_country")
            or source_payload.get("dxcc_name")
            or source_payload.get("adr_country")
            or ""
        ),
        "source": source_name,
    }
    return result


def _calls_table_has_columns(conn, wanted_cols) -> bool:
    try:
        cols = _table_columns_generic(conn, "calls")
        return all(col.lower() in cols for col in wanted_cols)
    except Exception:
        return False


def _table_columns(conn, table_name: str) -> set:
    return _table_columns_generic(conn, table_name)


def _split_name_parts(full_name: str) -> Tuple[str, str, str]:
    full_name = _clean_lookup_value(full_name)
    if not full_name:
        return "", "", ""
    parts = full_name.split()
    if len(parts) <= 1:
        return full_name, "", ""
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return parts[0], " ".join(parts[1:-1]), parts[-1]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_qrz_callsign_payload(root: ET.Element, callsign: str, session_id: str = "", raw_xml: str = "") -> dict:
    def q(path: str, default: str = "") -> str:
        return _clean_lookup_value(_xml_findtext_ns_agnostic(root, path, default))

    lookup_call = _normalize_callsign(q("Callsign/call", callsign) or callsign)
    first_name = q("Callsign/fname")
    last_name = q("Callsign/name")
    display_name = _format_lookup_name(first_name, "", last_name)

    lat = q("Callsign/lat") or q("Callsign/latitude")
    lon = q("Callsign/lon") or q("Callsign/longitude")
    country = q("Callsign/country")
    state = q("Callsign/state")
    city = q("Callsign/addr2")
    postal = q("Callsign/zip")
    street = q("Callsign/addr1")

    data = {
        "callsign": lookup_call,
        "class": q("Callsign/class"),
        "bmcode": q("Callsign/codes"),
        "first_name": first_name,
        "middle": "",
        "last_name": last_name,
        "suffix": q("Callsign/suffix"),
        "street": street,
        "prefix_country": q("Callsign/land") or country,
        "po_box": q("Callsign/attn"),
        "city": city,
        "state_province": state,
        "postal_code": postal,
        "birthdate": q("Callsign/born"),
        "date_first_issue": q("Callsign/efdate"),
        "expiration_date": q("Callsign/expdate"),
        "process_date": q("Callsign/moddate"),
        "county": q("Callsign/county") or q("Callsign/fips"),
        "gmt_offset": q("Callsign/GMToff") or q("Callsign/gmtoff") or q("Callsign/timezone"),
        "latitude": lat,
        "longitude": lon,
        "grid": q("Callsign/grid"),
        "area_code": q("Callsign/AreaCode") or q("Callsign/areacode"),
        "previous_call": q("Callsign/p_call") or q("Callsign/xref"),
        "previous_class": "",
        "transaction_type": "",
        "email": q("Callsign/email"),
        "qsl_manager": q("Callsign/qslmgr"),
        "mailing_country": country,
        "url": q("Callsign/url"),
        "vanity_flag": q("Callsign/vanity"),
        "fax": q("Callsign/fax"),
        "interest_profile": q("Callsign/bio"),
        "phone": q("Callsign/phone"),
        "fcc_licensee_id": q("Callsign/serial"),
        "ten_ten_number": "",
        "fcc_frn": q("Callsign/frn"),
        "iota": q("Callsign/iota"),
        "fists": q("Callsign/fists"),
        "qcwa": q("Callsign/qcwa"),
        "ootc": q("Callsign/ootc"),
        "fcc_license_key": q("Callsign/uls_url"),
        "naqcc": q("Callsign/naqcc"),
        "skcc": q("Callsign/skcc"),
        "dxcc_number": q("Callsign/dxcc"),
        "dxcc_name": country,
        "last_lookup_utc": _utc_now_iso(),
        "raw": raw_xml or "",
        "data_json": "",
        # convenience fields for current UI/cache logic
        "name": display_name,
        "prov_state": state,
        "country": country,
        "session_id": _clean_lookup_value(session_id),
    }
    if not data["data_json"]:
        data["data_json"] = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return data


def _upsert_hamcall_calls_from_qrz(conn, info: dict) -> None:
    if not info:
        return

    if conn.__class__.__module__.split('.')[0] in ('pymysql', 'mysql', 'mariadb'):
        _mysql_upsert_hamcall_calls_from_qrz(conn, info)
        return

    cols = _table_columns(conn, "hamcall_calls")
    if not cols:
        return

    callsign = _normalize_callsign(info.get("callsign"))
    if not callsign:
        return

    row = {
        "callsign": callsign,
        "class": _clean_lookup_value(info.get("class")),
        "bmcode": _clean_lookup_value(info.get("bmcode")),
        "first_name": _clean_lookup_value(info.get("first_name")),
        "middle": _clean_lookup_value(info.get("middle")),
        "last_name": _clean_lookup_value(info.get("last_name")),
        "suffix": _clean_lookup_value(info.get("suffix")),
        "street": _clean_lookup_value(info.get("street")),
        "prefix_country": _clean_lookup_value(info.get("prefix_country")),
        "po_box": _clean_lookup_value(info.get("po_box")),
        "city": _clean_lookup_value(info.get("city")),
        "state_province": _clean_lookup_value(info.get("state_province") or info.get("prov_state")),
        "postal_code": _clean_lookup_value(info.get("postal_code")),
        "birthdate": _clean_lookup_value(info.get("birthdate")),
        "date_first_issue": _clean_lookup_value(info.get("date_first_issue")),
        "expiration_date": _clean_lookup_value(info.get("expiration_date")),
        "process_date": _clean_lookup_value(info.get("process_date")),
        "county": _clean_lookup_value(info.get("county")),
        "gmt_offset": _clean_lookup_value(info.get("gmt_offset")),
        "latitude": _clean_lookup_value(info.get("latitude")),
        "longitude": _clean_lookup_value(info.get("longitude")),
        "grid": _clean_lookup_value(info.get("grid")),
        "area_code": _clean_lookup_value(info.get("area_code")),
        "previous_call": _clean_lookup_value(info.get("previous_call")),
        "previous_class": _clean_lookup_value(info.get("previous_class")),
        "transaction_type": _clean_lookup_value(info.get("transaction_type")),
        "email": _clean_lookup_value(info.get("email")),
        "qsl_manager": _clean_lookup_value(info.get("qsl_manager")),
        "mailing_country": _clean_lookup_value(info.get("mailing_country") or info.get("country")),
        "url": _clean_lookup_value(info.get("url")),
        "vanity_flag": _clean_lookup_value(info.get("vanity_flag")),
        "fax": _clean_lookup_value(info.get("fax")),
        "interest_profile": _clean_lookup_value(info.get("interest_profile")),
        "phone": _clean_lookup_value(info.get("phone")),
        "fcc_licensee_id": _clean_lookup_value(info.get("fcc_licensee_id")),
        "ten_ten_number": _clean_lookup_value(info.get("ten_ten_number")),
        "fcc_frn": _clean_lookup_value(info.get("fcc_frn")),
        "iota": _clean_lookup_value(info.get("iota")),
        "fists": _clean_lookup_value(info.get("fists")),
        "qcwa": _clean_lookup_value(info.get("qcwa")),
        "ootc": _clean_lookup_value(info.get("ootc")),
        "fcc_license_key": _clean_lookup_value(info.get("fcc_license_key")),
        "naqcc": _clean_lookup_value(info.get("naqcc")),
        "skcc": _clean_lookup_value(info.get("skcc")),
        "dxcc_number": _clean_lookup_value(info.get("dxcc_number")),
        "dxcc_name": _clean_lookup_value(info.get("dxcc_name") or info.get("country")),
        "last_lookup_utc": _clean_lookup_value(info.get("last_lookup_utc")) or _utc_now_iso(),
        "raw": _clean_lookup_value(info.get("raw")),
        "data_json": _clean_lookup_value(info.get("data_json")),
    }

    if not row["first_name"] and not row["last_name"]:
        first_name, middle, last_name = _split_name_parts(info.get("name"))
        row["first_name"] = row["first_name"] or first_name
        row["middle"] = row["middle"] or middle
        row["last_name"] = row["last_name"] or last_name

    if not row["data_json"]:
        row["data_json"] = json.dumps(info, ensure_ascii=False, sort_keys=True)

    insertable = {k: row.get(k, "") for k in row if k.lower() in cols}
    if "callsign" not in insertable:
        return

    ordered_cols = list(insertable.keys())
    placeholders = ", ".join(["?"] * len(ordered_cols))
    sql = f"INSERT OR REPLACE INTO hamcall_calls ({', '.join(ordered_cols)}) VALUES ({placeholders})"
    conn.execute(sql, [insertable[col] for col in ordered_cols])
    conn.commit()


def _cache_hamqth_result_in_calls(conn, info: dict) -> None:
    if not info:
        return

    callsign = _normalize_callsign(info.get("callsign"))
    if not callsign:
        return

    payload = {
        "hamqth": {
            "callsign": callsign,
            "name": _clean_lookup_value(info.get("name")),
            "city": _clean_lookup_value(info.get("city")),
            "prov_state": _clean_lookup_value(info.get("prov_state")),
            "country": _clean_lookup_value(info.get("country")),
            "grid": _clean_lookup_value(info.get("grid")),
            "latitude": _clean_lookup_value(info.get("latitude")),
            "longitude": _clean_lookup_value(info.get("longitude")),
            "street": _clean_lookup_value(info.get("street")),
            "postal_code": _clean_lookup_value(info.get("postal_code")),
        }
    }

    cur = conn.cursor()
    cur.execute("SELECT 1 FROM calls WHERE UPPER(callsign)=UPPER(?) LIMIT 1", (callsign,))
    if cur.fetchone():
        return

    if _calls_table_has_columns(conn, ["callsign", "grid", "name", "payload_json"]):
        cur.execute(
            """
            INSERT INTO calls (callsign, grid, name, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                callsign,
                _clean_lookup_value(info.get("grid")),
                _clean_lookup_value(info.get("name")),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
        return

    if _calls_table_has_columns(conn, ["callsign", "payload_json"]):
        cur.execute(
            """
            INSERT INTO calls (callsign, payload_json)
            VALUES (?, ?)
            """,
            (
                callsign,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def _cache_online_result_in_calls(conn, info: dict, provider: str) -> None:
    if not info:
        return

    provider_key = (provider or "").strip().lower()
    if provider_key not in ("hamdb.org", "hamqth.com", "qrz.com", "hamdb", "hamqth", "qrz"):
        return

    provider_key = {
        "hamdb.org": "hamdb",
        "hamqth.com": "hamqth",
        "qrz.com": "qrz",
    }.get(provider_key, provider_key)

    callsign = _normalize_callsign(info.get("callsign"))
    if not callsign:
        return

    payload = {
        provider_key: {
            "callsign": callsign,
            "call": callsign,
            "name": _clean_lookup_value(info.get("name")),
            "adr_name": _clean_lookup_value(info.get("name")),
            "city": _clean_lookup_value(info.get("city")),
            "addr2": _clean_lookup_value(info.get("city")),
            "adr_city": _clean_lookup_value(info.get("city")),
            "prov_state": _clean_lookup_value(info.get("prov_state")),
            "state": _clean_lookup_value(info.get("prov_state")),
            "country": _clean_lookup_value(info.get("country")),
            "grid": _clean_lookup_value(info.get("grid")),
            "latitude": _clean_lookup_value(info.get("latitude")),
            "longitude": _clean_lookup_value(info.get("longitude")),
            "street": _clean_lookup_value(info.get("street")),
            "postal_code": _clean_lookup_value(info.get("postal_code")),
        }
    }

    if conn.__class__.__module__.split('.')[0] in ('pymysql', 'mysql', 'mariadb'):
        _mysql_insert_or_update_calls(
            conn,
            callsign,
            _clean_lookup_value(info.get("grid")),
            _clean_lookup_value(info.get("name")),
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        )
        return

    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM calls WHERE UPPER(callsign)=UPPER(?) LIMIT 1", (callsign,))
    row = cur.fetchone()
    if row:
        try:
            existing = json.loads(row[0] or "{}")
            if not isinstance(existing, dict):
                existing = {}
        except Exception:
            existing = {}
        existing[provider_key] = payload[provider_key]
        if _calls_table_has_columns(conn, ["payload_json"]):
            cur.execute(
                "UPDATE calls SET payload_json=? WHERE UPPER(callsign)=UPPER(?)",
                (json.dumps(existing, ensure_ascii=False, sort_keys=True), callsign),
            )
            conn.commit()
        return

    if _calls_table_has_columns(conn, ["callsign", "grid", "name", "payload_json"]):
        cur.execute(
            """
            INSERT INTO calls (callsign, grid, name, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                callsign,
                _clean_lookup_value(info.get("grid")),
                _clean_lookup_value(info.get("name")),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
        return

    if _calls_table_has_columns(conn, ["callsign", "payload_json"]):
        cur.execute(
            """
            INSERT INTO calls (callsign, payload_json)
            VALUES (?, ?)
            """,
            (
                callsign,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def _lookup_callsign_local_first(callsign: str, cfg: Optional[AppConfig] = None) -> Tuple[Optional[dict], str]:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return None, ""

    module = _load_callsign_lookup_module()
    db_path = _callsign_db_path()

    if _db_is_mysql_cfg(cfg):
        try:
            if cfg is None:
                cfg = load_config()
            with _mysql_connect_cfg(cfg) as conn:
                try:
                    raw = _lookup_hamcall_calls_db(conn, callsign)
                    result = _normalize_lookup_result(raw, "hamcall_calls", callsign)
                    if result:
                        _debug_log(f"lookup source=hamcall_calls callsign={callsign!r} db=mysql")
                        return result, "mysql"
                except Exception as e:
                    _debug_log(f"lookup error source=hamcall_calls callsign={callsign!r} db='mysql': {e}")

                try:
                    raw = _lookup_calls_db(conn, callsign)
                    result = _normalize_lookup_result(raw, (raw or {}).get("source", "calls"), callsign)
                    if result:
                        _debug_log(f"lookup source={result.get('source','calls')} callsign={callsign!r} db=mysql")
                        return result, "mysql"
                except Exception as e:
                    _debug_log(f"lookup error source=calls callsign={callsign!r} db='mysql': {e}")
        except Exception as e:
            _debug_log(f"_lookup_callsign_local_first: mysql open failed callsign={callsign!r}: {e}")
        return None, "mysql"

    _debug_log(f"_lookup_callsign_local_first: callsign={callsign!r} db_path={db_path!r} exists={os.path.exists(db_path)}")
    try:
        with sqlite3.connect(db_path) as conn:
            try:
                if module is not None and hasattr(module, "lookup_hamcall_calls"):
                    raw = module.lookup_hamcall_calls(conn, callsign)
                else:
                    raw = _lookup_hamcall_calls_local(conn, callsign)
                result = _normalize_lookup_result(raw, "hamcall_calls", callsign)
                if result:
                    _debug_log(f"lookup source=hamcall_calls callsign={callsign!r}")
                    return result, db_path
            except Exception as e:
                _debug_log(f"lookup error source=hamcall_calls callsign={callsign!r} db={db_path!r}: {e}")

            try:
                if module is not None and hasattr(module, "lookup_calls"):
                    raw = module.lookup_calls(conn, callsign)
                else:
                    raw = _lookup_calls_local(conn, callsign)
                result = _normalize_lookup_result(raw, (raw or {}).get("source", "calls"), callsign)
                if result:
                    _debug_log(f"lookup source={result.get('source','calls')} callsign={callsign!r}")
                    return result, db_path
            except Exception as e:
                _debug_log(f"lookup error source=calls callsign={callsign!r} db={db_path!r}: {e}")
    except Exception as e:
        _debug_log(f"_lookup_callsign_local_first: sqlite open failed callsign={callsign!r} db={db_path!r}: {e}")

    return None, db_path





def lookup_callsign_info(callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {"callsign": "", "name": "", "city": "", "prov_state": "", "country": "", "source": "not found"}

    local_result, db_path = _lookup_callsign_local_first(callsign, cfg if 'cfg' in locals() else None)
    if local_result:
        return local_result

    module = _load_callsign_lookup_module()
    try:
        if module is not None and hasattr(module, "lookup_hamqth"):
            hamqth_raw = module.lookup_hamqth(callsign)
        else:
            hamqth_raw = _lookup_hamqth_local(callsign)

        result = _normalize_lookup_result(hamqth_raw, "hamqth", callsign)
        if result:
            _debug_log(f"lookup source=online provider=hamqth.com callsign={callsign!r}")
            try:
                merged = dict(hamqth_raw or {})
                merged.setdefault("callsign", result.get("callsign"))
                merged.setdefault("name", result.get("name"))
                merged.setdefault("city", result.get("city"))
                merged.setdefault("prov_state", result.get("prov_state"))
                merged.setdefault("country", result.get("country"))
                if _db_is_mysql_cfg():
                    cfg2 = load_config()
                    with _mysql_connect_cfg(cfg2) as conn:
                        _cache_online_result_in_calls(conn, merged, "hamqth.com")
                else:
                    with sqlite3.connect(db_path) as conn:
                        _cache_online_result_in_calls(conn, merged, "hamqth.com")
            except Exception as e:
                _debug_log(f"lookup cache write failed provider='hamqth.com' callsign={callsign!r} db={db_path!r}: {e}")
            return result
    except Exception as e:
        _debug_log(f"lookup error source=online provider='hamqth.com' callsign={callsign!r}: {e}")

    _debug_log(f"lookup source=not_found callsign={callsign!r}")
    return {
        "callsign": callsign,
        "name": "",
        "city": "",
        "prov_state": "",
        "country": "",
        "source": "not found",
    }


def _callsign_lookup_cache_path(filename: str) -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, filename)


def _load_cached_session(cache_file: str, max_age: int) -> str:
    path = _callsign_lookup_cache_path(cache_file)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        session_id = _clean_lookup_value(data.get("session_id"))
        saved_at = float(data.get("saved_at", 0))
        if session_id and (time.time() - saved_at) < float(max_age):
            return session_id
    except Exception:
        pass
    return ""


def _save_cached_session(cache_file: str, session_id: str) -> None:
    path = _callsign_lookup_cache_path(cache_file)
    tmp = path + ".tmp"
    data = {"session_id": _clean_lookup_value(session_id), "saved_at": time.time()}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _clear_cached_session(cache_file: str) -> None:
    path = _callsign_lookup_cache_path(cache_file)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _format_lookup_history_line(state: AppState) -> str:
    with state.online_lookup_lock:
        items = list(state.online_lookup_history)
    if not items:
        return "Lookup: idle"
    return " | ".join(items)


def _push_online_lookup_status(state: AppState, provider: str, session_id: str, callsign: str) -> None:
    provider = _clean_lookup_value(provider) or "-"
    sid = _clean_lookup_value(session_id) or "-"
    cs = _normalize_callsign(callsign)
    item = f"{provider} {sid} {cs}".strip()
    with state.online_lookup_lock:
        state.lookup_provider_display = provider
        state.lookup_session_id_display = sid
        state.online_lookup_history.insert(0, item)
        state.online_lookup_history = state.online_lookup_history[:4]
        state.online_lookup_status_line = f"Lookup: {provider} sid={sid}"
        if cs:
            state.online_lookup_status_line += f" call={cs}"
    state.dirty_status = True



def _session_id_for_provider_cfg(cfg: AppConfig, provider: str) -> str:
    provider = (_clean_lookup_value(provider) or "hamdb.org").lower()
    if provider == "hamqth.com":
        return _get_hamqth_session_id_cfg(cfg)
    if provider == "qrz.com":
        return _get_qrz_session_id(cfg)
    return "-"



def _set_lookup_provider_session_display(state: AppState, provider: str, session_id: str) -> None:
    provider_clean = _clean_lookup_value(provider) or "hamdb.org"
    sid = _clean_lookup_value(session_id) or "-"
    source = "-"
    if provider_clean.lower() == "qrz.com":
        source = QRZ_RUNTIME_SESSION_SOURCE or "-"
        if sid == "error" and _clean_lookup_value(QRZ_RUNTIME_SESSION_ID):
            sid = _clean_lookup_value(QRZ_RUNTIME_SESSION_ID)
            source = QRZ_RUNTIME_SESSION_SOURCE or source
    with state.online_lookup_lock:
        state.lookup_provider_display = provider_clean
        state.lookup_session_id_display = sid
        state.online_lookup_status_line = f"Lookup: {provider_clean} sid={sid}"
        if source and source != "-":
            state.online_lookup_status_line += f" src={source}"
    state.dirty_status = True


def initialize_online_lookup_session(cfg: AppConfig, state: AppState) -> None:
    provider = _get_selected_lookup_provider(cfg)
    _debug_log(f"initialize_online_lookup_session: provider={provider!r}")
    try:
        session_id = _session_id_for_provider_cfg(cfg, provider)
        _debug_log(f"initialize_online_lookup_session: resolved session_id={session_id!r} provider={provider!r}")
        _set_lookup_provider_session_display(state, provider, session_id)
    except Exception as e:
        _log_exception(f"initialize_online_lookup_session provider={provider!r}", e)
        if provider == "qrz.com" and _clean_lookup_value(QRZ_RUNTIME_SESSION_ID):
            _set_lookup_provider_session_display(state, provider, QRZ_RUNTIME_SESSION_ID)
        else:
            _set_lookup_provider_session_display(state, provider, "error")
        state.status_line = f"Lookup session init failed for {provider}: {e}"
        state.dirty_status = True

def _get_selected_lookup_provider(cfg: AppConfig) -> str:
    v = (_clean_lookup_value(getattr(cfg, "online_lookup_website", "")) or "hamdb.org").lower()
    if v not in ("hamdb.org", "hamqth.com", "qrz.com"):
        return "hamdb.org"
    return v


def _fetch_xml_url(url: str, timeout: float = 20.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "MyHamClock/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read()


def _fetch_hamdb_xml(callsign: str) -> bytes:
    url = f"https://api.hamdb.org/{urllib.parse.quote(callsign)}/xml/hamclock"
    return _fetch_xml_url(url)


def _lookup_hamdb_online(callsign: str) -> Optional[dict]:
    root = ET.fromstring(_fetch_hamdb_xml(callsign))

    def _t(path: str) -> str:
        node = root.find(path)
        return (node.text or "").strip() if node is not None and node.text is not None else ""

    status = (_t(".//messages/status") or _t(".//status")).lower()
    if "not found" in status:
        return None

    call = _normalize_callsign(_t(".//callsign/call") or callsign)
    fname = _t(".//callsign/fname")
    name = _t(".//callsign/name")
    full_name = " ".join(p for p in [fname, name] if p).strip()
    city = _t(".//callsign/addr2")
    prov = _t(".//callsign/state")
    country = _t(".//callsign/country")

    if not any([full_name, city, prov, country, call]):
        if status and "okay" not in status and "ok" not in status:
            return None

    return {
        "callsign": call or callsign,
        "name": full_name,
        "city": city,
        "prov_state": prov,
        "country": country,
    }


def _get_hamqth_credentials_from_cfg(cfg: AppConfig) -> Tuple[str, str]:
    return (
        _clean_lookup_value(getattr(cfg, "online_lookup_username", "")),
        _clean_lookup_value(getattr(cfg, "online_lookup_password", "")),
    )


def _load_hamqth_cached_session() -> str:
    return _load_cached_session(HAMQTH_CACHE_FILE, HAMQTH_CACHE_MAX_AGE)


def _save_hamqth_cached_session(session_id: str) -> None:
    _save_cached_session(HAMQTH_CACHE_FILE, session_id)


def _clear_hamqth_cached_session() -> None:
    _clear_cached_session(HAMQTH_CACHE_FILE)


def _get_hamqth_session_id_cfg(cfg: AppConfig) -> str:
    cached = _load_hamqth_cached_session()
    if cached:
        return cached

    username, password = _get_hamqth_credentials_from_cfg(cfg)
    if not username or not password:
        raise RuntimeError("HamQTH credentials not configured")

    xml_data = _fetch_hamqth_xml({"u": username, "p": password})
    root = ET.fromstring(xml_data)
    session_id = _xml_text(root, ".//hq:session/hq:session_id")
    error_text = _xml_text(root, ".//hq:session/hq:error")
    if session_id:
        _save_hamqth_cached_session(session_id)
        return session_id
    if error_text:
        raise RuntimeError(error_text)
    raise RuntimeError("HamQTH session request failed")


def _lookup_hamqth_online(cfg: AppConfig, callsign: str) -> Tuple[Optional[dict], str]:
    session_id = _get_hamqth_session_id_cfg(cfg)

    def _do_lookup(current_session_id: str):
        xml_data = _fetch_hamqth_xml({
            "id": current_session_id,
            "callsign": callsign,
            "prg": HAMQTH_PROGRAM,
        })
        return ET.fromstring(xml_data)

    root = _do_lookup(session_id)
    error_text = _xml_text(root, ".//hq:session/hq:error")
    if error_text and "expired" in error_text.lower():
        _clear_hamqth_cached_session()
        session_id = _get_hamqth_session_id_cfg(cfg)
        root = _do_lookup(session_id)
        error_text = _xml_text(root, ".//hq:session/hq:error")

    if error_text:
        if "not found" in error_text.lower():
            return None, session_id
        raise RuntimeError(error_text)

    return ({
        "callsign": _normalize_callsign(
            _xml_text(root, ".//hq:search/hq:callsign")
            or _xml_text(root, ".//hq:search/hq:call")
            or callsign
        ),
        "name": _xml_text(root, ".//hq:search/hq:adr_name"),
        "city": _xml_text(root, ".//hq:search/hq:adr_city"),
        "prov_state": (
            _xml_text(root, ".//hq:search/hq:district")
            or _xml_text(root, ".//hq:search/hq:oblast")
            or _xml_text(root, ".//hq:search/hq:us_state")
            or _xml_text(root, ".//hq:search/hq:state")
        ),
        "country": (
            _xml_text(root, ".//hq:search/hq:country")
            or _xml_text(root, ".//hq:search/hq:adr_country")
        ),
    }, session_id)


def _fetch_qrz_xml(params: dict) -> bytes:
    url = QRZ_URL + "?" + urllib.parse.urlencode(params)
    _debug_log(f"_fetch_qrz_xml request: {_mask_sensitive_text(url)}")
    data = _fetch_xml_url(url)
    try:
        preview = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
        _debug_log(f"_fetch_qrz_xml response: {_truncate_for_log(_mask_sensitive_text(preview), 2000)}")
    except Exception as exc:
        _log_exception("_fetch_qrz_xml response logging failed", exc)
    return data

def _xml_findtext_ns_agnostic(root: ET.Element, path: str, default: str = "") -> str:
    """
    Find element text while ignoring XML namespaces in tags.
    Supports simple slash-separated paths like:
      Session/Key
      Callsign/call
    """
    if root is None:
        return default
    parts = [p for p in path.split("/") if p]
    if not parts:
        return default

    current = [root]
    for part in parts:
        next_nodes = []
        for node in current:
            for child in list(node):
                tag = child.tag.rsplit("}", 1)[-1] if isinstance(child.tag, str) else child.tag
                if tag == part:
                    next_nodes.append(child)
        if not next_nodes:
            return default
        current = next_nodes

    text = current[0].text if current else None
    return (text or default).strip()




def _write_text_file(path: str, content: str) -> None:
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
        _debug_log(f"_write_text_file: path={path!r} bytes={len(content.encode('utf-8', errors='ignore'))}")
    except Exception as exc:
        _log_exception(f"_write_text_file failed path={path!r}", exc)


def _load_text_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = f.read()
        _debug_log(f"_load_text_file: path={path!r} chars={len(data)}")
        return data
    except Exception as exc:
        _log_exception(f"_load_text_file failed path={path!r}", exc)
        return ""


def _parse_qrz_subexp_datetime(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return None


def _extract_qrz_fields_from_text(xml_text: str) -> dict:
    xml_text = "" if xml_text is None else str(xml_text)
    out = {"key": "", "subexp": "", "error": "", "gmtime": "", "count": ""}
    for name in out:
        m = re.search(rf"<\s*{name}\s*>\s*(.*?)\s*<\s*/\s*{name}\s*>", xml_text, re.IGNORECASE | re.DOTALL)
        out[name] = m.group(1).strip() if m else ""
    return out


def _is_qrz_session_valid_from_xml(root: ET.Element = None, xml_text: str = "") -> bool:
    key = ""
    error = ""
    subexp = ""
    if root is not None:
        try:
            key = _xml_findtext_ns_agnostic(root, "Session/Key")
            error = _xml_findtext_ns_agnostic(root, "Session/Error")
            subexp = _xml_findtext_ns_agnostic(root, "Session/SubExp")
        except Exception:
            pass
    if not key or (not subexp and not error):
        fields = _extract_qrz_fields_from_text(xml_text)
        key = key or fields["key"]
        error = error or fields["error"]
        subexp = subexp or fields["subexp"]
    _debug_log(f"_is_qrz_session_valid_from_xml: key={key!r} subexp={subexp!r} error={error!r}")
    if not key:
        return False
    if error:
        return False
    expiry_dt = _parse_qrz_subexp_datetime(subexp)
    if expiry_dt is None:
        return True
    now_dt = datetime.now(expiry_dt.tzinfo) if getattr(expiry_dt, "tzinfo", None) else datetime.now()
    valid = expiry_dt > now_dt
    _debug_log(f"_is_qrz_session_valid_from_xml: parsed_expiry={expiry_dt!r} now={now_dt!r} valid={valid}")
    return valid


def _candidate_qrz_xml_paths() -> list:
    paths = []
    for p in (
        QRZ_XML_FILE,
        _app_file_path("qrz.xml"),
        os.path.join(os.getcwd(), "qrz.xml"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "qrz.xml"),
    ):
        if p and p not in paths:
            paths.append(p)
    return paths


def _load_qrz_session_from_xml_file() -> str:
    for path in _candidate_qrz_xml_paths():
        _debug_log(f"_load_qrz_session_from_xml_file: trying path={path!r} exists={os.path.exists(path)}")
        xml = _load_text_file(path)
        if not xml.strip():
            continue
        fields = _extract_qrz_fields_from_text(xml)
        _debug_log(f"_load_qrz_session_from_xml_file: regex fields key={fields['key']!r} subexp={fields['subexp']!r} error={fields['error']!r}")
        root = None
        try:
            root = ET.fromstring(xml)
        except Exception as exc:
            _log_exception("_load_qrz_session_from_xml_file parse failed", exc)
        if _is_qrz_session_valid_from_xml(root=root, xml_text=xml):
            _debug_log("_load_qrz_session_from_xml_file: xml considered valid")
            key = fields["key"]
            if not key and root is not None:
                key = _xml_findtext_ns_agnostic(root, "Session/Key")
            if key:
                _debug_log(f"_load_qrz_session_from_xml_file: returning key={key!r} from path={path!r}")
                return key
    _debug_log("_load_qrz_session_from_xml_file: no usable qrz.xml found")
    return ""


def _save_qrz_session_xml(xml_text: str) -> None:
    _write_text_file(QRZ_XML_FILE, xml_text)


def _fetch_qrz_session_xml(cfg: AppConfig) -> str:
    username = _clean_lookup_value(getattr(cfg, "online_lookup_username", ""))
    password = _clean_lookup_value(getattr(cfg, "online_lookup_password", ""))
    _debug_log(f"_fetch_qrz_session_xml: username={username!r} password_configured={bool(password)}")
    if not username or not password:
        raise RuntimeError("QRZ credentials not configured")
    xml_text = _fetch_qrz_xml({"username": username, "password": password, "agent": "hamclock"})
    if isinstance(xml_text, (bytes, bytearray)):
        xml_text = xml_text.decode("utf-8", errors="replace")
    _debug_log(f"_fetch_qrz_session_xml raw xml: {_truncate_for_log(_mask_sensitive_text(xml_text), 2000)}")
    _save_qrz_session_xml(xml_text)
    return xml_text

def _load_qrz_cached_session() -> str:
    sid = _load_cached_session(QRZ_CACHE_FILE, QRZ_CACHE_MAX_AGE)
    _debug_log(f"_load_qrz_cached_session: path={QRZ_CACHE_FILE!r} sid={sid!r}")
    return sid


def _save_qrz_cached_session(session_id: str) -> None:
    _debug_log(f"_save_qrz_cached_session: path={QRZ_CACHE_FILE!r} sid={session_id!r}")
    _save_cached_session(QRZ_CACHE_FILE, session_id)



def _clear_qrz_cached_session() -> None:
    global QRZ_RUNTIME_SESSION_ID, QRZ_RUNTIME_SESSION_SOURCE
    _debug_log(f"_clear_qrz_cached_session: path={QRZ_CACHE_FILE!r}")
    _clear_cached_session(QRZ_CACHE_FILE)
    QRZ_RUNTIME_SESSION_ID = ""
    QRZ_RUNTIME_SESSION_SOURCE = "-"


def _get_qrz_session_id(cfg: AppConfig, force_refresh: bool = False) -> str:
    global QRZ_RUNTIME_SESSION_ID, QRZ_RUNTIME_SESSION_SOURCE
    _debug_log(f"_get_qrz_session_id: begin force_refresh={force_refresh!r} runtime_sid_present={bool(QRZ_RUNTIME_SESSION_ID)} source={QRZ_RUNTIME_SESSION_SOURCE!r}")

    if not force_refresh and _clean_lookup_value(QRZ_RUNTIME_SESSION_ID):
        sid = _clean_lookup_value(QRZ_RUNTIME_SESSION_ID)
        _debug_log(f"_get_qrz_session_id: returning in-memory sid={sid!r} source={QRZ_RUNTIME_SESSION_SOURCE!r}")
        return sid

    if not force_refresh:
        xml_sid = _load_qrz_session_from_xml_file()
        _debug_log(f"_get_qrz_session_id: xml_sid={xml_sid!r}")
        if xml_sid:
            sid = _clean_lookup_value(xml_sid)
            QRZ_RUNTIME_SESSION_ID = sid
            QRZ_RUNTIME_SESSION_SOURCE = "xml"
            _save_qrz_cached_session(sid)
            return sid

        cached = _load_qrz_cached_session()
        _debug_log(f"_get_qrz_session_id: cached_sid={cached!r}")
        if cached:
            sid = _clean_lookup_value(cached)
            QRZ_RUNTIME_SESSION_ID = sid
            QRZ_RUNTIME_SESSION_SOURCE = "cache"
            return sid

    xml_text = _fetch_qrz_session_xml(cfg)
    fields = _extract_qrz_fields_from_text(xml_text)
    key = fields["key"]
    error = fields["error"]
    _debug_log(f"_get_qrz_session_id: fresh regex fields key={key!r} subexp={fields['subexp']!r} error={error!r}")
    if not key:
        try:
            root = ET.fromstring(xml_text)
            key = _xml_findtext_ns_agnostic(root, "Session/Key")
            error = error or _xml_findtext_ns_agnostic(root, "Session/Error")
            _debug_log(f"_get_qrz_session_id: fresh xml fields key={key!r} error={error!r}")
        except Exception as exc:
            _log_exception("_get_qrz_session_id parse fresh xml failed", exc)
    if key:
        sid = _clean_lookup_value(key)
        QRZ_RUNTIME_SESSION_ID = sid
        QRZ_RUNTIME_SESSION_SOURCE = "live"
        _save_qrz_cached_session(sid)
        return sid
    if error:
        raise RuntimeError(error)
    raise RuntimeError("QRZ session request failed")

def _lookup_qrz_online(cfg: AppConfig, callsign: str) -> Tuple[Optional[dict], str]:
    session_id = _get_qrz_session_id(cfg)

    def _do_lookup(current_session_id: str):
        xml_bytes = _fetch_qrz_xml({"s": current_session_id, "callsign": callsign})
        xml_text = xml_bytes.decode("utf-8", errors="replace") if isinstance(xml_bytes, (bytes, bytearray)) else str(xml_bytes)
        return ET.fromstring(xml_bytes), xml_text

    root, xml_text = _do_lookup(session_id)
    error = _xml_findtext_ns_agnostic(root, "Session/Error")
    if error and any(x in error.lower() for x in ("session timeout", "not found", "invalid session", "expired")):
        if "not found" not in error.lower():
            _clear_qrz_cached_session()
            session_id = _get_qrz_session_id(cfg, force_refresh=True)
            root, xml_text = _do_lookup(session_id)
            error = _xml_findtext_ns_agnostic(root, "Session/Error")

    if error:
        if "not found" in error.lower():
            return None, session_id
        raise RuntimeError(error)

    return (_extract_qrz_callsign_payload(root, callsign, session_id=session_id, raw_xml=xml_text), session_id)





def _get_hamcall_calls_row(conn, callsign: str) -> Optional[dict]:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return None
    cols = _table_columns_generic(conn, "hamcall_calls")
    if not cols:
        return None
    ph = _param_placeholder_for_conn(conn)
    cur = conn.cursor()
    cur.execute(
        f"SELECT {', '.join(cols)} FROM hamcall_calls WHERE UPPER(callsign)=UPPER({ph}) LIMIT 1",
        (callsign,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(cols, row))


def _get_local_hamcall_calls_row_cfg(cfg: AppConfig, callsign: str, db_path_hint: str = "") -> Optional[dict]:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return None

    if _db_is_mysql_cfg(cfg):
        with _mysql_connect_cfg(cfg) as conn:
            return _get_hamcall_calls_row(conn, callsign)

    db_path = db_path_hint or _callsign_db_path()
    if not db_path:
        return None
    with sqlite3.connect(db_path) as conn:
        return _get_hamcall_calls_row(conn, callsign)


def _missing_hamcall_profile_fields(row: Optional[dict]) -> set:
    if not row:
        return {"first_name", "last_name", "city", "country"}

    country = (
        _clean_lookup_value(row.get("mailing_country"))
        or _clean_lookup_value(row.get("prefix_country"))
        or _clean_lookup_value(row.get("dxcc_name"))
        or _clean_lookup_value(row.get("country"))
    )
    missing = set()
    if not _clean_lookup_value(row.get("first_name")):
        missing.add("first_name")
    if not _clean_lookup_value(row.get("last_name")):
        missing.add("last_name")
    if not _clean_lookup_value(row.get("city")):
        missing.add("city")
    if not country:
        missing.add("country")
    return missing


def _missing_profile_fields_from_lookup_result(result: Optional[dict]) -> set:
    if not result:
        return {"first_name", "last_name", "city", "country"}

    missing = set()
    first_name, _middle, last_name = _split_name_parts(result.get("name"))
    if not first_name:
        missing.add("first_name")
    if not last_name:
        missing.add("last_name")
    if not _clean_lookup_value(result.get("city")):
        missing.add("city")
    if not _clean_lookup_value(result.get("country")):
        missing.add("country")
    return missing


def _merge_lookup_result(local_result: Optional[dict], online_result: Optional[dict], provider: str, callsign: str) -> dict:
    merged = {
        "callsign": _normalize_callsign(callsign),
        "name": "",
        "city": "",
        "prov_state": "",
        "country": "",
        "source": provider or "not found",
    }
    for source in (local_result or {}, online_result or {}):
        if not isinstance(source, dict):
            continue
        for key in ("callsign", "name", "city", "prov_state", "country"):
            value = _clean_lookup_value(source.get(key))
            if value:
                merged[key] = value
        src = _clean_lookup_value(source.get("source"))
        if src:
            merged["source"] = src
    merged["callsign"] = merged["callsign"] or _normalize_callsign(callsign)
    return merged


def _build_hamcall_upsert_payload(existing_row: Optional[dict], online_raw: Optional[dict], online_result: Optional[dict], callsign: str, provider: str) -> dict:
    payload = dict(existing_row or {})
    if isinstance(online_raw, dict):
        payload.update({k: v for k, v in online_raw.items() if v is not None})

    payload["callsign"] = _normalize_callsign(
        payload.get("callsign") or (online_result or {}).get("callsign") or callsign
    )

    first_name = _clean_lookup_value(payload.get("first_name"))
    middle = _clean_lookup_value(payload.get("middle"))
    last_name = _clean_lookup_value(payload.get("last_name"))

    if online_result and (not first_name or not last_name):
        split_first, split_middle, split_last = _split_name_parts(online_result.get("name"))
        first_name = first_name or split_first
        middle = middle or split_middle
        last_name = last_name or split_last

    payload["first_name"] = first_name
    if middle:
        payload["middle"] = middle
    payload["last_name"] = last_name
    if online_result:
        payload["city"] = _clean_lookup_value(payload.get("city")) or _clean_lookup_value(online_result.get("city"))
        payload["state_province"] = _clean_lookup_value(payload.get("state_province")) or _clean_lookup_value(online_result.get("prov_state"))
        country = _clean_lookup_value(payload.get("mailing_country")) or _clean_lookup_value(payload.get("country"))
        if not country:
            country = _clean_lookup_value(online_result.get("country"))
        if country:
            payload["mailing_country"] = _clean_lookup_value(payload.get("mailing_country")) or country
            payload["dxcc_name"] = _clean_lookup_value(payload.get("dxcc_name")) or country

    payload["name"] = _clean_lookup_value(payload.get("name")) or _clean_lookup_value((online_result or {}).get("name"))
    payload["prov_state"] = _clean_lookup_value(payload.get("prov_state")) or _clean_lookup_value((online_result or {}).get("prov_state"))
    payload["country"] = (
        _clean_lookup_value(payload.get("country"))
        or _clean_lookup_value(payload.get("mailing_country"))
        or _clean_lookup_value(payload.get("prefix_country"))
        or _clean_lookup_value(payload.get("dxcc_name"))
        or _clean_lookup_value((online_result or {}).get("country"))
    )
    payload["source"] = provider
    return payload


def _update_local_hamcall_calls_from_online(cfg: AppConfig, db_path: str, callsign: str, existing_row: Optional[dict], online_raw: Optional[dict], online_result: Optional[dict], provider: str) -> None:
    payload = _build_hamcall_upsert_payload(existing_row, online_raw, online_result, callsign, provider)

    if _db_is_mysql_cfg(cfg):
        with _mysql_connect_cfg(cfg) as conn:
            _upsert_hamcall_calls_from_qrz(conn, payload)
        return

    with sqlite3.connect(db_path) as conn:
        _upsert_hamcall_calls_from_qrz(conn, payload)

def lookup_callsign_info_cfg(cfg: AppConfig, state: AppState, callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {"callsign": "", "name": "", "city": "", "prov_state": "", "country": "", "source": "not found"}

    local_result, db_path = _lookup_callsign_local_first(callsign, cfg)
    local_hamcall_row = None
    try:
        local_hamcall_row = _get_local_hamcall_calls_row_cfg(cfg, callsign, db_path)
    except Exception as e:
        _debug_log(f"lookup local hamcall row failed callsign={callsign!r}: {e}")

    if local_hamcall_row is not None:
        missing_fields = _missing_hamcall_profile_fields(local_hamcall_row)
    else:
        missing_fields = _missing_profile_fields_from_lookup_result(local_result) if local_result else {"first_name", "last_name", "city", "country"}

    if local_result and not missing_fields:
        return local_result

    provider = _get_selected_lookup_provider(cfg)
    try:
        sid = "-"
        raw = None
        if provider == "hamdb.org":
            raw = _lookup_hamdb_online(callsign)
            _push_online_lookup_status(state, "hamdb.org", "-", callsign)
        elif provider == "hamqth.com":
            raw, sid = _lookup_hamqth_online(cfg, callsign)
            _push_online_lookup_status(state, "hamqth.com", sid, callsign)
        elif provider == "qrz.com":
            raw, sid = _lookup_qrz_online(cfg, callsign)
            _push_online_lookup_status(state, "qrz.com", sid, callsign)

        result = _normalize_lookup_result(raw, provider, callsign)
        if result:
            _debug_log(f"lookup source=online provider={provider!r} callsign={callsign!r}")
            merged_result = _merge_lookup_result(local_result, result, provider, callsign)

            try:
                merged = dict(raw or {})
                merged.setdefault("callsign", merged_result.get("callsign"))
                merged.setdefault("name", merged_result.get("name"))
                merged.setdefault("city", merged_result.get("city"))
                merged.setdefault("prov_state", merged_result.get("prov_state"))
                merged.setdefault("country", merged_result.get("country"))
                if _db_is_mysql_cfg(cfg):
                    with _mysql_connect_cfg(cfg) as conn:
                        _cache_online_result_in_calls(conn, merged, provider)
                else:
                    with sqlite3.connect(db_path) as conn:
                        _cache_online_result_in_calls(conn, merged, provider)
            except Exception as e:
                _debug_log(f"lookup cache write failed provider={provider!r} callsign={callsign!r} db={db_path!r}: {e}")

            online_filled_missing = missing_fields & (
                {"first_name"} if _clean_lookup_value(_build_hamcall_upsert_payload(local_hamcall_row, raw, result, callsign, provider).get("first_name")) else set()
            )
            online_filled_missing |= missing_fields & (
                {"last_name"} if _clean_lookup_value(_build_hamcall_upsert_payload(local_hamcall_row, raw, result, callsign, provider).get("last_name")) else set()
            )
            online_filled_missing |= missing_fields & (
                {"city"} if _clean_lookup_value(merged_result.get("city")) else set()
            )
            online_filled_missing |= missing_fields & (
                {"country"} if _clean_lookup_value(merged_result.get("country")) else set()
            )

            if online_filled_missing:
                try:
                    _update_local_hamcall_calls_from_online(cfg, db_path, callsign, local_hamcall_row, raw, result, provider)
                except Exception as e:
                    _debug_log(f"lookup hamcall update failed provider={provider!r} callsign={callsign!r} db={db_path!r}: {e}")

            return merged_result

        _debug_log(f"lookup source=not_found provider={provider!r} callsign={callsign!r}")
    except Exception as e:
        _debug_log(f"lookup error source=online provider={provider!r} callsign={callsign!r}: {e}")
        state.status_line = f"Lookup error ({provider}): {e}"
        state.dirty_status = True

    if local_result:
        return local_result

    return {
        "callsign": callsign,
        "name": "",
        "city": "",
        "prov_state": "",
        "country": "",
        "source": "not found",
    }

def parse_dx_cluster_spot(line: str) -> Optional[dict]:
    m = re.match(
        r'^\s*DX\s+de\s+([A-Z0-9/+-]+):\s+(\d+(?:\.\d+)?)\s+([A-Z0-9/+-]+)\s*(.*?)\s+(\d{4})Z(?:\s+([A-Ra-r]{2}\d{2}))?\s*$',
        line,
        re.IGNORECASE
    )
    if not m:
        return None

    comment = (m.group(4) or "").strip()

    return {
        "spotter": _normalize_callsign(m.group(1)),
        "frequency": m.group(2),
        "spotted": _normalize_callsign(m.group(3)),
        "comment": comment,
        "mode": extract_dx_mode(comment),
        "utc": m.group(5) + "Z",
        "grid": (m.group(6) or "").upper(),
    }


def get_cached_callsign_info(cfg: AppConfig, state: AppState, callsign: str) -> dict:
    key = _normalize_callsign(callsign)
    if not key:
        return {"callsign": "", "name": "", "city": "", "prov_state": "", "country": "", "source": "not found"}

    with state.dedx_lookup_lock:
        cached = state.dedx_lookup_cache.get(key)
    if cached is not None:
        return dict(cached)

    info = lookup_callsign_info_cfg(cfg, state, key)

    with state.dedx_lookup_lock:
        state.dedx_lookup_cache[key] = dict(info)

    return info


def _dedx_field(label: str, value: str, width: int) -> str:
    value = _clean_lookup_value(value) or "-"
    line = f"{label}: {value}"
    return line[:max(1, width)]


def build_dedx_lines(state: AppState, width: int, height: int) -> List[str]:
    if state.dedx_lines:
        lines = list(state.dedx_lines)
    else:
        lines = [
            "Waiting for DX cluster spot...",
            "",
            "DE = spotter",
            "DX = spotted",
        ]

    usable_h = max(1, height)
    if len(lines) < usable_h:
        lines.extend([""] * (usable_h - len(lines)))
    return [line[:max(1, width)] for line in lines[:usable_h]]



def _dx_frequency_to_hz(freq_value) -> Optional[int]:
    try:
        return int(round(float(str(freq_value).strip()) * 1000.0))
    except Exception:
        return None


def set_rigctld_frequency(cfg: AppConfig, state: AppState, freq_hz: int) -> bool:
    enabled = bool(getattr(cfg, "rig_control_enabled", False))
    if not enabled:
        state.status_line = "Rig control is disabled"
        state.dirty_status = True
        return False

    host = str(getattr(cfg, "rigctld_host", "127.0.0.1") or "127.0.0.1").strip()
    port = int(getattr(cfg, "rigctld_port", 4532) or 4532)

    try:
        with socket.create_connection((host, port), timeout=3.0) as s:
            s.settimeout(3.0)
            s.sendall(f"F {int(freq_hz)}\n".encode("utf-8", errors="ignore"))
            try:
                reply = s.recv(1024).decode("utf-8", errors="ignore").strip()
            except socket.timeout:
                reply = ""
        if reply and "RPRT 0" not in reply and reply != "0":
            state.status_line = f"Rig tune may have failed: {reply}"
            state.dirty_status = True
            return False

        state.rig_frequency = _rig_format_freq_hz(str(freq_hz))
        state.dirty_rig = True
        state.status_line = f"Rig tuned to {state.rig_frequency}"
        state.dirty_status = True
        return True
    except Exception as e:
        state.status_line = f"Rig tune failed: {e}"
        state.dirty_status = True
        return False



def _normalize_dx_mode_for_rig(raw_mode: str, freq_hz: Optional[int]) -> Tuple[str, int]:
    """
    Map DX spot mode text to a rig mode command and passband width.

    Rules:
      - Digital modes (FT8, FT4, PSK, JS8, etc.) => USB-D @ 3000
      - RTTY => RTTY @ 250
      - Voice / unknown modes above 14 MHz => USB @ 3000
      - Voice / unknown modes below 14 MHz => LSB @ 3000
      - CW => CW @ 1200
      - Explicit USB / LSB are preserved with 3000 passband
    """
    mode = str(raw_mode or "").strip().upper()
    freq_mhz = (freq_hz / 1_000_000.0) if freq_hz else 0.0

    digital_modes = {
        "DATA", "DIGI", "DIGITAL",
        "FT8", "FT4", "FT2", "JT65", "JT9", "JT", "JS8",
        "PSK", "OLIVIA", "MFSK", "PACKET",
        "VARA", "WINLINK", "FSQ", "HELL"
    }

    if mode == "CW":
        return ("CW", 1200)
    if mode == "RTTY":
        return ("RTTY", 250)
    if mode == "USB":
        return ("USB", 3000)
    if mode == "LSB":
        return ("LSB", 3000)
    if mode in ("SSB", "", "-"):
        return (("USB", 3000) if freq_mhz >= 14.0 else ("LSB", 3000))
    if mode in digital_modes:
        return ("USB-D", 3000)

    return (("USB", 3000) if freq_mhz >= 14.0 else ("LSB", 3000))



def set_rigctld_mode(cfg: AppConfig, state: AppState, mode: str, passband: int) -> bool:
    if not getattr(cfg, "rig_control_enabled", False):
        return False
    host = str(getattr(cfg, "rigctld_host", "127.0.0.1") or "127.0.0.1")
    port = int(getattr(cfg, "rigctld_port", 4532) or 4532)

    requested_mode = str(mode or "").strip().upper()
    requested_passband = int(passband or 0)

    # rigctld expects: M <mode> <passband>
    # For digital spots, try USB-D first, then PKTUSB, then USB.
    candidates = []
    if requested_mode == "USB-D":
        candidates = [("USB-D", requested_passband), ("PKTUSB", requested_passband), ("USB", requested_passband)]
    elif requested_mode == "CW":
        candidates = [("CW", requested_passband), ("CWR", requested_passband)]
    else:
        candidates = [(requested_mode, requested_passband)]

    last_reply = ""
    try:
        for mode_name, pb in candidates:
            with socket.create_connection((host, port), timeout=3.0) as s:
                s.settimeout(3.0)
                s.sendall((f"M {mode_name} {int(pb)}\n").encode("utf-8", errors="ignore"))
                try:
                    reply = s.recv(256).decode("utf-8", errors="ignore").strip()
                except Exception:
                    reply = ""
            last_reply = reply
            if not reply or "RPRT 0" in reply or reply == "0":
                state.rig_mode = mode_name
                state.dirty_rig = True
                state.status_line = f"Rig mode set to {mode_name} {int(pb)}"
                state.dirty_status = True
                return True

        state.status_line = f"Rig mode rejected: {last_reply or 'no reply'}"
        state.dirty_status = True
        return False
    except Exception as e:
        state.status_line = f"Rig mode set failed: {e}"
        state.dirty_status = True
        return False



def tune_rig_to_selected_dx(cfg: AppConfig, state: AppState) -> bool:
    with state.dx_lock:
        idx = getattr(state, "dx_selected_idx", -1)
        spots = list(getattr(state, "dx_spots", []))
    if idx < 0 or idx >= len(spots):
        state.status_line = "No DX spot selected"
        state.dirty_status = True
        return False

    spot = spots[idx]
    freq_hz = _dx_frequency_to_hz(spot.get("frequency", ""))
    if freq_hz is None:
        state.status_line = "Selected DX spot has no valid frequency"
        state.dirty_status = True
        return False

    raw_text = spot.get("raw_text", "")
    raw_mode = str(spot.get("mode", "") or "").strip()
    if not raw_mode:
        raw_mode = extract_dx_mode(str(spot.get("comment", "") or ""))
    if not raw_mode and raw_text:
        parsed_live = parse_dx_cluster_spot(raw_text)
        if parsed_live:
            raw_mode = str(parsed_live.get("mode", "") or parsed_live.get("comment", "") or "").strip()
    rig_mode, rig_passband = _normalize_dx_mode_for_rig(raw_mode, freq_hz)

    if raw_text:
        update_dedx_panel_from_spot(cfg, state, raw_text)

    freq_ok = set_rigctld_frequency(cfg, state, freq_hz)
    mode_ok = set_rigctld_mode(cfg, state, rig_mode, rig_passband)

    if freq_ok and mode_ok:
        state.status_line = f"Tuned {freq_hz/1_000_000.0:.6f} MHz {rig_mode}"
        state.dirty_status = True
        return True
    if freq_ok:
        state.status_line = f"Tuned frequency, but mode set failed ({rig_mode})"
        state.dirty_status = True
        return False
    return False


def draw_dx_cluster_panel(win, state: AppState) -> None:
    try:
        h, w = win.getmaxyx()
    except Exception:
        return

    inner_h = max(1, h - 2)
    inner_w = max(1, w - 2)

    win.erase()
    win.box()
    safe_addstr(win, 0, 2, " DX Cluster ", curses.A_BOLD)

    with state.dx_lock:
        spots = list(getattr(state, "dx_spots", []))
        selected_idx = getattr(state, "dx_selected_idx", -1)

    if not spots:
        safe_addstr(win, 1, 1, "Waiting for DX cluster spot..."[:inner_w])
        win.noutrefresh()
        return

    if selected_idx < 0:
        selected_idx = len(spots) - 1
    selected_idx = max(0, min(selected_idx, len(spots) - 1))

    start = max(0, selected_idx - inner_h + 1)
    end = min(len(spots), start + inner_h)

    for row, spot in enumerate(spots[start:end], start=1):
        absolute_idx = start + row - 1
        style = curses.A_REVERSE if absolute_idx == selected_idx else curses.A_NORMAL
        safe_addstr(win, row, 1, str(spot.get("display", ""))[:inner_w], style)

    win.noutrefresh()


def build_rig_lines(state: AppState, width: int, height: int) -> List[str]:
    lines = [
        f"Freq: {getattr(state, 'rig_frequency', '14.074.00')}",
        f"Mode: {getattr(state, 'rig_mode', 'USB')}",
    ]

    usable_h = max(1, height)
    if len(lines) < usable_h:
        lines.extend([""] * (usable_h - len(lines)))
    return [line[:max(1, width)] for line in lines[:usable_h]]


def update_dedx_panel_from_spot(cfg: AppConfig, state: AppState, raw_text: str) -> None:
    spot = parse_dx_cluster_spot(raw_text)
    if not spot:
        return

    de_info = get_cached_callsign_info(cfg, state, spot["spotter"])
    dx_info = get_cached_callsign_info(cfg, state, spot["spotted"])

    lines = []

    for heading, info in (("DE", de_info), ("DX", dx_info)):
        lines.extend([
            "",
            f"{heading}",
            _dedx_field("Call", info.get("callsign", ""), 999),
            _dedx_field("Name", info.get("name", ""), 999),
            _dedx_field("City", info.get("city", ""), 999),
            _dedx_field("Prov/St", info.get("prov_state", ""), 999),
            _dedx_field("Country", info.get("country", ""), 999),
            _dedx_field("DB", info.get("source", ""), 999),
        ])

    state.dedx_lines = lines
    state.dirty_dedx = True


def dx_cluster_worker(cfg: AppConfig, state: AppState):

    while state.running:
        try:
            state.dx_cluster_ready = False
            state.dx_status_line = f"DX: connecting {cfg.dx_host}:{cfg.dx_port}..."
            state.dirty_status = True
            with socket.create_connection((cfg.dx_host, cfg.dx_port), timeout=10) as s:
                s.settimeout(2.0)

                t0 = time.time()
                while time.time() - t0 < 2.0:
                    try:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                    except socket.timeout:
                        continue

                if str(cfg.dx_user).strip():
                    s.sendall((str(cfg.dx_user).strip() + "\n").encode("utf-8", errors="ignore"))
                    time.sleep(0.2)
                if str(cfg.dx_pass).strip():
                    s.sendall((str(cfg.dx_pass).strip() + "\n").encode("utf-8", errors="ignore"))
                    time.sleep(0.3)

                if cfg.dx_filter.strip():
                    s.sendall((cfg.dx_filter.strip() + "\n").encode("utf-8", errors="ignore"))
                    time.sleep(0.2)

                state.dx_cluster_ready = True
                state.dx_status_line = f"DX: connected {cfg.dx_host}:{cfg.dx_port}"
                state.dirty_status = True
                s.settimeout(0.5)

                linebuf = b""
                while state.running:
                    try:
                        while True:
                            try:
                                cmd = state.dx_cmd_queue.get_nowait()
                            except queue.Empty:
                                break
                            s.sendall((cmd.strip() + "\n").encode("utf-8", errors="ignore"))
                            time.sleep(0.15)

                        chunk = s.recv(4096)
                        if not chunk:
                            raise ConnectionError("DX cluster disconnected")

                        linebuf += chunk
                        while b"\n" in linebuf:
                            raw, linebuf = linebuf.split(b"\n", 1)
                            raw_text = raw.decode("utf-8", errors="ignore").rstrip("\r")

                            display_text = format_dx_spot(raw_text)
                            if display_text.strip():
                                with state.dx_lock:
                                    state.dx_lines.append(display_text)
                                    state.dx_lines = state.dx_lines[-500:]

                            if raw_text.lstrip().upper().startswith("DX DE "):
                                status_spot = " ".join((raw_text or "").split())
                                state.dx_status_line = f"DX: connected {cfg.dx_host}:{cfg.dx_port} | {status_spot}"
                                state.dirty_status = True

                                parsed = parse_dx_cluster_spot(raw_text)
                                if parsed:
                                    spot_record = dict(parsed)
                                    spot_record["raw_text"] = raw_text
                                    spot_record["display"] = display_text

                                    with state.dx_lock:
                                        state.dx_spots.append(spot_record)
                                        state.dx_spots = state.dx_spots[-500:]
                                        if state.dx_auto_follow or state.dx_selected_idx < 0:
                                            state.dx_selected_idx = len(state.dx_spots) - 1

                                    update_dedx_panel_from_spot(cfg, state, raw_text)
                                    state.dirty_dedx = True

                            state.dirty_dx = True
                    except socket.timeout:
                        continue
        except Exception as e:
            state.dx_cluster_ready = False
            state.dx_status_line = f"DX: error {e} (retrying in 5s)"
            state.dirty_status = True
            time.sleep(5)


ASCII_RAMP = " .:-=+*#%@"

def image_to_ascii_lines(path: str, width: int, height: int) -> List[str]:
    if Image is None:
        return ["Pillow not installed; cannot render map."]

    try:
        img = Image.open(path).convert("L")
    except Exception as e:
        return [f"Map load error: {e}"]

    aspect_fix = 0.5
    target_w = max(10, width)
    target_h = max(5, int(height / aspect_fix))

    img = img.resize((target_w, target_h))
    px = img.getdata()

    lines = []
    for y in range(target_h):
        row = []
        for x in range(target_w):
            v = px[y * target_w + x]
            idx = int((v / 255) * (len(ASCII_RAMP) - 1))
            row.append(ASCII_RAMP[idx])
        lines.append("".join(row))

    if len(lines) > height:
        step = len(lines) / height
        sampled = []
        for i in range(height):
            sampled.append(lines[int(i * step)])
        lines = sampled

    return lines[:height]


def _clear_rect(stdscr, y: int, x: int, h: int, w: int):
    for yy in range(y, y + h):
        safe_addstr(stdscr, yy, x, " " * max(0, w))


def build_dedx_placeholder_lines(width: int, height: int) -> List[str]:
    lines = [
        "Waiting for DX cluster spot...",
        "",
        "DE = spotter",
        "DX = spotted",
    ]
    usable_h = max(1, height)
    if len(lines) < usable_h:
        lines.extend([""] * (usable_h - len(lines)))
    return [line[: max(1, width)] for line in lines[:usable_h]]




def _simple_message_popup(stdscr, title: str, message: str):
    lines = [line for line in str(message).splitlines()] or [""]
    maxy, maxx = stdscr.getmaxyx()
    inner_w = min(maxx - 6, max(40, min(100, max((len(title) + 4), *(len(line) + 4 for line in lines)))))
    h = min(maxy - 4, max(6, len(lines) + 4))
    y0 = max(1, (maxy - h) // 2)
    x0 = max(1, (maxx - inner_w) // 2)
    win = curses.newwin(h, inner_w, y0, x0)
    win.keypad(True)
    win.erase()
    win.box()
    safe_addstr(win, 0, max(2, (inner_w - len(title) - 2) // 2), f" {title} ", curses.A_BOLD)
    for i, line in enumerate(lines[: max(1, h - 3)]):
        safe_addstr(win, 1 + i, 2, line[: max(1, inner_w - 4)])
    safe_addstr(win, h - 2, 2, "Press any key to continue.", curses.A_DIM)
    win.refresh()
    win.getch()
    try:
        win.erase()
        win.noutrefresh()
    except Exception:
        pass


def show_help_screen(stdscr, state: AppState):
    """Show keyboard help. Close with F1 or Esc."""
    lines = [
        "F1 = Close help",
        "Esc = Close help",
        "",
        "Main screen",
        "  F1 = Show this help screen",
        "  Esc = Open the File menu",
        "  L = Toggle Logbook mode on/off",
        "  Q = Quit the program",
        "",
        "Logbook mode",
        "  N = New QSO entry",
        "  C = Manage/select station callsigns",
        "  E = Edit selected QSO",
        "  D = Delete selected QSO",
        "  Up/Down = Select recent QSO",
        "",
        "File menu",
        "  Up/Down = Move through menu items",
        "  Enter = Open selected item",
        "  Esc = Close the File menu",
        "",
        "QSO entry form",
        "  Tab = Next field",
        "  Shift+Tab = Previous field",
        "  Up/Down = Move between fields",
        "  Enter or F2 = Save",
        "  Esc = Cancel",
        "  F5 = Callsign lookup",
        "  Backspace = Delete previous character",
        "  Ctrl+U = Clear current field",
        "",
        "Dialogs",
        "  Enter = Select/confirm current item",
        "  Esc = Cancel/close current dialog",
        "",  
        "Callsign lookup source names",  
        "  qrz.com = live online QRZ lookup",  
        "  qrz = the same QRZ source from the local cache/database,",  
        "        where provider names are stored using short internal keys",  
        "  hamqth.com and hamdb.org may also appear in shortened cached",  
        "        form as hamqth and hamdb",  
        "  These labels refer to the same provider, just different",  
        "        lookup paths (live lookup vs cached result)",  
    ]

    maxy, maxx = stdscr.getmaxyx()
    inner_w = min(maxx - 4, max(56, min(100, max(len(line) for line in lines) + 4)))
    h = min(maxy - 2, max(12, len(lines) + 4))
    y0 = max(0, (maxy - h) // 2)
    x0 = max(0, (maxx - inner_w) // 2)

    win = curses.newwin(h, inner_w, y0, x0)
    win.keypad(True)

    top = 0
    visible_lines = max(1, h - 2)

    while True:
        win.erase()
        win.box()
        title = " Help (F1/Esc=Close, Up/Down=Scroll) "
        safe_addstr(win, 0, max(2, (inner_w - len(title)) // 2), title[: max(1, inner_w - 4)], curses.A_BOLD)

        for row in range(visible_lines):
            idx = top + row
            if idx >= len(lines):
                break
            safe_addstr(win, 1 + row, 2, lines[idx][: max(1, inner_w - 4)])

        win.refresh()
        k = win.getch()

        if k in (curses.KEY_F1, 27):
            break
        elif k == curses.KEY_UP:
            top = max(0, top - 1)
        elif k == curses.KEY_DOWN:
            top = min(max(0, len(lines) - visible_lines), top + 1)
        elif k == curses.KEY_PPAGE:
            top = max(0, top - visible_lines)
        elif k == curses.KEY_NPAGE:
            top = min(max(0, len(lines) - visible_lines), top + visible_lines)

    try:
        win.erase()
        win.noutrefresh()
    except Exception:
        pass

    force_full_redraw(stdscr, state)
    curses.doupdate()

def edit_mysql_settings_dialog(stdscr, cfg: AppConfig, state: AppState):
    """Curses popup for MySQL/MariaDB settings."""
    fields = [
        ("Use MySQL/MariaDB", "mysql_enabled"),
        ("MySQL Host/IP", "mysql_host"),
        ("MySQL Port", "mysql_port"),
        ("MySQL Username", "mysql_username"),
        ("MySQL Password", "mysql_password"),
        ("MySQL Database", "mysql_database"),
    ]

    values = {}
    for _, attr in fields:
        v = getattr(cfg, attr, "")
        if attr == "mysql_enabled":
            values[attr] = "On" if bool(v) else "Off"
        else:
            values[attr] = "" if v is None else str(v)

    idx = 0
    curses.curs_set(1)
    stdscr.nodelay(False)

    def close_dialog(win, y0, x0, h, w):
        try:
            win.erase()
            win.noutrefresh()
        except Exception:
            pass
        try:
            _clear_rect(stdscr, y0, x0, h, w)
        except Exception:
            pass
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_status = True

    while True:
        maxy, maxx = stdscr.getmaxyx()
        h = min(maxy - 4, len(fields) + 8)
        w = min(maxx - 4, max(64, min(100, maxx - 4)))
        y0 = (maxy - h) // 2
        x0 = (maxx - w) // 2
        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        title = " MySQL/MariaDB Settings (Enter=edit/toggle, F2=Save, F5=Test, ESC=Cancel) "
        safe_addstr(win, 0, max(2, (w - len(title)) // 2), title[: max(1, w - 4)], curses.A_BOLD)

        top = max(0, idx - (h - 7))
        visible = fields[top: top + (h - 6)]

        for row, (label, attr) in enumerate(visible):
            y = 2 + row
            is_sel = (top + row) == idx
            attr_style = curses.A_REVERSE if is_sel else curses.A_NORMAL
            val = values.get(attr, "")
            if attr == "mysql_password" and val:
                disp = "*" * len(val)
            else:
                disp = val
            safe_addstr(win, y, 2, f"{label}:"[: w - 4], attr_style)
            col = min(w - 4, 28)
            safe_addstr(win, y, col, disp[: max(0, w - col - 2)], attr_style)

        safe_addstr(win, h - 3, 2, "Toggle MySQL On/Off here. Saved to hamclock_settings.json.", curses.A_DIM)
        safe_addstr(win, h - 2, 2, "F5 tests connection with current values.", curses.A_DIM)
        win.refresh()

        k = win.getch()
        if k in (27,):
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_UP,):
            idx = max(0, idx - 1)
        elif k in (curses.KEY_DOWN,):
            idx = min(len(fields) - 1, idx + 1)
        elif k in (curses.KEY_F5,):
            test_cfg = cfg
            try:
                test_cfg.mysql_enabled = str(values.get("mysql_enabled", "Off")).strip().lower() == "on"
                test_cfg.mysql_host = values.get("mysql_host", "")
                test_cfg.mysql_port = int(str(values.get("mysql_port", "3306") or "3306"))
                test_cfg.mysql_username = values.get("mysql_username", "")
                test_cfg.mysql_password = values.get("mysql_password", "")
                test_cfg.mysql_database = values.get("mysql_database", "")
            except Exception:
                pass
            ok, msg = _test_mysql_connection_cfg(test_cfg)
            _simple_message_popup(stdscr, "MySQL Connection Test", ("SUCCESS: " if ok else "FAILED: ") + str(msg))
        elif k in (curses.KEY_F2,):
            try:
                cfg.mysql_enabled = str(values.get("mysql_enabled", "Off")).strip().lower() == "on"
                cfg.mysql_host = values.get("mysql_host", "")
                cfg.mysql_port = int(str(values.get("mysql_port", "3306") or "3306"))
                cfg.mysql_username = values.get("mysql_username", "")
                cfg.mysql_password = values.get("mysql_password", "")
                cfg.mysql_database = values.get("mysql_database", "")
                save_config(cfg)
                state.status_line = f"MySQL settings saved to {CONFIG_FILE}"
            except Exception as e:
                state.status_line = f"MySQL settings save failed: {e}"
            state.dirty_status = True
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            if attr == "mysql_enabled":
                values[attr] = "Off" if str(values.get(attr, "On")).strip().lower() == "on" else "On"
                continue
            prompt = f"{label}: "
            inp = values.get(attr, "")
            win2 = curses.newwin(3, w, y0 + h - 4, x0)
            win2.erase()
            win2.box()
            safe_addstr(win2, 1, 2, prompt[: max(1, w - 4)])
            shown = "*" * len(str(inp)) if attr == "mysql_password" and inp else str(inp)
            safe_addstr(win2, 1, min(w - 3, 2 + len(prompt)), shown[: max(0, w - (4 + len(prompt)))])
            win2.refresh()
            curses.echo()
            try:
                s = win2.getstr(1, 2 + len(prompt), max(1, w - (4 + len(prompt)))).decode("utf-8", errors="ignore")
                if s is not None:
                    values[attr] = s
            except Exception:
                pass
            finally:
                curses.noecho()

    curses.curs_set(0)
    stdscr.timeout(100)

def edit_settings_dialog(stdscr, cfg: AppConfig, state: AppState):
    fields = [
        ("World Map Refresh (sec)", "world_map_refresh_sec"),
        ("Space Weather Refresh (sec)", "space_weather_refresh_sec"),
        ("Latitude (Decimal)", "latitude_decimal"),
        ("Longitude (Decimal)", "longitude_decimal"),
        ("Grid Square", "grid_square"),
        ("Local Weather Refresh (sec)", "local_weather_refresh_sec"),
        ("Time Zone", "time_zone"),
    ]

    normalize_config(cfg)
    if not getattr(cfg, "time_zone", ""):
        cfg.time_zone = "America/Edmonton"

    values = {}
    for _, attr in fields:
        v = getattr(cfg, attr, "")
        values[attr] = "" if v is None else str(v)

    idx = 0
    curses.curs_set(1)
    stdscr.nodelay(False)

    while True:
        maxy, maxx = stdscr.getmaxyx()
        h = min(maxy - 4, len(fields) + 6)
        w = min(maxx - 4, max(60, min(100, maxx - 4)))
        y0 = (maxy - h) // 2
        x0 = (maxx - w) // 2
        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        title = " Settings (Enter=edit, F2=Save, ESC=Cancel) "
        safe_addstr(win, 0, max(2, (w - len(title)) // 2), title, curses.A_BOLD)

        for row, (label, attr) in enumerate(fields):
            y = 2 + row
            is_sel = row == idx
            style = curses.A_REVERSE if is_sel else curses.A_NORMAL
            val = values.get(attr, "")
            safe_addstr(win, y, 2, f"{label}:"[: w - 4], style)
            safe_addstr(win, y, 32, val[: max(1, w - 34)], style)

        safe_addstr(win, h - 2, 2, "Up/Down select. Enter edits. F2 saves and applies to the running app.", curses.A_DIM)
        win.refresh()

        k = win.getch()
        if k in (27,):
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k == curses.KEY_F2:
            try:
                cfg.world_map_refresh_sec = float(values.get("world_map_refresh_sec", "300") or "300")
                cfg.space_weather_refresh_sec = float(values.get("space_weather_refresh_sec", "1800") or "1800")
                cfg.latitude_decimal = None if str(values.get("latitude_decimal", "")).strip() == "" else float(values.get("latitude_decimal"))
                cfg.longitude_decimal = None if str(values.get("longitude_decimal", "")).strip() == "" else float(values.get("longitude_decimal"))
                cfg.grid_square = values.get("grid_square", "").strip()
                cfg.local_weather_refresh_sec = float(values.get("local_weather_refresh_sec", "1800") or "1800")
                cfg.time_zone = values.get("time_zone", "").strip() or "America/Edmonton"
                save_config(cfg)
                apply_config_runtime(cfg, state)
                state.status_line = "Settings saved and applied"
            except Exception as e:
                state.status_line = f"Settings save failed: {e}"
            state.dirty_status = True
            curses.curs_set(0)
            stdscr.timeout(100)
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            s = _input_box(stdscr, y0 + h - 3, x0, w, f"{label}: ", values.get(attr, ""))
            if s is not None:
                values[attr] = s

def edit_online_lookup_dialog(stdscr, cfg: AppConfig, state: AppState):
    values = {
        "online_lookup_website": str(getattr(cfg, "online_lookup_website", "hamdb.org") or "hamdb.org"),
        "online_lookup_username": str(getattr(cfg, "online_lookup_username", "") or ""),
        "online_lookup_password": str(getattr(cfg, "online_lookup_password", "") or ""),
    }
    old_provider = _get_selected_lookup_provider(cfg)
    options = ["hamdb.org", "hamqth.com", "qrz.com"]
    idx = 0
    curses.curs_set(1)
    stdscr.nodelay(False)

    def close_dialog(win, y0, x0, h, w):
        try:
            win.erase()
            win.noutrefresh()
        except Exception:
            pass
        try:
            _clear_rect(stdscr, y0, x0, h, w)
        except Exception:
            pass
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_status = True

    while True:
        provider = str(values.get("online_lookup_website", "hamdb.org") or "hamdb.org")
        fields = [("Website", "online_lookup_website")]
        if provider in ("hamqth.com", "qrz.com"):
            fields.extend([
                ("Username", "online_lookup_username"),
                ("Password", "online_lookup_password"),
            ])

        maxy, maxx = stdscr.getmaxyx()
        w = min(76, max(44, maxx - 4))
        h = len(fields) + 7
        y0 = max(1, (maxy - h) // 2)
        x0 = max(0, (maxx - w) // 2)

        win = curses.newwin(h, w, y0, x0)
        win.keypad(True)
        win.erase()
        win.box()
        title = "Online Lookup"
        safe_addstr(win, 0, max(2, (w - len(title)) // 2), title, curses.A_BOLD)

        for row, (label, attr) in enumerate(fields):
            y = 2 + row
            is_sel = row == idx
            attr_style = curses.A_REVERSE if is_sel else curses.A_NORMAL
            val = values.get(attr, "")
            if attr == "online_lookup_password" and val:
                val = "*" * len(val)
            safe_addstr(win, y, 2, f"{label}:"[:w-4], attr_style)
            safe_addstr(win, y, 18, val[: max(1, w - 20)], attr_style)

        safe_addstr(win, 6, 2, "Website cycles with Left/Right or Enter on Website.", curses.A_DIM)
        safe_addstr(win, 7, 2, "hamdb.org uses no credentials; hamqth.com and qrz.com do.", curses.A_DIM)
        safe_addstr(win, 8, 2, "F2 saves   ESC closes", curses.A_DIM)
        win.refresh()

        k = win.getch()
        if k in (27,):
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k == curses.KEY_UP:
            idx = max(0, idx - 1)
        elif k == curses.KEY_DOWN:
            idx = min(len(fields) - 1, idx + 1)
        elif k in (curses.KEY_LEFT, curses.KEY_RIGHT) and idx == 0:
            cur = values["online_lookup_website"]
            try:
                pos = options.index(cur)
            except ValueError:
                pos = 0
            pos = (pos + (1 if k == curses.KEY_RIGHT else -1)) % len(options)
            values["online_lookup_website"] = options[pos]
        elif k == curses.KEY_F2:
            for _, attr in fields:
                setattr(cfg, attr, values.get(attr, ""))
            try:
                save_config(cfg)
                new_provider = _get_selected_lookup_provider(cfg)
                _clear_hamqth_cached_session()
                _clear_qrz_cached_session()
                with state.dedx_lookup_lock:
                    state.dedx_lookup_cache.clear()
                with state.online_lookup_lock:
                    state.online_lookup_history.clear()
                initialize_online_lookup_session(cfg, state)
                if new_provider != old_provider:
                    state.status_line = f"Online lookup set to {new_provider}"
                else:
                    state.status_line = "Online lookup settings saved"
            except Exception as e:
                state.status_line = f"Online lookup save failed: {e}"
            state.dirty_status = True
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            if attr == "online_lookup_website":
                cur = values["online_lookup_website"]
                try:
                    pos = options.index(cur)
                except ValueError:
                    pos = 0
                values["online_lookup_website"] = options[(pos + 1) % len(options)]
                continue

            prompt = f"{label}: "
            win2 = curses.newwin(3, w, y0 + h - 4, x0)
            win2.erase()
            win2.box()
            safe_addstr(win2, 1, 2, prompt)
            win2.refresh()
            curses.echo()
            try:
                s = win2.getstr(1, 2 + len(prompt), w - (4 + len(prompt))).decode("utf-8", errors="ignore")
                if s is not None:
                    values[attr] = s
            except Exception:
                pass
            finally:
                curses.noecho()




def callsign_lookup_dialog(stdscr, cfg: AppConfig, state: AppState):
    """Prompt for a callsign, then display lookup results in a modal overlay until ESC."""
    maxy, maxx = stdscr.getmaxyx()
    prompt = "Callsign: "
    w = min(60, max(34, maxx - 4))
    h = 5
    y0 = max(1, (maxy - h) // 2)
    x0 = max(0, (maxx - w) // 2)

    win = curses.newwin(h, w, y0, x0)
    win.keypad(True)
    win.erase()
    win.box()
    title = " Callsign Lookup "
    safe_addstr(win, 0, max(2, (w - len(title)) // 2), title, curses.A_BOLD)
    safe_addstr(win, 2, 2, prompt)
    win.refresh()

    curses.curs_set(1)
    stdscr.nodelay(False)
    curses.echo()
    entered = ""
    try:
        raw = win.getstr(2, 2 + len(prompt), max(1, w - (4 + len(prompt))))
        if raw is None:
            return
        entered = raw.decode("utf-8", errors="ignore").strip()
    except Exception:
        entered = ""
    finally:
        curses.noecho()
        curses.curs_set(0)
        stdscr.timeout(100)

    try:
        win.erase()
        win.noutrefresh()
    except Exception:
        pass
    try:
        _clear_rect(stdscr, y0, x0, h, w)
    except Exception:
        pass

    callsign = _normalize_callsign(entered)
    if not callsign:
        state.status_line = "Callsign lookup cancelled"
        state.dirty_status = True
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        return

    info = get_cached_callsign_info(cfg, state, callsign)
    city = _clean_lookup_value(info.get("city"))
    prov = _clean_lookup_value(info.get("prov_state"))
    country = _clean_lookup_value(info.get("country"))
    source = _clean_lookup_value(info.get("source")) or "not found"

    location_parts = [p for p in (city, prov, country) if p]
    location = ", ".join(location_parts) if location_parts else "-"

    lines = [
        f"Callsign: {callsign}",
        f"Name: {_clean_lookup_value(info.get('name')) or '-'}",
        f"Location: {location}",
        f"Source: {source}",
        "",
        "Esc = close",
    ]

    box_w = min(72, max(40, max(len(line) for line in lines) + 4, min(maxx - 4, 40)))
    box_h = len(lines) + 2
    box_y = max(1, (maxy - box_h) // 2)
    box_x = max(0, (maxx - box_w) // 2)

    panel = curses.newwin(box_h, box_w, box_y, box_x)
    panel.keypad(True)
    while True:
        panel.erase()
        panel.box()
        safe_addstr(panel, 0, 2, " Callsign Lookup Result ", curses.A_BOLD)
        for i, line in enumerate(lines, start=1):
            safe_addstr(panel, i, 2, line[: max(1, box_w - 4)])
        panel.refresh()

        k = panel.getch()
        if k in (27, ord('q'), ord('Q'), curses.KEY_ENTER, 10, 13):
            break

    try:
        panel.erase()
        panel.noutrefresh()
    except Exception:
        pass
    try:
        _clear_rect(stdscr, box_y, box_x, box_h, box_w)
    except Exception:
        pass

    state.status_line = f"Lookup {callsign}: {source}"
    state.dirty_status = True
    state.dirty_space = True
    state.dirty_dx = True
    state.dirty_wx_static = True
    state.dirty_wx_time = True
    state.dirty_rig = True
    state.dirty_dedx = True
    state.dirty_map = True


def dx_command_dialog(stdscr, state: AppState):
    """Prompt user for a DX cluster command and queue it for sending."""
    maxy, maxx = stdscr.getmaxyx()
    w = min(80, max(40, maxx - 4))
    h = 5
    y0 = max(1, (maxy - h) // 2)
    x0 = max(0, (maxx - w) // 2)

    win = curses.newwin(h, w, y0, x0)
    win.erase()
    win.box()
    title = "DX Cluster Command"
    safe_addstr(win, 0, max(2, (w - len(title)) // 2), title, curses.A_BOLD)
    prompt = "Command: "
    safe_addstr(win, 2, 2, prompt)
    safe_addstr(win, 3, 2, "Enter=Send   ESC=Cancel")
    win.refresh()

    curses.curs_set(1)
    curses.echo()
    try:
        # Read input
        s = win.getstr(2, 2 + len(prompt), w - (4 + len(prompt))).decode("utf-8", errors="ignore")
        if s is None:
            return
        cmd = s.strip()
        if cmd:
            state.dx_cmd_queue.put(cmd)
            state.status_line = f"DX cmd queued: {cmd}"
            state.dirty_status = True
    except Exception:
        # ignore
        pass
    finally:
        curses.noecho()
        curses.curs_set(0)
        stdscr.timeout(100)

def draw_box_contents(win, lines: List[str], title: Optional[str] = None):
    # Redraw border/title too, so panels recover cleanly after overlays (menu) are erased
    win.erase()
    win.box()
    if title:
        safe_addstr(win, 0, 2, f" {title} ")
    clear_interior(win)
    h, w = win.getmaxyx()
    usable_h = h - 2
    for i in range(min(usable_h, len(lines))):
        safe_addstr(win, 1 + i, 1, lines[i][: w - 2])
    win.noutrefresh()


def build_rig_lines(state: AppState, width: int, height: int) -> List[str]:
    freq = str(getattr(state, "rig_frequency", "-") or "-")
    mode = str(getattr(state, "rig_mode", "-") or "-")
    comp = bool(getattr(state, "rig_comp", False))
    preamp = str(getattr(state, "rig_preamp", "Off") or "Off")
    agc = str(getattr(state, "rig_agc", "-") or "-")
    nb = bool(getattr(state, "rig_nb", False))
    nr = bool(getattr(state, "rig_nr", False))

    line1 = f"Freq: {freq}"
    line2 = f"Mode: {mode}"
    line3 = f"Comp: {'COMP' if comp else 'comp'}"
    line4 = f"Pre-Amp: {preamp}"
    line5 = f"AGC: {agc}"
    line6 = f"NB: {'NB' if nb else 'nb'}    NR: {'NR' if nr else 'nr'}"

    lines = [line1, line2, line3, line4, line5, line6]
    while len(lines) < height:
        lines.append("")
    return [ln[:width] for ln in lines[:height]]



def draw_status(stdscr, state: AppState, maxy: int, maxx: int):
    lookup_msg = (getattr(state, "online_lookup_status_line", "") or "").strip() or "Lookup: idle"
    radio_msg = (getattr(state, "rig_status_line", "") or "").strip() or "Radio: disabled"
    dx_msg = (getattr(state, "dx_status_line", "") or "").strip() or "DX: disconnected"
    bar_w = max(0, maxx - 1)
    if bar_w <= 0:
        return
    if maxy >= 2:
        safe_addstr(stdscr, maxy - 2, 0, " " * bar_w, curses.A_REVERSE)
        top_msg = f"{lookup_msg} | {radio_msg}"
        safe_addstr(stdscr, maxy - 2, 0, top_msg[:bar_w].ljust(bar_w), curses.A_REVERSE)
    safe_addstr(stdscr, maxy - 1, 0, " " * bar_w, curses.A_REVERSE)
    safe_addstr(stdscr, maxy - 1, 0, dx_msg[:bar_w].ljust(bar_w), curses.A_REVERSE)



def force_full_redraw(stdscr, state: AppState):
    """Fully repaint the base screen after overlays/dialogs are closed."""
    try:
        stdscr.touchwin()
        stdscr.erase()
        stdscr.noutrefresh()
    except Exception:
        pass
    state.dirty_space = True
    state.dirty_dx = True
    state.dirty_wx_static = True
    state.dirty_wx_time = True
    state.dirty_rig = True
    state.dirty_dedx = True
    state.dirty_map = True
    state.dirty_status = True



def _dx_move_selection(state: AppState, delta: int) -> bool:
    try:
        delta = int(delta)
    except Exception:
        delta = 0
    if delta == 0:
        return False
    with state.dx_lock:
        spots = list(getattr(state, "dx_spots", []) or [])
        if not spots:
            state.dx_selected_idx = -1
            return False
        idx = int(getattr(state, "dx_selected_idx", len(spots) - 1) or (len(spots) - 1))
        if idx < 0:
            idx = len(spots) - 1
        idx = max(0, min(len(spots) - 1, idx + delta))
        state.dx_selected_idx = idx
        state.dx_auto_follow = (idx >= len(spots) - 1)
    state.dirty_dx = True
    return True


def _is_dx_up_key(k: int) -> bool:
    return k in (
        curses.KEY_UP,
        ord('8'),
        ord('k'),
        ord('K'),
    )


def _is_dx_down_key(k: int) -> bool:
    return k in (
        curses.KEY_DOWN,
        ord('2'),
        ord('j'),
        ord('J'),
    )


def _is_dx_enter_key(k: int) -> bool:
    return k in (
        curses.KEY_ENTER,
        10,
        13,
    )


def main(stdscr):
    config_exists = os.path.exists(CONFIG_FILE)
    cfg = load_config()
    set_logging_enabled(getattr(cfg, "enable_logging", True))
    state = AppState()
    state.rig_frequency = "-"
    state.rig_mode = "-"
    state.rig_status_line = "Radio: disabled"
    state.dirty_rig = True
    curses.curs_set(0)
    stdscr.keypad(True)
    stdscr.timeout(100)  # 0.1s polling

    # If this is the first run (no settings JSON yet), immediately prompt for Settings.
    if not config_exists:
        edit_settings_dialog(stdscr, cfg, state)
        # Reload from disk to ensure type conversions are applied consistently.
        cfg = load_config()

    initialize_online_lookup_session(cfg, state)

    # Enable curses color support (used for ANSI->curses map rendering)
    if curses.has_colors():
        curses.start_color()
        try:
            curses.use_default_colors()
        except Exception:
            pass
        try:
            curses.init_pair(20, curses.COLOR_WHITE, -1)
            curses.init_pair(21, curses.COLOR_BLACK, -1)
        except Exception:
            pass


    # Layout: compute once, but also handle resize by rebuilding windows
    def build_windows():
        maxy, maxx = stdscr.getmaxyx()
        top_box_height = 8
        top_y = 0

        col_w = maxx // 3
        widths = [col_w, col_w, maxx - 2 * col_w]

        # Create three top windows
        x = 0
        w_space = curses.newwin(top_box_height, widths[0], top_y, x)
        box_title(w_space, "Space Weather")
        x += widths[0]

        w_dx = curses.newwin(top_box_height, widths[1], top_y, x)
        box_title(w_dx, "DX Cluster")
        x += widths[1]

        w_wx = curses.newwin(top_box_height, widths[2], top_y, x)
        box_title(w_wx, "Local Info")

        map_start_y = top_y + top_box_height
        map_h = max(3, (maxy - 3) - map_start_y)

        dedx_w = max(20, maxx // 3)
        map_w = max(20, maxx - dedx_w)

        rig_h = min(6, map_h)
        dedx_h = max(3, map_h - rig_h)

        w_rig = curses.newwin(rig_h, dedx_w, map_start_y, 0)
        box_title(w_rig, "Radio")

        w_dedx = curses.newwin(dedx_h, dedx_w, map_start_y + rig_h, 0)
        box_title(w_dedx, "DE / DX")

        w_map = curses.newwin(map_h, map_w, map_start_y, dedx_w)
        box_title(w_map, "World Map")

        # Force redraw everything in new windows
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_rig = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_status = True

        return w_space, w_dx, w_wx, w_rig, w_dedx, w_map

    w_space, w_dx, w_wx, w_rig, w_dedx, w_map = build_windows()

    def _space_inner_w():
        try:
            return max(0, w_space.getmaxyx()[1] - 2)
        except Exception:
            return 0

    space_inner_w = _space_inner_w()

    # Start DX thread
    t = threading.Thread(target=dx_cluster_worker, args=(cfg, state), daemon=True)
    t.start()

    # Initial data
    update_space_weather(state, panel_inner_w=space_inner_w)
    update_weather_open_meteo(cfg, state)
    update_time_lines(state)

    last_refresh = 0.0
    last_wx_refresh = 0.0
    last_map_refresh = 0.0
    last_time_refresh = 0.0
    last_rig_refresh = 0.0

    while state.running:
        now = time.time()

        # DX spot map overlay disabled: leave base world map rendering unchanged.

        # periodic updates (paused when menu is open)
        if (now - last_refresh) >= cfg.refresh_seconds:
            update_space_weather(state, panel_inner_w=space_inner_w)
            if (now - last_wx_refresh) >= cfg.local_weather_refresh_sec:
                update_weather_open_meteo(cfg, state)
                last_wx_refresh = now
            last_refresh = now

        # World map refresh (every cfg.map_refresh_seconds)
        if (not state.logging_mode) and (now - last_map_refresh) >= cfg.map_refresh_seconds:
            state.dirty_map = True
            last_map_refresh = now

        if (now - last_time_refresh) >= cfg.time_refresh_seconds:
            update_time_lines(state)
            last_time_refresh = now

        try:
            rig_poll_sec = max(0.2, float(getattr(cfg, "rigctld_poll_ms", 1000) or 1000) / 1000.0)
        except Exception:
            rig_poll_sec = 1.0
        if (now - last_rig_refresh) >= rig_poll_sec:
            poll_rigctld_once(cfg, state)
            last_rig_refresh = now

        # input
        k = stdscr.getch()
        if k is not None and k != -1:
            if k == curses.KEY_RESIZE:
                stdscr.erase()
                stdscr.noutrefresh()
                w_space, w_dx, w_wx, w_rig, w_dedx, w_map = build_windows()

                space_inner_w = _space_inner_w()

                state.dirty_dx = True
                state.dirty_wx_static = True
                state.dirty_wx_time = True
                state.dirty_rig = True
                state.dirty_dedx = True
                state.dirty_map = True
                state.dirty_logbook = True
                state.dirty_status = True
            elif k == curses.KEY_F1:
                show_help_screen(stdscr, state)
                now = time.time()
                last_refresh = now
                last_wx_refresh = now
                last_map_refresh = now
                last_time_refresh = now
                update_space_weather(state, panel_inner_w=space_inner_w)
                update_weather_open_meteo(cfg, state)
                update_time_lines(state)
            elif k == 27:
                file_menu_dialog(stdscr, cfg, state)
                force_full_redraw(stdscr, state)
                now = time.time()
                last_refresh = now
                last_wx_refresh = now
                last_map_refresh = now
                last_time_refresh = now
                update_space_weather(state, panel_inner_w=space_inner_w)
                update_weather_open_meteo(cfg, state)
                update_time_lines(state)
            elif k in (ord('l'), ord('L')):
                state.logging_mode = not state.logging_mode
                if state.logging_mode:
                    try:
                        ensure_logbook_schema(cfg)
                        select_default_station_callsign(cfg, state)
                        refresh_logbook_lines(cfg, state)
                        state.status_line = f"Logbook mode ON - {_db_target_desc(cfg)}"
                    except Exception as e:
                        state.selected_station_callsign_id = None
                        state.selected_station_callsign = ""
                        state.logbook_lines = [
                            "Logbook mode",
                            "",
                            "Database/schema error:",
                            str(e),
                            "",
                            f"Configured backend: {_db_target_desc(cfg)}",
                            "Press L to return to normal mode.",
                        ]
                        state.status_line = f"Logbook error: {e}"
                        state.dirty_logbook = True
                else:
                    state.status_line = "Logbook mode OFF"
                state.dirty_map = True
                state.dirty_status = True
            elif k in (ord('N'), ord('n')):
                if state.logging_mode:
                    try:
                        ensure_logbook_schema(cfg)
                        if not state.selected_station_callsign_id:
                            callsign_dialog(stdscr, cfg, state)
                            force_full_redraw(stdscr, state)
                        else:
                            qso_entry_dialog(stdscr, cfg, state, None)
                            force_full_redraw(stdscr, state)
                            now = time.time()
                            last_refresh = now
                            last_wx_refresh = now
                            last_map_refresh = now
                            last_time_refresh = now
                            update_space_weather(state, panel_inner_w=space_inner_w)
                            update_weather_open_meteo(cfg, state)
                            update_time_lines(state)
                    except Exception as e:
                        _simple_message_popup(stdscr, "Logbook", str(e))
                        state.status_line = f"Logbook error: {e}"
                        state.dirty_status = True
            elif k in (ord('C'), ord('c')):
                if state.logging_mode:
                    try:
                        ensure_logbook_schema(cfg)
                        callsign_dialog(stdscr, cfg, state)
                        force_full_redraw(stdscr, state)
                    except Exception as e:
                        _simple_message_popup(stdscr, "Callsign", str(e))
                        state.status_line = f"Callsign error: {e}"
                        state.dirty_status = True
            elif k in (ord('E'), ord('e')):
                if state.logging_mode and state.logbook_recent_rows:
                    idx = max(0, min(len(state.logbook_recent_rows) - 1, int(getattr(state, 'logbook_selected_idx', 0) or 0)))
                    qso_id = int(state.logbook_recent_rows[idx]['id'])
                    qso_entry_dialog(stdscr, cfg, state, qso_id)
                    force_full_redraw(stdscr, state)
                    now = time.time()
                    last_refresh = now
                    last_wx_refresh = now
                    last_map_refresh = now
                    last_time_refresh = now
                    update_space_weather(state, panel_inner_w=space_inner_w)
                    update_weather_open_meteo(cfg, state)
                    update_time_lines(state)
            elif k in (ord('D'), ord('d')):
                if state.logging_mode:
                    delete_selected_qso_dialog(stdscr, cfg, state)
            elif _is_dx_up_key(k):
                if state.logging_mode:
                    _move_logbook_selection(state, -1)
                    refresh_logbook_lines(cfg, state)
                else:
                    _dx_move_selection(state, -1)
            elif _is_dx_down_key(k):
                if state.logging_mode:
                    _move_logbook_selection(state, 1)
                    refresh_logbook_lines(cfg, state)
                else:
                    _dx_move_selection(state, 1)
            elif _is_dx_enter_key(k):
                if not state.logging_mode:
                    tune_rig_to_selected_dx(cfg, state)
            elif k in (ord('q'), ord('Q')):
                state.running = False

        # draw ONLY dirty areas
        if state.dirty_space:
            draw_box_contents(w_space, state.space_weather_lines, "Space Weather")
            state.dirty_space = False

        if state.dirty_dx:  
            dx_visible = max(1, w_dx.getmaxyx()[0] - 2)  
            draw_dx_cluster_panel(w_dx, state)  
            state.dirty_dx = False

        if state.dirty_wx_static or state.dirty_wx_time:
            # combine static + time lines into the wx box
            lines = list(state.wx_static_lines) + list(state.wx_time_lines)
            draw_box_contents(w_wx, lines, "Local Info")
            state.dirty_wx_static = False
            state.dirty_wx_time = False

        if state.dirty_rig:
            h_rig, w_rig_inner = w_rig.getmaxyx()
            inner_w = max(1, w_rig_inner - 2)
            inner_h = max(1, h_rig - 2)
            rig_lines = build_rig_lines(state, inner_w, inner_h)
            draw_box_contents(w_rig, rig_lines, "Radio")
            try:
                on_attr = curses.color_pair(20) | curses.A_BOLD
            except Exception:
                on_attr = curses.A_BOLD
            try:
                off_attr = curses.color_pair(21) | curses.A_DIM
            except Exception:
                off_attr = curses.A_DIM

            comp_attr = on_attr if bool(getattr(state, "rig_comp", False)) else off_attr
            nb_attr = on_attr if bool(getattr(state, "rig_nb", False)) else off_attr
            nr_attr = on_attr if bool(getattr(state, "rig_nr", False)) else off_attr

            # Interior starts at row 1, col 1
            if inner_h >= 3:
                safe_addstr(w_rig, 3, 8, "COMP", comp_attr)
            if inner_h >= 6:
                safe_addstr(w_rig, 6, 5, "NB", nb_attr)
                safe_addstr(w_rig, 6, 16, "NR", nr_attr)
            w_rig.noutrefresh()
            state.dirty_rig = False


        if state.dirty_dedx:
            h_dedx, w_dedx_inner = w_dedx.getmaxyx()
            inner_w = max(1, w_dedx_inner - 2)
            inner_h = max(1, h_dedx - 2)
            dedx_lines = build_dedx_lines(state, inner_w, inner_h)
            draw_box_contents(w_dedx, dedx_lines, "DE / DX")
            state.dirty_dedx = False

        if state.logging_mode:
            if state.dirty_map or state.dirty_logbook:
                draw_box_contents(w_map, state.logbook_lines, "Logbook")
                state.dirty_map = False
                state.dirty_logbook = False
        elif state.dirty_map:
            # Redraw border/title too so the map panel is fully restored after overlays.
            w_map.erase()
            w_map.box()
            safe_addstr(w_map, 0, 2, " World Map ")
            clear_interior(w_map)
            h, w = w_map.getmaxyx()
            inner_h = max(1, h - 2)
            inner_w = max(1, w - 2)

            try:
                lines = asciiworld_to_lines(inner_w, inner_h, locations_path=state.dx_points_file)
            except Exception as e:
                lines = [f"asciiworld error: {e}"]

            for i in range(min(inner_h, len(lines))):
                add_ansi_str(w_map, 1 + i, 1, lines[i], inner_w)

            w_map.noutrefresh()
            state.dirty_map = False

        if state.dirty_status:
            maxy, maxx = stdscr.getmaxyx()
            draw_status(stdscr, state, maxy, maxx)
            state.dirty_status = False

        curses.doupdate()

    # exiting: let curses.wrapper restore terminal


if __name__ == "__main__":
    try:
        curses.wrapper(main)
    except Exception:
        import traceback
        traceback.print_exc()
def edit_simple_fields_dialog(stdscr, title: str, cfg: AppConfig, fields: List[dict]):
    h = max(10, len(fields) + 4)
    w = 60
    maxy, maxx = stdscr.getmaxyx()
    y = max(0, (maxy - h) // 2)
    x = max(0, (maxx - w) // 2)
    win = curses.newwin(h, w, y, x)
    win.keypad(True)
    idx = 0
    while True:
        win.erase()
        box_title(win, title)
        for i, fld in enumerate(fields):
            val = getattr(cfg, fld["attr"], "")
            if fld["type"] == "bool":
                disp = "Yes" if bool(val) else "No"
            else:
                disp = str(val)
            prefix = ">" if i == idx else " "
            safe_addstr(win, 1 + i, 2, f"{prefix} {fld['label']}: {disp}"[:w-4])
        safe_addstr(win, h - 2, 2, "Enter=Edit  Space=Toggle Bool  S=Save  Esc=Cancel"[:w-4])
        win.refresh()
        ch = win.getch()
        if ch in (27,):
            break
        elif ch in (curses.KEY_UP, ord('k')):
            idx = (idx - 1) % len(fields)
        elif ch in (curses.KEY_DOWN, ord('j')):
            idx = (idx + 1) % len(fields)
        elif ch == ord(' '):
            fld = fields[idx]
            if fld["type"] == "bool":
                setattr(cfg, fld["attr"], not bool(getattr(cfg, fld["attr"], False)))
        elif ch in (10, 13, curses.KEY_ENTER):
            fld = fields[idx]
            if fld["type"] != "bool":
                curses.echo()
                try:
                    win.move(1 + idx, min(w - 20, len(fld["label"]) + 6))
                    win.clrtoeol()
                    win.refresh()
                    raw = win.getstr(1 + idx, min(w - 20, len(fld["label"]) + 6), 30).decode("utf-8", errors="ignore")
                finally:
                    curses.noecho()
                if fld["type"] == "int":
                    try:
                        setattr(cfg, fld["attr"], int(raw.strip()))
                    except Exception:
                        pass
                else:
                    setattr(cfg, fld["attr"], raw.strip())
        elif ch in (ord('s'), ord('S')):
            break

