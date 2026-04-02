
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
    wx_lat: Optional[float] = None
    wx_lon: Optional[float] = None
    wx_grid: str = ""  # Maidenhead grid square (e.g., "EO42nu"); used if lat/lon not set
    wx_update_seconds: float = 1800.0  # 30 minutes (Open-Meteo poll interval)
    tz_label: str = "America/Denver"

    map_image_path: str = "world_map.jpg"

    refresh_seconds: float = 10.0
    map_refresh_seconds: float = 300.0  # 5 minutes

    time_refresh_seconds: float = 1.0
    enable_logging: bool = True
    online_lookup_website: str = "hamdb.org"
    online_lookup_username: str = ""
    online_lookup_password: str = ""


@dataclass
class AppState:
    running: bool = True
    paused: bool = False

    menu_visible: bool = False
    file_menu_open: bool = False
    menu_selected_idx: int = 0  # 0 Settings, 1 Online Lookup, 2 Callsign Lookup, 3 DX Command, 4 Quit

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




# ---- config persistence ----
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hamclock_settings.json")

def load_config(path: str = CONFIG_FILE) -> AppConfig:
    """Load persisted configuration from JSON.

    Returns an AppConfig populated with defaults, overridden by any values found
    in the JSON file. Unknown keys in the file are ignored.
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

    # Only accept known fields (avoid crashing on extra keys)
    for k in getattr(cfg, "__dataclass_fields__", {}).keys():
        if k in data:
            try:
                setattr(cfg, k, data[k])
            except Exception:
                pass

    set_logging_enabled(getattr(cfg, "enable_logging", True))
    return cfg

def save_config(cfg: AppConfig, path: str = CONFIG_FILE) -> None:
    set_logging_enabled(getattr(cfg, "enable_logging", True))
    data = {k: getattr(cfg, k) for k in cfg.__dataclass_fields__.keys()}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

# ---- asciiworld integration ----
_ASCIIWORLD_MOD = None

def _load_asciiworld_module():
    global _ASCIIWORLD_MOD
    if _ASCIIWORLD_MOD is not None:
        return _ASCIIWORLD_MOD

    here = os.path.dirname(os.path.abspath(__file__))
    aw_path = os.path.join(here, "asciiworld.py")
    if not os.path.exists(aw_path):
        raise FileNotFoundError(f"asciiworld.py not found next to MyHamClock.py: {aw_path}")

    spec = importlib.util.spec_from_file_location("asciiworld", aw_path)
    mod = importlib.util.module_from_spec(spec)
    import sys as _sys
    _sys.modules["asciiworld"] = mod  # required for dataclasses on some Python versions
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    _ASCIIWORLD_MOD = mod
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
    """Populate state.space_weather_lines with live space weather + optional HF band conditions."""
    try:
        xml = _fetch_url("https://www.hamqsl.com/solarxml.php", timeout=10.0)

        def get(tag: str) -> str:
            m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.IGNORECASE | re.DOTALL)
            return m.group(1).strip() if m else ""

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

        # Band conditions (grouped to match solarxml.php style)
        #
        # Groups: 80-40, 30-20, 17-15, 12-10 (day and night columns)
        def _band_condition(sfi_val: str, kp_val: str, mhz: float) -> str:
            try:
                s = float(sfi_val) if sfi_val else 0.0
            except Exception:
                s = 0.0
            try:
                k = float(kp_val) if kp_val else 0.0
            except Exception:
                k = 0.0

            # Simple, stable heuristic (not VOACAP): higher bands are more sensitive to SFI and Kp.
            if mhz >= 21:
                if s >= 140 and k <= 4: return "Open"
                if s >= 110 and k <= 5: return "Fair"
                if s >= 90 and k <= 6:  return "Poor"
                return "Closed"
            if mhz >= 14:
                if s >= 120 and k <= 5: return "Open"
                if s >= 100 and k <= 6: return "Fair"
                if s >= 80:             return "Poor"
                return "Closed"
            if mhz >= 7:
                if k >= 7: return "Poor"
                return "Open" if s >= 70 else "Fair"
            # 80m/40m generally robust; geomagnetic storms can still degrade.
            return "Poor" if k >= 7 else "Open"

        def _worsen_one_step(cond: str) -> str:
            order = ["Open", "Fair", "Poor", "Closed"]
            try:
                i = order.index(cond)
            except ValueError:
                return cond or "—"
            return order[min(i + 1, len(order) - 1)]

        def band_group_lines(sfi_val: str, kp_val: str):
            groups = [
                ("80-40", (3.5, 7.0)),
                ("30-20", (10.0, 14.0)),
                ("17-15", (18.0, 21.0)),
                ("12-10", (24.0, 28.0)),
            ]

            # Header line (no band label on this line)
            out = ["     Day  Night"]

            for label, (f1, f2) in groups:
                # Use the best condition within the group for "day" (optimistic, matches how
                # many operators read grouped band guidance). If you prefer "worst", we can switch.
                c1 = _band_condition(sfi_val, kp_val, f1)
                c2 = _band_condition(sfi_val, kp_val, f2)
                day = c1 if ["Open","Fair","Poor","Closed"].index(c1) <= ["Open","Fair","Poor","Closed"].index(c2) else c2

                # Night is typically worse on higher bands; apply a conservative one-step degrade.
                night = _worsen_one_step(day) if f2 >= 14.0 else day

                # Fixed-width formatting so the whole right column can be right-justified later.
                out.append(f"{label:>5} {day:<4} {night:<5}")
            return out

        right = band_group_lines(sfi, kindex)

        # Decide 1 or 2 columns based on available width (interior)
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
    """Return (lat, lon) from cfg.wx_lat/lon, else derived from cfg.wx_grid."""
    if cfg.wx_lat is not None and cfg.wx_lon is not None:
        return float(cfg.wx_lat), float(cfg.wx_lon)
    if cfg.wx_grid:
        return maidenhead_to_latlon(cfg.wx_grid)
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
            "Temp:  N/A (set wx_lat/wx_lon or wx_grid)",
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

        loc = cfg.wx_location_name or f"{lat:.3f},{lon:.3f}"
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
    # Example: "DX de VE6XYZ:  14074.0 ..." -> "VE6XYZ:  14074.0 ..."
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
    primary = os.path.join(_script_dir(), CALLSIGN_DB_NAME)
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


def _calls_table_has_columns(conn: sqlite3.Connection, wanted_cols) -> bool:
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(calls)")
        cols = {str(row[1]).lower() for row in cur.fetchall()}
        return all(col.lower() in cols for col in wanted_cols)
    except Exception:
        return False


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set:
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1]).lower() for row in cur.fetchall()}
    except Exception:
        return set()


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


def _upsert_hamcall_calls_from_qrz(conn: sqlite3.Connection, info: dict) -> None:
    if not info:
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


def _cache_hamqth_result_in_calls(conn: sqlite3.Connection, info: dict) -> None:
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


def _cache_online_result_in_calls(conn: sqlite3.Connection, info: dict, provider: str) -> None:
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


def _lookup_callsign_local_first(callsign: str) -> Tuple[Optional[dict], str]:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return None, ""

    module = _load_callsign_lookup_module()
    db_path = _callsign_db_path()
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
                _debug_log(f"lookup miss source=hamcall_calls callsign={callsign!r}")
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
                _debug_log(f"lookup miss source=calls callsign={callsign!r}")
            except Exception as e:
                _debug_log(f"lookup error source=calls callsign={callsign!r} db={db_path!r}: {e}")
    except Exception as e:
        _debug_log(f"_lookup_callsign_local_first: sqlite open failed callsign={callsign!r} db={db_path!r}: {e}")

    return None, db_path





def lookup_callsign_info(callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {"callsign": "", "name": "", "city": "", "prov_state": "", "country": "", "source": "not found"}

    local_result, db_path = _lookup_callsign_local_first(callsign)
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
                with sqlite3.connect(db_path) as conn:
                    merged = dict(hamqth_raw or {})
                    merged.setdefault("callsign", result.get("callsign"))
                    merged.setdefault("name", result.get("name"))
                    merged.setdefault("city", result.get("city"))
                    merged.setdefault("prov_state", result.get("prov_state"))
                    merged.setdefault("country", result.get("country"))
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



def lookup_callsign_info_cfg(cfg: AppConfig, state: AppState, callsign: str) -> dict:
    callsign = _normalize_callsign(callsign)
    if not callsign:
        return {"callsign": "", "name": "", "city": "", "prov_state": "", "country": "", "source": "not found"}

    local_result, db_path = _lookup_callsign_local_first(callsign)
    if local_result:
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
            try:
                with sqlite3.connect(db_path) as conn:
                    merged = dict(raw or {})
                    merged.setdefault("callsign", result.get("callsign"))
                    merged.setdefault("name", result.get("name"))
                    merged.setdefault("city", result.get("city"))
                    merged.setdefault("prov_state", result.get("prov_state"))
                    merged.setdefault("country", result.get("country"))
                    _cache_online_result_in_calls(conn, merged, provider)
                    if provider == "qrz.com":
                        _upsert_hamcall_calls_from_qrz(conn, merged)
            except Exception as e:
                _debug_log(f"lookup cache write failed provider={provider!r} callsign={callsign!r} db={db_path!r}: {e}")
            return result

        _debug_log(f"lookup source=not_found provider={provider!r} callsign={callsign!r}")
    except Exception as e:
        _debug_log(f"lookup error source=online provider={provider!r} callsign={callsign!r}: {e}")
        state.status_line = f"Lookup error ({provider}): {e}"
        state.dirty_status = True

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

    return {
        "spotter": _normalize_callsign(m.group(1)),
        "frequency": m.group(2),
        "spotted": _normalize_callsign(m.group(3)),
        "comment": (m.group(4) or "").strip(),
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

                s.sendall((cfg.dx_user + "\n").encode("utf-8", errors="ignore"))
                time.sleep(0.2)
                s.sendall((cfg.dx_pass + "\n").encode("utf-8", errors="ignore"))
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
                                state.dx_lines.append(display_text)
                                state.dx_lines = state.dx_lines[-80:]
                                state.dirty_dx = True

                            if not raw_text.lstrip().upper().startswith("DX DE "):
                                continue

                            update_dedx_panel_from_spot(cfg, state, raw_text)
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


def edit_settings_dialog(stdscr, cfg: AppConfig, state: AppState):
    """Simple curses form to edit settings from AppConfig and persist to JSON."""
    fields = [
        ("DX Host", "dx_host"),
        ("DX Port", "dx_port"),
        ("DX User", "dx_user"),
        ("DX Pass", "dx_pass"),
        ("DX Filter", "dx_filter"),
        ("WX Location Name", "wx_location_name"),
        ("WX Lat (blank=auto)", "wx_lat"),
        ("WX Lon (blank=auto)", "wx_lon"),
        ("WX Grid (optional)", "wx_grid"),
        ("WX Update Seconds", "wx_update_seconds"),
        ("TZ Label", "tz_label"),
        ("Refresh Seconds", "refresh_seconds"),
        ("Map Refresh Seconds", "map_refresh_seconds"),
        ("Time Refresh Seconds", "time_refresh_seconds"),
        ("Enable Logging", "enable_logging"),
    ]

    # build working copy as strings
    values = {}
    old_dx_filter = cfg.dx_filter
    for label, attr in fields:
        v = getattr(cfg, attr)
        if attr == "enable_logging":
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
        state.dirty_menu = True
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_status = True

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

        top = max(0, idx - (h - 6))
        visible = fields[top: top + (h - 5)]

        for row, (label, attr) in enumerate(visible):
            y = 2 + row
            is_sel = (top + row) == idx
            attr_style = curses.A_REVERSE if is_sel else curses.A_NORMAL
            val = values.get(attr, "")
            left = f"{label}:"
            safe_addstr(win, y, 2, left[: w - 4], attr_style)
            # value column
            col = min(w - 4, 28)
            safe_addstr(win, y, col, val[: (w - col - 2)], attr_style)

        safe_addstr(win, h - 2, 2, "Up/Down select. Enter edits or toggles. F2 saves to hamclock_settings.json.", curses.A_DIM)
        win.refresh()

        k = win.getch()
        if k in (27,):  # ESC
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_UP,):
            idx = max(0, idx - 1)
        elif k in (curses.KEY_DOWN,):
            idx = min(len(fields) - 1, idx + 1)
        elif k in (curses.KEY_F2,):
            # apply + save
            for _, attr in fields:
                s = values.get(attr, "")
                try:
                    if attr in ("dx_port",):
                        setattr(cfg, attr, int(s))
                    elif attr in ("refresh_seconds", "map_refresh_seconds", "time_refresh_seconds", "wx_update_seconds"):
                        setattr(cfg, attr, float(s))
                    elif attr in ("wx_lat", "wx_lon"):
                        setattr(cfg, attr, None if s.strip() == "" else float(s))
                    elif attr == "enable_logging":
                        setattr(cfg, attr, str(s).strip().lower() in ("1", "true", "yes", "on", "y"))
                    else:
                        setattr(cfg, attr, s)
                except Exception:
                    pass
            try:
                save_config(cfg)
                set_logging_enabled(getattr(cfg, "enable_logging", True))
                state.status_line = f"Settings saved to {CONFIG_FILE}"
                # If DX filter changed, clear local spot list and re-apply filter on the live cluster.
                new_dx_filter = cfg.dx_filter
                if (new_dx_filter or "").strip() != (old_dx_filter or "").strip():
                    # Clear local display immediately
                    state.dx_lines.clear()
                    state.dirty_dx = True

                    # Enqueue cluster commands (DX Spider). These will be sent by dx_cluster_worker if connected.
                    # CLEAR/SPOTS clears existing spot filter state on the cluster before applying the new filter.
                    state.dx_cmd_queue.put("CLEAR/SPOTS")
                    if (new_dx_filter or "").strip():
                        state.dx_cmd_queue.put((new_dx_filter or "").strip())
                    state.dx_cmd_queue.put("SH/DX 6")
                    state.dirty_status = True
            except Exception as e:
                state.status_line = f"Settings save failed: {e}"
            state.dirty_status = True
            close_dialog(win, y0, x0, h, w)
            curses.curs_set(0)
            stdscr.timeout(100)
            curses.doupdate()
            return
        elif k in (curses.KEY_ENTER, 10, 13):
            label, attr = fields[idx]
            if attr == "enable_logging":
                values[attr] = "Off" if str(values.get(attr, "On")).strip().lower() == "on" else "On"
                continue
            prompt = f"{label}: "
            # simple input line at bottom
            inp = values.get(attr, "")
            win2 = curses.newwin(3, w, y0 + h - 4, x0)
            win2.erase()
            win2.box()
            safe_addstr(win2, 1, 2, prompt)
            safe_addstr(win2, 1, min(w - 3, 2 + len(prompt)), str(inp)[: max(0, w - (4 + len(prompt)))])
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

    curses.curs_set(0)
    stdscr.timeout(100)




def edit_online_lookup_dialog(stdscr, cfg: AppConfig, state: AppState):
    fields = [
        ("Website", "online_lookup_website"),
        ("Username", "online_lookup_username"),
        ("Password", "online_lookup_password"),
    ]

    values = {}
    old_provider = _get_selected_lookup_provider(cfg)
    for _, attr in fields:
        values[attr] = str(getattr(cfg, attr, "") or "")

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
        state.dirty_menu = True
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_status = True

    while True:
        maxy, maxx = stdscr.getmaxyx()
        w = min(76, max(44, maxx - 4))
        h = 10
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
        state.dirty_menu = True
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
    state.dirty_menu = True
    state.dirty_space = True
    state.dirty_dx = True
    state.dirty_wx_static = True
    state.dirty_wx_time = True
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

def draw_menu(stdscr, state: AppState, maxx: int):
    # Always manage menu row (y=0) to avoid terminal artifacts
    if state.menu_visible:
        safe_addstr(stdscr, 0, 0, " " * maxx, curses.A_REVERSE)
        safe_addstr(stdscr, 0, 1, "File", curses.A_REVERSE | curses.A_BOLD)
    else:
        safe_addstr(stdscr, 0, 0, " " * maxx, curses.A_NORMAL)

    # If submenu should not be shown, actively erase it if it exists
    if (not state.menu_visible) or (not state.file_menu_open):
        if state.menu_win is not None:
            try:
                state.menu_win.erase()
                state.menu_win.noutrefresh()
            except curses.error:
                pass
            # also clear its rectangle on stdscr in case underlying content isn't repainted immediately
            if state.menu_h and state.menu_w:
                _clear_rect(stdscr, state.menu_y, state.menu_x, state.menu_h, state.menu_w)
        return

    # Build/reuse submenu window under "File"
    items = ["Settings", "Online Lookup", "Callsign Lookup...", "DX Command...", "Quit"]
    x0, y0 = 1, 1
    w = max(len(i) for i in items) + 4
    h = len(items) + 2

    # If size/pos changed, recreate
    if (state.menu_win is None or state.menu_h != h or state.menu_w != w
            or state.menu_y != y0 or state.menu_x != x0):
        state.menu_win = curses.newwin(h, w, y0, x0)
        state.menu_h, state.menu_w, state.menu_y, state.menu_x = h, w, y0, x0

    win = state.menu_win
    win.bkgd(" ", curses.A_NORMAL)
    win.erase()
    win.box()

    for i, item in enumerate(items):
        attr = curses.A_REVERSE if i == state.menu_selected_idx else curses.A_NORMAL
        safe_addstr(win, 1 + i, 2, item.ljust(w - 3), attr)

    win.noutrefresh()


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


def draw_status(stdscr, state: AppState, maxy: int, maxx: int):
    lookup_msg = (getattr(state, "online_lookup_status_line", "") or "").strip() or "Lookup: idle"
    dx_msg = (getattr(state, "dx_status_line", "") or "").strip() or "DX: disconnected"
    bar_w = max(0, maxx - 1)
    if bar_w <= 0:
        return
    if maxy >= 2:
        safe_addstr(stdscr, maxy - 2, 0, " " * bar_w, curses.A_REVERSE)
        safe_addstr(stdscr, maxy - 2, 0, lookup_msg[:bar_w].ljust(bar_w), curses.A_REVERSE)
    safe_addstr(stdscr, maxy - 1, 0, " " * bar_w, curses.A_REVERSE)
    safe_addstr(stdscr, maxy - 1, 0, dx_msg[:bar_w].ljust(bar_w), curses.A_REVERSE)


def read_key_with_esc_logic(stdscr, esc_delay_ms=120) -> Optional[int]:
    k = stdscr.getch()
    if k == -1:
        return None

    if k == 27:
        stdscr.nodelay(True)
        curses.napms(esc_delay_ms)
        k2 = stdscr.getch()
        stdscr.nodelay(False)
        if k2 == -1:
            return 27
        return k2

    return k


def main(stdscr):
    config_exists = os.path.exists(CONFIG_FILE)
    cfg = load_config()
    set_logging_enabled(getattr(cfg, "enable_logging", True))
    state = AppState()
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


    # Layout: compute once, but also handle resize by rebuilding windows
    def build_windows():
        maxy, maxx = stdscr.getmaxyx()
        menu_rows = 1  # menu row reserved even if hidden (reduces jitter); we simply don't draw it
        top_box_height = 8
        top_y = 1  # keep row 0 for menu (even hidden)

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

        w_dedx = curses.newwin(map_h, dedx_w, map_start_y, 0)
        box_title(w_dedx, "DE / DX")

        w_map = curses.newwin(map_h, map_w, map_start_y, dedx_w)
        box_title(w_map, "World Map")

        # Force redraw everything in new windows
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_dedx = True
        state.dirty_map = True
        state.dirty_menu = True
        state.dirty_status = True

        return w_space, w_dx, w_wx, w_dedx, w_map

    w_space, w_dx, w_wx, w_dedx, w_map = build_windows()

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

    while state.running:
        now = time.time()

        # DX spot map overlay disabled: leave base world map rendering unchanged.

        # periodic updates (paused when menu is open)
        if not state.paused and (now - last_refresh) >= cfg.refresh_seconds:
            update_space_weather(state, panel_inner_w=space_inner_w)
            if (now - last_wx_refresh) >= cfg.wx_update_seconds:
                update_weather_open_meteo(cfg, state)
                last_wx_refresh = now
            last_refresh = now

        # World map refresh (every cfg.map_refresh_seconds)
        if not state.paused and (now - last_map_refresh) >= cfg.map_refresh_seconds:
            state.dirty_map = True
            last_map_refresh = now

        if not state.paused and (now - last_time_refresh) >= cfg.time_refresh_seconds:
            update_time_lines(state)
            last_time_refresh = now

        # input
        k = read_key_with_esc_logic(stdscr)
        if k is not None:
            if k == curses.KEY_RESIZE:
                stdscr.erase()
                w_space, w_dx, w_wx, w_dedx, w_map = build_windows()

                space_inner_w = _space_inner_w()

                state.dirty_dx = True
                state.dirty_wx_static = True
                state.dirty_wx_time = True
                state.dirty_dedx = True

            elif state.menu_visible:
                if k == 27:  # ESC closes menu
                    state.menu_visible = False
                    state.file_menu_open = False
                    state.paused = False
                    # Force repaint of underlying panels that may have been covered
                    state.dirty_menu = True
                    state.dirty_space = True
                    state.dirty_dx = True
                    state.dirty_wx_static = True
                    state.dirty_wx_time = True
                    state.dirty_dedx = True
                    state.dirty_map = True
                    state.dirty_status = True
                elif k == curses.KEY_UP:
                    state.menu_selected_idx = max(0, state.menu_selected_idx - 1)
                    state.dirty_menu = True
                elif k == curses.KEY_DOWN:
                    state.menu_selected_idx = min(4, state.menu_selected_idx + 1)
                    state.dirty_menu = True
                elif k in (curses.KEY_ENTER, 10, 13):
                    if state.menu_selected_idx == 0:
                        edit_settings_dialog(stdscr, cfg, state)
                        state.menu_visible = False
                        state.file_menu_open = False
                        state.paused = False
                        state.dirty_menu = True
                        state.dirty_space = True
                        state.dirty_dx = True
                        state.dirty_wx_static = True
                        state.dirty_wx_time = True
                        state.dirty_map = True
                        state.dirty_status = True

                    elif state.menu_selected_idx == 1:
                        edit_online_lookup_dialog(stdscr, cfg, state)
                        state.menu_visible = False
                        state.file_menu_open = False
                        state.paused = False
                        state.dirty_menu = True
                        state.dirty_space = True
                        state.dirty_dx = True
                        state.dirty_wx_static = True
                        state.dirty_wx_time = True
                        state.dirty_dedx = True
                        state.dirty_map = True
                        state.dirty_status = True

                    elif state.menu_selected_idx == 2:
                        callsign_lookup_dialog(stdscr, cfg, state)
                        state.menu_visible = False
                        state.file_menu_open = False
                        state.paused = False
                        state.dirty_menu = True
                        state.dirty_space = True
                        state.dirty_dx = True
                        state.dirty_wx_static = True
                        state.dirty_wx_time = True
                        state.dirty_dedx = True
                        state.dirty_map = True
                        state.dirty_status = True

                    elif state.menu_selected_idx == 3:
                        dx_command_dialog(stdscr, state)
                        state.menu_visible = False
                        state.file_menu_open = False
                        state.paused = False
                        state.dirty_menu = True
                        state.dirty_space = True
                        state.dirty_dx = True
                        state.dirty_wx_static = True
                        state.dirty_wx_time = True
                        state.dirty_dedx = True
                        state.dirty_map = True
                        state.dirty_status = True

                    elif state.menu_selected_idx == 4:
                        state.running = False
                elif k in (ord('q'), ord('Q')):
                    state.running = False

            else:
                if k == 27:  # ESC opens menu
                    state.menu_visible = True
                    state.file_menu_open = True
                    state.menu_selected_idx = 0
                    state.paused = True
                    state.dirty_menu = True
                elif k in (ord('q'), ord('Q')):
                    state.running = False

        # draw ONLY dirty areas
        if state.dirty_space:
            draw_box_contents(w_space, state.space_weather_lines, "Space Weather")
            state.dirty_space = False

        if state.dirty_dx:  
            dx_visible = max(1, w_dx.getmaxyx()[0] - 2)  
            draw_box_contents(w_dx, state.dx_lines[-dx_visible:], "DX Cluster")  
            state.dirty_dx = False

        if state.dirty_wx_static or state.dirty_wx_time:
            # combine static + time lines into the wx box
            lines = list(state.wx_static_lines) + list(state.wx_time_lines)
            draw_box_contents(w_wx, lines, "Local Info")
            state.dirty_wx_static = False
            state.dirty_wx_time = False

        if state.dirty_dedx:
            h_dedx, w_dedx_inner = w_dedx.getmaxyx()
            inner_w = max(1, w_dedx_inner - 2)
            inner_h = max(1, h_dedx - 2)
            dedx_lines = build_dedx_lines(state, inner_w, inner_h)
            draw_box_contents(w_dedx, dedx_lines, "DE / DX")
            state.dirty_dedx = False

        if state.dirty_map:
            # Render asciiworld into the interior (exactly inner_w x inner_h)
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

        if state.dirty_menu:
            # only redraw menu line when menu visibility changes
            maxy, maxx = stdscr.getmaxyx()
            if state.menu_visible:
                draw_menu(stdscr, state, maxx)
            else:
                # clear the menu row to avoid artifacts
                safe_addstr(stdscr, 0, 0, " " * maxx, curses.A_NORMAL)
            state.dirty_menu = False

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
        input("Press Enter to close...")
