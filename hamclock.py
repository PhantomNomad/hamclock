
import curses
import os
import io
import contextlib
import importlib.util
import time
import threading
import socket
import ssl
import re
import json
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from typing import List, Optional

try:
    from PIL import Image
except Exception:
    Image = None


@dataclass
class AppConfig:
    dx_host: str = ""
    dx_port: int = 7300
    dx_user: str = ""
    dx_pass: str = ""

    wx_location_name: str = ""
    wx_lat: Optional[float] = None
    wx_lon: Optional[float] = None
    wx_grid: str = ""  # Maidenhead grid square (e.g., "DM79nu"); used if lat/lon not set
    wx_update_seconds: float = 1800.0  # 30 minutes (Open-Meteo poll interval)
    tz_label: str = "America/Denver"

    map_image_path: str = "world_map.jpg"

    refresh_seconds: float = 10.0
    map_refresh_seconds: float = 300.0  # 5 minutes

    time_refresh_seconds: float = 1.0


@dataclass
class AppState:
    running: bool = True
    paused: bool = False

    menu_visible: bool = False
    file_menu_open: bool = False
    menu_selected_idx: int = 0  # 0 Settings, 1 Exit

    menu_win: Optional['curses.window'] = None
    menu_h: int = 0
    menu_w: int = 0
    menu_y: int = 0
    menu_x: int = 0

    space_weather_lines: List[str] = field(default_factory=list)
    dx_lines: List[str] = field(default_factory=list)
    wx_static_lines: List[str] = field(default_factory=list)   # non-time lines
    wx_time_lines: List[str] = field(default_factory=list)     # local/utc only

    status_line: str = ""
    dirty_menu: bool = True
    dirty_space: bool = True
    dirty_dx: bool = True
    dirty_wx_static: bool = True
    dirty_wx_time: bool = True
    dirty_map: bool = True
    dirty_status: bool = True



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


def asciiworld_to_lines(inner_w: int, inner_h: int, map_shp_path: Optional[str] = None) -> List[str]:
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


def degrees_to_compass_16(deg: float) -> str:
    """Convert degrees (meteorological) to nearest 16-wind compass point (N, NNE, ...).
    Accepts any numeric value; normalizes to [0, 360).
    """
    deg = float(deg) % 360.0
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    idx = int((deg + 11.25) // 22.5) % 16
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
            f"Wind:  {ws:.0f} km/h @ {wd:.0f}° {degrees_to_compass_16(wd)}  Gust {gust:.0f}" if ws is not None and wd is not None and gust is not None else "Wind:  N/A",
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


def format_dx_spot(line: str) -> str:
    """
    Parse common DX cluster spot lines and reformat to:
        <spotter> <freq> <spotted> <HHMM>Z <grid>

    Example input:
        DX de F1SNB: 14090.0 XX9W FT8 JN33dh -> OL62 1750Z JN33
    Output:
        F1SNB 14090.0 XX9W 1750Z JN33

    If a line does not match expected format, returns the line with "DX de" removed.
    """
    clean = strip_dx_prefix(line).strip()

    # Expect: "<spotter>: <rest>"
    m = re.match(r'^(\S+):\s*(.+)$', clean)
    if not m:
        return clean

    spotter = m.group(1)
    rest = m.group(2).strip()

    # Heuristic parse for common "sh/dx" format: freq, spotted call, (optional comment...), HHMMZ, grid
    m2 = re.match(r'^(\d+(?:\.\d+)?)\s+(\S+)(?:\s+(.*?))?\s+(\d{4})Z\s+(\S+)', rest)
    if not m2:
        return clean

    freq = m2.group(1)
    spotted = m2.group(2)
    hhmm = m2.group(4)
    grid = m2.group(5)

    return f"{spotter} {freq} {spotted} {hhmm}Z {grid}"

def dx_cluster_worker(cfg: AppConfig, state: AppState):
    while state.running:
        try:
            state.status_line = f"Connecting to DX cluster {cfg.dx_host}:{cfg.dx_port}..."
            state.dirty_status = True
            with socket.create_connection((cfg.dx_host, cfg.dx_port), timeout=10) as s:
                s.settimeout(2.0)

                # Read a bit of banner
                t0 = time.time()
                while time.time() - t0 < 2.0:
                    try:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                    except socket.timeout:
                        break

                # Naive login: user then pass
                s.sendall((cfg.dx_user + "\n").encode("utf-8", errors="ignore"))
                time.sleep(0.2)
                s.sendall((cfg.dx_pass + "\n").encode("utf-8", errors="ignore"))
                time.sleep(0.3)

                # Requested: show DX spots
                s.sendall(b"sh/dx\n")

                state.status_line = "DX cluster connected. Command sent: sh/dx"
                state.dirty_status = True

                linebuf = b""
                while state.running:
                    try:
                        chunk = s.recv(4096)
                        if not chunk:
                            raise ConnectionError("DX cluster disconnected")

                        linebuf += chunk
                        while b"\n" in linebuf:
                            raw, linebuf = linebuf.split(b"\n", 1)
                            text = raw.decode("utf-8", errors="ignore").rstrip("\r")
                            text = format_dx_spot(text)
                            if text.strip():
                                state.dx_lines.append(text)
                                state.dx_lines = state.dx_lines[-80:]
                                state.dirty_dx = True
                    except socket.timeout:
                        continue

        except Exception as e:
            state.status_line = f"DX cluster error: {e} (retrying in 5s)"
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
    items = ["Settings", "Quit"]
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
    msg = state.status_line[: maxx - 2]
    safe_addstr(stdscr, maxy - 1, 0, " " * (maxx - 1), curses.A_REVERSE)
    safe_addstr(stdscr, maxy - 1, 1, msg, curses.A_REVERSE)


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
    cfg = AppConfig()
    state = AppState()

    curses.curs_set(0)
    stdscr.keypad(True)
    stdscr.timeout(100)  # 0.1s polling
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
        map_h = max(3, (maxy - 2) - map_start_y)
        w_map = curses.newwin(map_h, maxx, map_start_y, 0)
        box_title(w_map, "World Map")

        # Force redraw everything in new windows
        state.dirty_space = True
        state.dirty_dx = True
        state.dirty_wx_static = True
        state.dirty_wx_time = True
        state.dirty_map = True
        state.dirty_menu = True
        state.dirty_status = True

        return w_space, w_dx, w_wx, w_map

    w_space, w_dx, w_wx, w_map = build_windows()

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
                w_space, w_dx, w_wx, w_map = build_windows()

                space_inner_w = _space_inner_w()

                state.dirty_dx = True
                state.dirty_wx_static = True
                state.dirty_wx_time = True

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
                    state.dirty_map = True
                    state.dirty_status = True
                elif k == curses.KEY_UP:
                    state.menu_selected_idx = max(0, state.menu_selected_idx - 1)
                    state.dirty_menu = True
                elif k == curses.KEY_DOWN:
                    state.menu_selected_idx = min(1, state.menu_selected_idx + 1)
                    state.dirty_menu = True
                elif k in (curses.KEY_ENTER, 10, 13):
                    if state.menu_selected_idx == 0:
                        state.status_line = "Settings selected (not implemented yet)."
                        state.dirty_status = True
                    elif state.menu_selected_idx == 1:
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
            draw_box_contents(w_dx, state.dx_lines[-8:], "DX Cluster")
            state.dirty_dx = False

        if state.dirty_wx_static or state.dirty_wx_time:
            # combine static + time lines into the wx box
            lines = list(state.wx_static_lines) + list(state.wx_time_lines)
            draw_box_contents(w_wx, lines, "Local Info")
            state.dirty_wx_static = False
            state.dirty_wx_time = False

        if state.dirty_map:
            # Render asciiworld into the interior (exactly inner_w x inner_h)
            clear_interior(w_map)
            h, w = w_map.getmaxyx()
            inner_h = max(1, h - 2)
            inner_w = max(1, w - 2)

            try:
                lines = asciiworld_to_lines(inner_w, inner_h)
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