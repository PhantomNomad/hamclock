#!/usr/bin/env python3
"""
asciiworld (Python 3 port)

A mostly line-by-line port of the original asciiworld.c:
- Reads a polygon shapefile (default: ne_110m_land.shp) and draws it onto an in-memory raster.
- Optionally computes current sun position (UTC) and shades the map (day/night + twilight).
- Optionally draws the sun marker, terminator border, world border, and tracks/points/circles from a locations file.
- Outputs either:
  * ANSI-colored ASCII art to stdout (default), or
  * a PNG image with -W <out.png>

Notes:
- This port uses pure-Python libraries instead of libgd/shapelib:
  * Pillow (PIL) for raster drawing
  * pyshp (shapefile) for shapefile reading

Compatibility:
- Keeps CLI flags aligned with the C version.
- Keeps all other repo files unchanged. This file replaces the compiled C binary with a Python executable script.

"""
from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import sys
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple

try:
    import shapefile  # pyshp
except Exception as e:  # pragma: no cover
    print("ERROR: Missing dependency 'pyshp' (import shapefile). Install: pip install pyshp", file=sys.stderr)
    raise

try:
    from PIL import Image, ImageDraw
except Exception as e:  # pragma: no cover
    print("ERROR: Missing dependency 'Pillow'. Install: pip install Pillow", file=sys.stderr)
    raise


DEG_2_RAD = math.pi / 180.0
RAD_2_DEG = 180.0 / math.pi

NUM_SHADES = 8
NUM_TRACKS = 3

DEFAULT_MAP = "ne_110m_land.shp"

# ANSI sequences must match the C enum order.
SEQ_RESET = 0
SEQ_LOCATION = 1
SEQ_SUN = 2
SEQ_SUN_BORDER = 3
SEQ_SHADE1 = 4
SEQ_SHADE2 = 5
SEQ_SHADE3 = 6
SEQ_SHADE4 = 7
SEQ_SHADE5 = 8
SEQ_SHADE6 = 9
SEQ_SHADE7 = 10
SEQ_SHADE8 = 11
SEQ_LINE = 12
SEQ_TRACK1 = 13
SEQ_TRACK2 = 14
SEQ_TRACK3 = 15
SEQ_TITLE = 16

SEQ_256COLORS = [
    "\033[0m",        # reset
    "\033[38;5;196m", # location
    "\033[38;5;220m", # sun
    "\033[38;5;220m", # sun border
    "\033[38;5;18m",  # shade1
    "\033[38;5;19m",  # shade2
    "\033[38;5;21m",  # shade3
    "\033[38;5;26m",  # shade4
    "\033[38;5;30m",  # shade5
    "\033[38;5;35m",  # shade6
    "\033[38;5;40m",  # shade7
    "\033[38;5;46m",  # shade8
    "\033[38;5;255m", # line
    "\033[38;5;201m", # track1
    "\033[38;5;255m", # track2
    "\033[38;5;202m", # track3
    "\033[1m",        # title
]

SEQ_8COLORS = [
    "\033[0m",    # reset
    "\033[31;1m", # location
    "\033[33m",   # sun
    "\033[36m",   # sun border
    "\033[34m",   # shade1
    "\033[34m",   # shade2
    "\033[34;1m", # shade3
    "\033[34;1m", # shade4
    "\033[32m",   # shade5
    "\033[32m",   # shade6
    "\033[32;1m", # shade7
    "\033[32;1m", # shade8
    "\033[37m",   # line
    "\033[35;1m", # track1
    "\033[35;1m", # track2
    "\033[35;1m", # track3
    "\033[1m",    # title
]


@dataclass
class Sun:
    active: bool = False
    lon: float = 0.0
    lat: float = 0.0


ProjectFn = Callable[["Screen", float, float], Tuple[float, float]]


@dataclass
class Screen:
    width: int = 0
    height: int = 0

    # "Raster" is an RGB image; palette-based ids from gd are replaced with explicit RGB colors.
    img: Optional[Image.Image] = None
    draw: Optional[ImageDraw.ImageDraw] = None

    # Colors are RGB tuples.
    col_black: Tuple[int, int, int] = (0, 0, 0)
    col_normal: Tuple[int, int, int] = (200, 200, 200)
    col_shade: List[Tuple[int, int, int]] = field(default_factory=list)
    col_track: List[Tuple[int, int, int]] = field(default_factory=list)
    col_highlight: Tuple[int, int, int] = (255, 0, 0)
    col_sun: Tuple[int, int, int] = (255, 255, 0)
    col_sun_border: Tuple[int, int, int] = (255, 255, 0)
    col_line: Tuple[int, int, int] = (255, 255, 255)

    brush: Tuple[int, int, int] = (255, 255, 255)

    esc_seq: Sequence[str] = field(default_factory=lambda: SEQ_256COLORS)
    title: Optional[str] = None

    solid_land: bool = False
    world_border: bool = False
    disable_colors: bool = False
    shade_steps_degree: float = 1.0
    dusk_degree: float = 6.0

    project: ProjectFn = None  # assigned in init
    sun: Sun = field(default_factory=Sun)

    def __post_init__(self):
        if self.project is None:
            self.project = project_equirect


def project_kavrayskiy(s: Screen, lon: float, lat: float) -> Tuple[float, float]:
    lonr = lon * DEG_2_RAD
    latr = lat * DEG_2_RAD
    # actual projection
    x = 3.0 / 2.0 * lonr * math.sqrt(1.0 / 3.0 - (latr / math.pi) * (latr / math.pi))
    y = lat
    # scale to screen
    x *= RAD_2_DEG
    x = (x + 180.0) / 360.0 * s.width
    y = (180.0 - (y + 90.0)) / 180.0 * s.height
    return x, y


def project_lambert(s: Screen, lon: float, lat: float) -> Tuple[float, float]:
    # actual projection
    x = lon
    y = math.sin(lat * DEG_2_RAD)
    # scale to screen
    x = (x + 180.0) / 360.0 * s.width
    y *= 90.0
    y = (180.0 - (y + 90.0)) / 180.0 * s.height
    return x, y


def project_hammer(s: Screen, lon: float, lat: float) -> Tuple[float, float]:
    lonr = lon * DEG_2_RAD
    latr = lat * DEG_2_RAD
    denom = math.sqrt(1.0 + math.cos(latr) * math.cos(lonr * 0.5))
    x = (2.0 * math.sqrt(2.0) * math.cos(latr) * math.sin(lonr * 0.5)) / denom
    y = (math.sqrt(2.0) * math.sin(latr)) / denom
    x *= RAD_2_DEG
    y *= RAD_2_DEG
    x = (x + 180.0) / 360.0 * s.width
    y = (180.0 - (y + 90.0)) / 180.0 * s.height
    return x, y


def project_equirect(s: Screen, lon: float, lat: float) -> Tuple[float, float]:
    x = (lon + 180.0) / 360.0 * s.width
    y = (180.0 - (lat + 90.0)) / 180.0 * s.height
    return x, y


def calc_sun(sun: Sun, now_utc: Optional[_dt.datetime] = None) -> None:
    """
    Port of calc_sun() from the C code. Uses UTC time and the same equations
    referenced by 'sonnenstand.txt' in the repo.
    """
    if now_utc is None:
        now_utc = _dt.datetime.now(tz=_dt.timezone.utc)

    # Match struct tm fields
    yday = int(now_utc.strftime("%j")) - 1  # 0-based
    sec_of_day = now_utc.hour * 3600 + now_utc.minute * 60 + now_utc.second

    # Direct port; keep numeric constants and signs as-is.
    eq_term = (-0.171 * math.sin(0.0337 * (yday + 1) + 0.465) -
               0.1299 * math.sin(0.01787 * (yday + 1) - 0.168))
    sun.lon = ((sec_of_day) - (86400.0 / 2.0 + (eq_term * -3600.0))) * (-360.0 / 86400.0)
    sun.lat = 0.4095 * math.sin(0.016906 * ((yday + 1) - 80.086)) * RAD_2_DEG


def screen_init_img(s: Screen, width: int, height: int) -> bool:
    s.width = int(width)
    s.height = int(height)

    s.img = Image.new("RGB", (s.width, s.height), s.col_black)
    s.draw = ImageDraw.Draw(s.img)

    # shades: in C they interpolate green/blue; keep same formula
    s.col_shade = []
    for i in range(NUM_SHADES):
        g = int(round(255 * i / float(NUM_SHADES - 1)))
        b = int(round(255 * (NUM_SHADES - 1 - i) / float(NUM_SHADES - 1)))
        s.col_shade.append((0, g, b))

    s.col_track = []
    for i in range(NUM_TRACKS):
        b = int(round(255 * (NUM_TRACKS - 1 - i) / float(NUM_TRACKS - 1)))
        s.col_track.append((255, 0, b))

    return True


def _print_color(s: Screen, seq: int) -> None:
    if not s.disable_colors:
        sys.stdout.write(s.esc_seq[seq])


def _screen_print_title(s: Screen, x: int, y: int) -> bool:
    # Direct port of the ASCII-title rendering logic.
    if not s.title:
        return False

    title = s.title
    box_x_margin = 2

    tx1 = (s.width // 2 - len(title)) - 2 * box_x_margin
    tx2 = tx1 + len(title)

    bx1 = tx1 - box_x_margin
    bx2 = tx2 + box_x_margin

    bmx1 = bx1 - box_x_margin
    bmx2 = bx2 + box_x_margin

    if bmx1 < 0 or bmx2 > s.width // 2:
        return False

    xr = x // 2
    yr = y // 2
    ti = xr - tx1

    if bmx1 <= xr < bmx2 and yr <= 4:
        if xr < bx1 or bx2 <= xr:
            sys.stdout.write(" ")
        else:
            if yr in (0, 4):
                sys.stdout.write(" ")
            elif yr in (1, 3):
                if xr == bx1 or xr == bx2 - 1:
                    sys.stdout.write("+")
                else:
                    sys.stdout.write("-")
            elif yr == 2:
                if xr == bx1 or xr == bx2 - 1:
                    sys.stdout.write("|")
                else:
                    if xr < tx1 or tx2 <= xr:
                        sys.stdout.write(" ")
                    else:
                        _print_color(s, SEQ_TITLE)
                        sys.stdout.write(title[ti])
                        _print_color(s, SEQ_RESET)
        return True
    return False


def screen_show_interpreted(s: Screen, trailing_newline: bool) -> None:
    """
    Mimic the original 'interpreted' rendering: 2x2 pixels -> one character.
    """
    assert s.img is not None

    charset = [" ", ".", ",", "_", "'", "|", "/", "J",
               "`", "\\", "|", "L", "\"", "7", "r", "o"]
    char_location = "X"
    char_sun = "S"
    char_sun_border = ":"
    char_track = "O"

    pix = s.img.load()

    for y in range(0, s.height - 1, 2):
        for x in range(0, s.width - 1, 2):
            if _screen_print_title(s, x, y):
                continue

            a = pix[x, y]
            b = pix[x + 1, y]
            c = pix[x, y + 1]
            d = pix[x + 1, y + 1]

            if (a == s.col_highlight or b == s.col_highlight or
                c == s.col_highlight or d == s.col_highlight):
                _print_color(s, SEQ_LOCATION)
                sys.stdout.write(char_location)
                _print_color(s, SEQ_RESET)
                continue

            sun_found = False
            is_sun_border = False
            is_track = False

            for i in range(NUM_TRACKS):
                ct = s.col_track[i]
                if a == ct or b == ct or c == ct or d == ct:
                    _print_color(s, SEQ_TRACK1 + i)
                    is_track = True
                    break

            if s.sun.active:
                if a == s.col_sun or b == s.col_sun or c == s.col_sun or d == s.col_sun:
                    sun_found = True
                    _print_color(s, SEQ_SUN)
                    sys.stdout.write(char_sun)
                    _print_color(s, SEQ_RESET)
                    continue
                elif (a == s.col_sun_border or b == s.col_sun_border or
                      c == s.col_sun_border or d == s.col_sun_border):
                    is_sun_border = True
                    _print_color(s, SEQ_SUN_BORDER)
                elif not is_track:
                    for i in range(NUM_SHADES):
                        sh = s.col_shade[i]
                        if a == sh or b == sh or c == sh or d == sh:
                            _print_color(s, SEQ_SHADE1 + i)
                            break

            # Not sun pixel: render line/track/border/land glyph.
            is_line = False
            if a == s.col_line or b == s.col_line or c == s.col_line or d == s.col_line:
                is_line = True
                _print_color(s, SEQ_RESET)
                _print_color(s, SEQ_LINE)

            if is_track:
                sys.stdout.write(char_track)
            elif is_sun_border:
                sys.stdout.write(char_sun_border)
            else:
                # In C: gd uses color index 0 as black; in our RGB image we use (0,0,0).
                glyph = ((a != s.col_black) << 3) | ((b != s.col_black) << 2) | ((c != s.col_black) << 1) | (d != s.col_black)
                sys.stdout.write(charset[glyph])

            if s.sun.active or is_line or is_track:
                _print_color(s, SEQ_RESET)

        if trailing_newline or y + 1 < s.height - 1:
            sys.stdout.write("\n")


def _draw_line(s: Screen, x1: float, y1: float, x2: float, y2: float) -> None:
    assert s.draw is not None
    s.draw.line((x1, y1, x2, y2), fill=s.brush)


def screen_draw_line_projected(s: Screen, lon1: float, lat1: float, lon2: float, lat2: float) -> None:
    x1, y1 = s.project(s, lon1, lat1)
    x2, y2 = s.project(s, lon2, lat2)
    if int(x1) == int(x2) and int(y1) == int(y2):
        return
    _draw_line(s, x1, y1, x2, y2)


def screen_draw_segment(s: Screen, x1: float, y1: float, x2: float, y2: float) -> None:
    # Avoid long wrap-around line segments, same as C heuristic.
    if abs(x1 - x2) < 0.1 * s.width and abs(y1 - y2) < 0.1 * s.height:
        _draw_line(s, x1, y1, x2, y2)


def screen_draw_spherical_circle(s: Screen, lon_deg: float, lat_deg: float, r_deg: float) -> None:
    steps = 1024

    # Choose a point on the small circle without crossing a pole.
    if lat_deg > 0:
        slat_deg = lat_deg - r_deg
    else:
        slat_deg = lat_deg + r_deg

    # Geographic -> spherical
    s_theta = -(lat_deg * DEG_2_RAD) + (90 * DEG_2_RAD)
    s_phi = lon_deg * DEG_2_RAD

    # Rotation axis in Cartesian
    rx = math.sin(s_theta) * math.cos(s_phi)
    ry = math.sin(s_theta) * math.sin(s_phi)
    rz = math.cos(s_theta)

    alpha = (360.0 / steps) * DEG_2_RAD

    ca = math.cos(alpha)
    sa = math.sin(alpha)
    one_ca = 1.0 - ca

    # Rotation matrix around (rx,ry,rz) by alpha
    m0 = rx * rx * one_ca + ca
    m1 = ry * rx * one_ca + rz * sa
    m2 = rz * rx * one_ca - ry * sa

    m3 = rx * ry * one_ca - rz * sa
    m4 = ry * ry * one_ca + ca
    m5 = rz * ry * one_ca + rx * sa

    m6 = rx * rz * one_ca + ry * sa
    m7 = ry * rz * one_ca - rx * sa
    m8 = rz * rz * one_ca + ca

    # initial vector
    s_theta = -(slat_deg * DEG_2_RAD) + (90 * DEG_2_RAD)
    s_phi = lon_deg * DEG_2_RAD
    px = math.sin(s_theta) * math.cos(s_phi)
    py = math.sin(s_theta) * math.sin(s_phi)
    pz = math.cos(s_theta)

    x1 = y1 = 0.0
    for i in range(steps + 1):
        # rotate p
        p2x = px * m0 + py * m3 + pz * m6
        p2y = px * m1 + py * m4 + pz * m7
        p2z = px * m2 + py * m5 + pz * m8

        p2z_fixed = max(-1.0, min(1.0, p2z))
        s_theta = math.acos(p2z_fixed)
        s_phi = math.atan2(p2y, p2x)

        lat2 = ((90 * DEG_2_RAD) - s_theta) * RAD_2_DEG
        lon2 = s_phi * RAD_2_DEG

        x2, y2 = s.project(s, lon2, lat2)
        if i >= 1:
            screen_draw_segment(s, x1, y1, x2, y2)
        x1, y1 = x2, y2

        px, py, pz = p2x, p2y, p2z


def poly_orientation(v: Sequence[float]) -> int:
    e1x = v[2] - v[0]
    e1y = v[3] - v[1]
    e2x = v[4] - v[2]
    e2y = v[5] - v[3]
    z = e1x * e2y - e1y * e2x
    return 1 if z > 0 else -1


def screen_draw_map(s: Screen, shp_path: str) -> bool:
    """
    Draw polygons from a shapefile. The original expects SHPT_POLYGON.
    pyshp uses shape.shapeType and parts.
    """
    assert s.draw is not None

    try:
        r = shapefile.Reader(shp_path)
    except Exception:
        print("Could not open shapefile", file=sys.stderr)
        return False

    # Polygon type: 5 (POLYGON) or 15/25 etc with Z/M variants.
    # Accept polygon and polygonZ/M.
    if r.shapeType not in (shapefile.POLYGON, shapefile.POLYGONZ, shapefile.POLYGONM):
        print("This is not a polygon file", file=sys.stderr)
        return False

    for i, shp in enumerate(r.iterShapes()):
        if shp.shapeType not in (shapefile.POLYGON, shapefile.POLYGONZ, shapefile.POLYGONM):
            print(f"Shape {i} is not a polygon", file=sys.stderr)
            return False

        pts = shp.points  # list[(x,y)]
        parts = list(shp.parts) + [len(pts)]

        # Iterate each part (ring) individually; attempt the same "hole" handling hack:
        # compute orientation estimate using successive triples, sum; only apply hole fill if multi-part.
        for p_i in range(len(parts) - 1):
            start = parts[p_i]
            end = parts[p_i + 1]
            ring = pts[start:end]
            if len(ring) < 3:
                continue

            proj_ring = [s.project(s, lon, lat) for (lon, lat) in ring]

            # orientation estimate in geographic coordinates
            ori = 0
            vori = [0.0] * 6
            for vi, (lon, lat) in enumerate(ring):
                if vi < 3:
                    vori[2 * vi] = lon
                    vori[2 * vi + 1] = lat
                else:
                    vori[0], vori[1] = vori[2], vori[3]
                    vori[2], vori[3] = vori[4], vori[5]
                    vori[4], vori[5] = lon, lat
                    ori += poly_orientation(vori)

            if s.solid_land:
                fill = s.col_black if (ori > 0 and len(shp.parts) > 1) else s.col_normal
                s.draw.polygon(proj_ring, outline=None, fill=fill)
            else:
                s.draw.polygon(proj_ring, outline=s.col_normal, fill=None)

    return True


def screen_mark_locations(s: Screen, file_path: str) -> bool:
    """
    Parse locations file in the same ad-hoc format as the C code:
      track
      <lat> <lon>
      ...
      .
      circles
      <lat> <lon> <r>
      ...
      .
      points
      <lat> <lon>
      ...
      .
    """
    assert s.draw is not None

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as fp:
            lines = fp.readlines()
    except Exception:
        print("Could not open locations file", file=sys.stderr)
        return False

    tracki = -1

    def parse_floats(line: str) -> List[float]:
        try:
            return [float(x) for x in line.strip().split()]
        except Exception:
            return []

    idx = 0
    # First pass: tracks and circles
    while idx < len(lines):
        line = lines[idx]
        idx += 1

        if line == "track\n":
            tracki = (tracki + 1) % NUM_TRACKS
            s.brush = s.col_track[tracki]

            x1 = y1 = x2 = y2 = 0.0
            first = True

            while idx < len(lines) and lines[idx] != ".\n":
                vals = parse_floats(lines[idx])
                idx += 1
                if len(vals) == 2:
                    lat, lon = vals
                    x2, y2 = s.project(s, lon, lat)
                    if not first:
                        screen_draw_segment(s, x1, y1, x2, y2)
                    x1, y1 = x2, y2
                    first = False
            if idx < len(lines) and lines[idx] == ".\n":
                idx += 1

        elif line == "circles\n":
            while idx < len(lines) and lines[idx] != ".\n":
                vals = parse_floats(lines[idx])
                idx += 1
                if len(vals) == 3:
                    lat, lon, r = vals
                    tracki = (tracki + 1) % NUM_TRACKS
                    s.brush = s.col_track[tracki]
                    screen_draw_spherical_circle(s, lon, lat, r)
            if idx < len(lines) and lines[idx] == ".\n":
                idx += 1

    # Second pass: points only
    s.brush = s.col_highlight
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        idx += 1
        if line.startswith("points"):
            while idx < len(lines) and lines[idx] != ".\n":
                vals = parse_floats(lines[idx])
                idx += 1
                if len(vals) == 2:
                    lat, lon = vals
                    x, y = s.project(s, lon, lat)
                    # Pixel set: draw a point
                    s.draw.point((x, y), fill=s.brush)
            if idx < len(lines) and lines[idx] == ".\n":
                idx += 1

    return True


def screen_mark_sun(s: Screen) -> None:
    assert s.draw is not None
    x, y = s.project(s, s.sun.lon, s.sun.lat)
    s.draw.point((x, y), fill=s.col_sun)


def screen_mark_sun_border(s: Screen) -> None:
    s.brush = s.col_sun_border
    screen_draw_spherical_circle(s, s.sun.lon, s.sun.lat, 90.0)


def screen_shade_map(s: Screen) -> None:
    """
    Shade the map by drawing patches colored by day/twilight/night, then
    overlaying those colors onto all non-black pixels, matching the C approach.
    """
    assert s.img is not None
    assert s.draw is not None

    overlay = Image.new("L", (s.width, s.height), 0)  # store shade index 0..NUM_SHADES-1
    od = ImageDraw.Draw(overlay)

    aspan = s.shade_steps_degree * DEG_2_RAD

    lambda_sun = s.sun.lon * DEG_2_RAD
    phi_sun = s.sun.lat * DEG_2_RAD

    # Render shade-index patches.
    lam = -180.0 * DEG_2_RAD
    while lam < 180.0 * DEG_2_RAD - aspan:
        phi = -90.0 * DEG_2_RAD
        while phi < 90.0 * DEG_2_RAD - aspan:
            # Great Circle Distance
            zeta = math.acos(
                math.sin(phi_sun) * math.sin(phi) +
                math.cos(phi_sun) * math.cos(phi) * math.cos(lam - lambda_sun)
            )

            d90 = zeta * RAD_2_DEG - 90.0
            d90 /= s.dusk_degree
            di90 = (NUM_SHADES - 1) - int(round(d90 * (NUM_SHADES - 1)))
            di90 = 0 if di90 < 0 else di90
            di90 = (NUM_SHADES - 1) if di90 > (NUM_SHADES - 1) else di90

            x0, y0 = s.project(s, lam * RAD_2_DEG, phi * RAD_2_DEG)
            x1, y1 = s.project(s, (lam + aspan) * RAD_2_DEG, phi * RAD_2_DEG)
            x2, y2 = s.project(s, (lam + aspan) * RAD_2_DEG, (phi + aspan) * RAD_2_DEG)
            x3, y3 = s.project(s, lam * RAD_2_DEG, (phi + aspan) * RAD_2_DEG)

            od.polygon([(x0, y0), (x1, y1), (x2, y2), (x3, y3)], fill=int(di90))
            phi += aspan
        lam += aspan

    # Apply overlay shade colors to all non-black pixels.
    base = s.img.load()
    ov = overlay.load()
    for y in range(s.height):
        for x in range(s.width):
            if base[x, y] != s.col_black:
                idx = ov[x, y]
                # idx should be 0..NUM_SHADES-1; map to shade color
                base[x, y] = s.col_shade[int(idx)]


def screen_draw_world_border(s: Screen) -> None:
    steps = 128
    s.brush = s.col_line

    for i in range(steps):
        screen_draw_line_projected(s, -179.99999, (i / steps) * 180.0 - 90.0,
                                   -179.99999, ((i + 1) / steps) * 180.0 - 90.0)
    for i in range(steps):
        screen_draw_line_projected(s, 179.99999, (i / steps) * 180.0 - 90.0,
                                   179.99999, ((i + 1) / steps) * 180.0 - 90.0)
    for i in range(steps):
        screen_draw_line_projected(s, (i / steps) * 360.0 - 180.0, -89.99999,
                                   ((i + 1) / steps) * 360.0 - 180.0, -89.99999)
    for i in range(steps):
        screen_draw_line_projected(s, (i / steps) * 360.0 - 180.0, 89.99999,
                                   ((i + 1) / steps) * 360.0 - 180.0, 89.99999)


def _get_terminal_size_fallback() -> Tuple[int, int]:
    # columns, rows
    try:
        sz = os.get_terminal_size(sys.stdout.fileno())
        return sz.columns, sz.lines
    except Exception:
        return 80, 24


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    # Match original single-letter flags.
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--help", action="help", help="Show this help message and exit")
    p.add_argument("-w", type=int, dest="width", help="Width (terminal columns)")
    p.add_argument("-h", type=int, dest="height", help="Height (terminal rows)")
    p.add_argument("-m", dest="map", default=DEFAULT_MAP, help="Polygon shapefile")
    p.add_argument("-l", dest="locations", help="Locations file (tracks/circles/points)")
    p.add_argument("-s", dest="sun", action="store_true", help="Enable sun shading")
    p.add_argument("-S", dest="no_sun_markers", action="store_true", help="Disable sun markers")
    p.add_argument("-T", dest="no_trailing_newline", action="store_true", help="No trailing newline")
    p.add_argument("-p", dest="projection", choices=["kav", "lam", "ham"], help="Projection: kav|lam|ham")
    p.add_argument("-b", dest="world_border", action="store_true", help="Draw world border")
    p.add_argument("-c", dest="colors", type=int, choices=[0, 8, 256], help="Colors: 0(off), 8, 256(default)")
    p.add_argument("-o", dest="solid_land", action="store_true", help="Solid land fill")
    p.add_argument("-d", dest="dusk", choices=["civil", "nautical", "astronomical", "civ", "nau", "ast"],
                   help="Dusk degree: civil(6), nautical(12), astronomical(18)")
    p.add_argument("-W", dest="outimg", help="Write PNG instead of ASCII")
    p.add_argument("-t", dest="title", help="Title text (rendered at top)")
    ns = p.parse_args(list(argv))

    cols, rows = _get_terminal_size_fallback()
    if ns.width is not None:
        cols = ns.width
    if ns.height is not None:
        rows = ns.height

    s = Screen()
    s.title = ns.title
    s.world_border = bool(ns.world_border)
    s.solid_land = bool(ns.solid_land)

    if ns.colors == 0:
        s.disable_colors = True
    elif ns.colors == 8:
        s.esc_seq = SEQ_8COLORS
    else:
        s.esc_seq = SEQ_256COLORS

    if ns.dusk:
        key = ns.dusk[:3].lower()
        if key == "nau":
            s.dusk_degree = 12.0
        elif key == "ast":
            s.dusk_degree = 18.0
        else:
            s.dusk_degree = 6.0

    if ns.projection:
        if ns.projection == "kav":
            s.project = project_kavrayskiy
        elif ns.projection == "lam":
            s.project = project_lambert
        elif ns.projection == "ham":
            s.project = project_hammer

    if ns.sun:
        s.sun.active = True
        calc_sun(s.sun)

    sun_markers = not ns.no_sun_markers
    trailing_newline = not ns.no_trailing_newline

    # Match C sizing trick: ASCII mode uses 2x raster to get 2x2->1 char.
    scale = 1 if ns.outimg else 2
    if not screen_init_img(s, scale * cols, scale * rows):
        return 1

    if not screen_draw_map(s, ns.map):
        return 1

    if s.sun.active:
        screen_shade_map(s)
        if sun_markers:
            screen_mark_sun_border(s)

    if s.world_border:
        screen_draw_world_border(s)

    if ns.locations:
        if not screen_mark_locations(s, ns.locations):
            return 1

    if s.sun.active and sun_markers:
        screen_mark_sun(s)

    if ns.outimg:
        assert s.img is not None
        try:
            s.img.save(ns.outimg, format="PNG")
        except Exception as e:
            print(f"Opening output file failed: {e}", file=sys.stderr)
            return 1
    else:
        screen_show_interpreted(s, trailing_newline)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
