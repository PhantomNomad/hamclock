"""Minimal ADIF (.adi) reader and writer for hamclock.

Reads ADI files from any logger (eQSL, LoTW, QRZ, Log4OM, WSJT-X, ...),
including a .zip that contains one. Writes ADIF 3.1 ADI.

Field lengths in ADI are counted in bytes, so parsing works on the raw bytes
and each value is decoded afterwards (UTF-8, falling back to Latin-1).
"""

import io
import re
import zipfile
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

ADIF_VERSION = "3.1.4"

_TAG_RE = re.compile(rb"<([A-Za-z0-9_]+)(?::(\d+))?(?::([A-Za-z]))?>")


def _decode(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def parse_adif(data: bytes) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """Parse ADI bytes into (header fields, list of records).

    Field names are upper-cased. Unknown/extra fields are kept as-is.
    """
    header: Dict[str, str] = {}
    records: List[Dict[str, str]] = []
    current: Dict[str, str] = {}
    pos = 0
    in_header = not data.lstrip().startswith(b"<")  # free-text preamble => header first
    preamble_end = None

    while True:
        m = _TAG_RE.search(data, pos)
        if not m:
            break
        name = m.group(1).decode("ascii").upper()
        length = m.group(2)
        pos = m.end()
        if preamble_end is None:
            preamble_end = m.start()
        if length is None:
            if name == "EOH":
                header.update(current)
                current = {}
                in_header = False
            elif name == "EOR":
                if current:
                    records.append(current)
                current = {}
                in_header = False
            continue
        n = int(length)
        value = _decode(data[pos:pos + n])
        pos += n
        current[name] = value

    if current and not in_header:
        records.append(current)
    elif current:
        header.update(current)

    if preamble_end:
        header.setdefault("_PREAMBLE", _decode(data[:preamble_end]).strip())
    return header, records


def read_adif_file(path: str) -> Tuple[Dict[str, str], List[Dict[str, str]], str]:
    """Read an .adi/.adif file, or the first .adi/.adif inside a .zip.

    Returns (header, records, name_of_file_read).
    """
    with open(path, "rb") as f:
        data = f.read()
    if data[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            names = [n for n in z.namelist() if n.lower().endswith((".adi", ".adif")) and not n.endswith("/")]
            if not names:
                raise ValueError("No .adi file inside that zip")
            info = z.getinfo(names[0])
            if info.file_size > 200 * 1024 * 1024:
                raise ValueError("ADIF file inside zip is unreasonably large")
            header, records = parse_adif(z.read(names[0]))
            return header, records, names[0]
    header, records = parse_adif(data)
    return header, records, path


def format_field(name: str, value) -> str:
    if value is None:
        return ""
    text = str(value)
    if text == "":
        return ""
    return f"<{name.upper()}:{len(text.encode('utf-8'))}>{text}"


def write_adif(records: Iterable[Dict[str, str]], program_id: str = "hamclock",
               comment: str = "") -> str:
    """Return ADI text for the given records (dicts of FIELD -> value)."""
    now = datetime.now(timezone.utc)
    lines = [
        comment or f"ADIF export from {program_id}",
        f"Generated {now.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        format_field("ADIF_VER", ADIF_VERSION),
        format_field("PROGRAMID", program_id),
        format_field("CREATED_TIMESTAMP", now.strftime("%Y%m%d %H%M%S")),
        "<EOH>",
        "",
    ]
    for rec in records:
        parts = [format_field(k, v) for k, v in rec.items()]
        lines.append("".join(p for p in parts if p) + "<EOR>")
    return "\n".join(lines) + "\n"


def fields_to_string(fields: Dict[str, str]) -> str:
    """Pack fields into an ADI fragment (used to keep fields with no DB column)."""
    return "".join(format_field(k, v) for k, v in fields.items())


def string_to_fields(text: str) -> Dict[str, str]:
    if not text:
        return {}
    _, records = parse_adif((text + "<EOR>").encode("utf-8"))
    return records[0] if records else {}
