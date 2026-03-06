#!/usr/bin/env python3
"""
Oxford Suites Dashboard Builder
- Connects to SFTP, downloads any new CSV files
- Parses all archived CSVs to compute monthly + daily consumption
- Injects updated data into index.html
- GitHub Actions runs this daily at 06:00 UTC
"""
import os, csv, re, json, logging, hashlib, shutil
from pathlib import Path
from datetime import date, datetime
from collections import defaultdict

try:
    import paramiko
except ImportError:
    paramiko = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SFTP_HOST = os.environ.get("SFTP_HOST", "82.25.83.153")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "65002"))
SFTP_USER = os.environ.get("SFTP_USER", "")
SFTP_PASS = os.environ.get("SFTP_PASS", "")
SFTP_REMOTE_DIR = "/meter_uploads/"

REPO_ROOT   = Path(__file__).parent.parent
ARCHIVE_DIR = REPO_ROOT / "data" / "archive"
INDEX_HTML  = REPO_ROOT / "index.html"
TEMPLATE    = REPO_ROOT / "index.template.html"
METER_MAP   = REPO_ROOT / "data" / "meter_map.json"

ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# ── CSV column indices ─────────────────────────────────────────────────────────
COL_SERIAL  = 2   # device-identification
COL_CUR_VAL = 13  # current cumulative volume m³

# Snapshot pairs [value_col, date_col] — indices 0..7
SNAP_PAIRS = [(15,16),(17,18),(19,20),(21,22),(23,24),(25,26),(27,28),(29,30)]

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_float(s):
    try:
        return float(str(s).replace(",", ".").strip())
    except:
        return None

def file_date(fname):
    m = re.search(r"_(\d{8})\d{6}_", fname)
    if m:
        d = m.group(1)
        return date(int(d[:4]), int(d[4:6]), int(d[6:8]))
    return None

def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

# ── Step 1: SFTP sync ─────────────────────────────────────────────────────────
def sftp_sync():
    if not paramiko:
        log.warning("paramiko not installed — skipping SFTP sync")
        return
    if not SFTP_USER:
        log.warning("No SFTP credentials — skipping sync")
        return

    log.info(f"Connecting to SFTP {SFTP_HOST}:{SFTP_PORT}")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Probe remote dir
    remote_dir = None
    for candidate in [SFTP_REMOTE_DIR, f"/home/{SFTP_USER}/meter_uploads/", "/"]:
        try:
            entries = sftp.listdir(candidate)
            remote_dir = candidate.rstrip("/") + "/"
            log.info(f"Remote dir: {remote_dir} ({len(entries)} entries)")
            break
        except:
            pass
    if not remote_dir:
        log.error("Could not find remote directory")
        sftp.close(); transport.close()
        return

    # Download new/changed CSV files
    new_count = 0
    for entry in sftp.listdir_attr(remote_dir):
        fname = entry.filename
        if not fname.endswith(".csv"):
            continue
        local_path = ARCHIVE_DIR / fname
        remote_path = remote_dir + fname
        if local_path.exists() and local_path.stat().st_size == entry.st_size:
            continue
        log.info(f"  Downloading {fname}")
        sftp.get(remote_path, str(local_path))
        new_count += 1

    sftp.close(); transport.close()
    log.info(f"SFTP sync done — {new_count} new files")

# ── Step 2: Load meter map ─────────────────────────────────────────────────────
def load_meter_map():
    if not METER_MAP.exists():
        log.error(f"Meter map not found at {METERE_MAP}")
        return {}, {}
    with open(METER_MAP) as f:
        meter_list = json.load(f)
    serial_to_unit = {}
    for u in meter_list:
        serial_to_unit[u["dh_s"]] = (u["u"], "dh")
        serial_to_unit[u["dc_s"]] = (u["u"], "dc")
    log.info(f"Meter map: {len(meter_list)} units, {len(serial_to_unit)} serials")
    return meter_list, serial_to_unit
