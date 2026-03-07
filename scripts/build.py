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
        log.error(f"Meter map not found at {METER_MAP}")
        return {}, {}
    with open(METER_MAP) as f:
        meter_list = json.load(f)
    serial_to_unit = {}
    for u in meter_list:
        serial_to_unit[u["dh_s"]] = (u["u"], "dh")
        serial_to_unit[u["dc_s"]] = (u["u"], "dc")
    log.info(f"Meter map: {len(meter_list)} units, {len(serial_to_unit)} serials")
    return meter_list, serial_to_unit

# ── Step 3: Parse all CSVs ────────────────────────────────────────────────────
def parse_csvs(serial_to_unit):
    """Returns:
       snapshot_data: serial -> {snap_date_str: cumulative_value}  (from meter snapshots)
       daily_data:    serial -> {date_str: cumulative_value}        (from file timestamps)
    """
    snapshot_data = defaultdict(dict)   # from columns 15-30
    daily_data    = defaultdict(dict)   # from file date + col 13

    files = sorted(ARCHIVE_DIR.glob("*.csv"))
    log.info(f"Parsing {len(files)} CSV files...")

    for fpath in files:
        fdate = file_date(fpath.name)

        with open(fpath, encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if len(row) < 31 or row[0] == "#serial-number":
                    continue
                serial = str(row[COL_SERIAL]).strip()
                if serial not in serial_to_unit:
                    continue

                # Current cumulative (for daily timeline)
                cur_val = parse_float(row[COL_CUR_VAL])
                if cur_val is not None and fdate:
                    dstr = str(fdate)
                    # Take max value seen on this date (latest file wins)
                    if dstr not in daily_data[serial] or cur_val > daily_data[serial][dstr]:
                        daily_data[serial][dstr] = cur_val

                # Snapshot pairs (historical monthly snapshots)
                for val_col, date_col in SNAP_PAIRS:
                    if val_col >= len(row) or date_col >= len(row):
                        continue
                    snap_val  = parse_float(row[val_col])
                    snap_date = str(row[date_col]).strip()[:10]  # YYYY-MM-DD
                    if snap_val is not None and re.match(r"\d{4}-\d{2}-\d{2}", snap_date):
                        # Keep max value seen for this snap_date
                        if snap_date not in snapshot_data[serial] or snap_val > snapshot_data[serial][snap_date]:
                            snapshot_data[serial][snap_date] = snap_val

    log.info(f"  snapshot_data: {len(snapshot_data)} serials")
    log.info(f"  daily_data:    {len(daily_data)} serials")
    return snapshot_data, daily_data

# ── Step 4: Compute unit data ─────────────────────────────────────────────────
KNOWN_SNAP_DATES = [
    "2025-09-30",  # 0 baseline
    "2025-10-31",  # 1
    "2025-11-30",  # 2
    "2025-12-31",  # 3
    "2026-01-01",  # 4 YTD start
    "2026-01-31",  # 5
    "2026-02-28",  # 6
]
MONTHS_LABELS = ["Oct '25", "Nov '25", "Dec '25", "Jan '26", "Feb '26"]

def build_unit_data(meter_list, serial_to_unit, snapshot_data, daily_data):
    today = date.today()
    current_month_snap = str(today)  # latest file date = MTD snapshot

    units_out = []
    for u in meter_list:
        uid = u["u"]
        dh_s = u["dh_s"]
        dc_s = u["dc_s"]

        # Collect snapshots for this unit
        def get_snaps(serial):
            snaps = snapshot_data.get(serial, {})
            daily = daily_data.get(serial, {})
            result = []
            for d in KNOWN_SNAP_DATES:
                result.append(snaps.get(d) or daily.get(d))

            # Add current MTD: latest daily reading
            if daily:
                latest_date = max(daily.keys())
                result.append(daily[latest_date])
            else:
                result.append(None)

            return result, (max(daily.keys()) if daily else None)

        dh_snaps, dh_latest_date = get_snaps(dh_s)
        dc_snaps, dc_latest_date = get_snaps(dc_s)

        all_snap_dates = KNOWN_SNAP_DATES + [str(dh_latest_date or today)]

        def deltas(snaps):
            # 5 monthly periods + current MTD
            # Oct = snap[1]-snap[0], Nov = snap[2]-snap[1], ...
            # Jan = snap[5]-snap[4] (skip Jan1 -> use billing year)
            # Feb = snap[6]-snap[5]
            # Mar MTD = snap[7]-snap[6]
            out = []
            pairs = [(0,1),(1,2),(2,3),(4,5),(5,6),(6,7)]
            for a, b in pairs:
                va = snaps[a] if snaps[a] is not None else 0
                vb = snaps[b] if snaps[b] is not None else 0
                out.append(round(max(0, vb - va), 4))
            return out

        dh_d = deltas(dh_snaps)
        dc_d = deltas(dc_snaps)

        # YTD from Jan 1
        dh_cur = round(max(0, (dh_snaps[7] or 0) - (dh_snaps[4] or 0)), 4)
        dc_cur = round(max(0, (dc_snaps[7] or 0) - (dc_snaps[4] or 0)), 4)

        # Daily deltas
        def daily_deltas(serial):
            daily = daily_data.get(serial, {})
            if not daily:
                return {}
            sorted_dates = sorted(daily.keys())
            result = {}
            for i in range(1, len(sorted_dates)):
                d0, d1 = sorted_dates[i-1], sorted_dates[i]
                delta = round(max(0, daily[d1] - daily[d0]), 4)
                result[d1] = delta
            return result

        dh_daily = daily_deltas(dh_s)
        dc_daily = daily_deltas(dc_s)
        daily_merged = {}
        all_days = sorted(set(dh_daily) | set(dc_daily))
        for d in all_days:
            daily_merged[d] = {"dh": dh_daily.get(d, 0), "dc": dc_daily.get(d, 0)}

        # Cumulative arrays (for YTD line chart)
        dh_cum = [round(v, 4) if v is not None else None for v in dh_snaps]
        dc_cum = [round(v, 4) if v is not None else None for v in dc_snaps]

        units_out.append({
            "u": uid,
            "f": u["f"],
            "dh": dh_d,
            "dc": dc_d,
            "dh_cur": dh_cur,
            "dc_cur": dc_cur,
            "dh_s": dh_s,
            "dc_s": dc_s,
            "snap_dates": all_snap_dates,
            "dh_cum": dh_cum,
            "dc_cum": dc_cum,
            "daily": daily_merged,
        })

    log.info(f"Built data for {len(units_out)} units")
    return units_out

# ── Step 5: Inject into index.html ────────────────────────────────────────────
def build_html(units_out):
    if not TEMPLATE.exists():
        log.error(f"Template not found: {TEMPLATE}")
        return

    with open(TEMPLATE, "r", encoding="utf-8") as f:
        html = f.read()

    today = date.today()
    latest_date = max(
        (max(u["daily"].keys()) for u in units_out if u["daily"]),
        default=str(today)
    )

    payload = json.dumps({
        "units": units_out,
        "status": "ok",
        "stale": False,
        "last_sync": latest_date,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }, separators=(",", ":"))

    # Replace placeholder in template
    html = html.replace("__SEED_DATA_JSON__", payload)

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"Built {INDEX_HTML} ({INDEX_HTML.stat().st_size:,} bytes)")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=== Oxford Dashboard Build ===")
    sftp_sync()
    meter_list, serial_to_unit = load_meter_map()
    if not meter_list:
        log.error("No meter map — aborting")
        exit(1)
    snapshot_data, daily_data = parse_csvs(serial_to_unit)
    units_out = build_unit_data(meter_list, serial_to_unit, snapshot_data, daily_data)
    build_html(units_out)
    log.info("=== Done ===")
