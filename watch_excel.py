# watch_excel.py
import os
import time
import json
import shutil
import requests
import pandas as pd
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

print("SCRIPT:", __file__)
print("VERSION: tempcopy-before-suffix-v2")

# ── Load env (backend/.env if you run from backend) ────────────────────────────
load_dotenv()

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/ingest/rows")   # set to your ngrok HTTPS in .env
SECRET  = os.getenv("BRIDGE_SECRET", "change-me")

# 👇 Point to your real file (CSV or XLSX)
WATCH_FILE = Path(r"C:\Users\toufi\Downloads\sample_contacts.csv").resolve()

# Tuning
READ_RETRIES     = 40    # total attempts while file is locked
RETRY_DELAY_SECS = 0.75  # wait between attempts
DEBOUNCE_SECS    = 1.0   # ignore duplicate bursts

WATCH_DIR   = WATCH_FILE.parent
TARGET_NAME = WATCH_FILE.name
_last_event = 0.0


def wait_until_stable(path: Path, checks: int = 3, interval: float = 0.3) -> bool:
    """Wait until file size stays the same for `checks` intervals (file finished saving)."""
    last_size = -1
    stable = 0
    for _ in range(READ_RETRIES):
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            time.sleep(interval)
            continue
        if size == last_size and size > 0:
            stable += 1
            if stable >= checks:
                return True
        else:
            stable = 0
            last_size = size
        time.sleep(interval)
    return False


def copy_to_temp(path: Path) -> Path | None:
    """
    Copy to a temp path next to the source (to bypass file locks).
    IMPORTANT: keep the original suffix at the end so pandas detects the type.
      e.g.  sample_contacts.csv -> sample_contacts.tmpcopy.csv
            data.xlsx          -> data.tmpcopy.xlsx
    """
    if not wait_until_stable(path):
        return None
    temp_path = path.with_name(f"{path.stem}.tmpcopy{path.suffix}")
    for _ in range(READ_RETRIES):
        try:
            shutil.copyfile(path, temp_path)
            return temp_path
        except Exception:
            time.sleep(RETRY_DELAY_SECS)
    return None


def read_table_any(path: Path) -> pd.DataFrame | None:
    """Read CSV or XLSX from a temp copy to avoid lock/partial writes."""
    tmp = copy_to_temp(path)
    if tmp is None:
        return None
    try:
        suf = tmp.suffix.lower()
        if suf in (".xlsx", ".xls"):
            df = pd.read_excel(tmp, engine="openpyxl")
        elif suf == ".csv":
            # tolerate odd encodings; fallback to python engine if needed
            try:
                df = pd.read_csv(tmp, encoding="utf-8", errors="ignore")
            except Exception:
                df = pd.read_csv(tmp, engine="python")
        else:
            print(f"⚠️ Unsupported file type: {suf}")
            return None
        return df
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def send_first_row(df: pd.DataFrame):
    """Send header + first data row to the API (/ingest/rows)."""
    if df.empty:
        print("⚠️ No rows to send.")
        return

    df2 = df.fillna("").astype(str)
    headers = list(map(str, df2.columns.tolist()))
    row     = df2.iloc[0].tolist()

    payload = {
        "spreadsheetId": "excel-desktop",
        "sheetName": "Sheet1",
        "rowIndex": 0,
        "headers": headers,   # include headers for backend auto-mapping
        "values": row
    }

    try:
        r = requests.post(
            API_URL,
            headers={"X-Bridge-Secret": SECRET, "Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=30
        )
        print(f"📤 POST {API_URL} → {r.status_code}")
        if r.status_code >= 400:
            print("💥 Error body:", r.text[:500])
        else:
            try:
                print("✅ Server:", r.json())
            except Exception:
                print("✅ Server (text):", r.text[:200])
    except Exception as e:
        print("💥 Request failed:", repr(e))


def handle_change(path: Path):
    """Debounce, read and send when target file changes."""
    global _last_event
    now = time.monotonic()
    if now - _last_event < DEBOUNCE_SECS:
        return
    _last_event = now

    print(f"🔎 Change detected: {path}")
    df = read_table_any(path)
    if df is None:
        print("❌ Could not read file (locked/partial/unsupported).")
        return
    send_first_row(df)


class FileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and Path(event.src_path).name == TARGET_NAME:
            handle_change(WATCH_FILE)

    def on_created(self, event):
        if not event.is_directory and Path(event.src_path).name == TARGET_NAME:
            handle_change(WATCH_FILE)

    def on_moved(self, event):
        dest = Path(getattr(event, "dest_path", event.src_path))
        if not event.is_directory and dest.name == TARGET_NAME:
            handle_change(WATCH_FILE)


def main():
    print("Watching folder:", WATCH_DIR)
    print("Target file     :", TARGET_NAME)
    print("API_URL         :", API_URL)
    print("BRIDGE_SECRET   :", "*** set ***" if SECRET and SECRET != "change-me" else "⚠️ NOT SET")

    obs = Observer()
    obs.schedule(FileHandler(), str(WATCH_DIR), recursive=False)
    obs.start()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()


if __name__ == "__main__":
    main()
