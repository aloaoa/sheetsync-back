# main.py
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
from io import BytesIO, StringIO
import requests, time, os, sqlite3
from hashlib import sha256
from pathlib import Path

# ────────────────────────────────────────────────────────────────────────────────
# .env & paths
# ────────────────────────────────────────────────────────────────────────────────
from dotenv import load_dotenv
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BRIDGE_SECRET = os.getenv("BRIDGE_SECRET", "change-me")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN", "")

HUBSPOT_API_BASE = "https://api.hubapi.com"

# ────────────────────────────────────────────────────────────────────────────────
# App / CORS
# ────────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="SheetSync AI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ────────────────────────────────────────────────────────────────────────────────
# SQLite (idempotency + logs)
# ────────────────────────────────────────────────────────────────────────────────
DB_PATH = BASE_DIR / "sheetsync.db"

def ensure_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        spreadsheet_id TEXT,
        sheet_name TEXT,
        row_index INTEGER,
        row_hash TEXT,
        hubspot_id TEXT,
        action TEXT,
        detail TEXT,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    """)
    return conn

DB = ensure_db()

def row_hash(headers: List[str], values: List[Any]) -> str:
    h = sha256()
    h.update(("|".join([str(x) for x in headers])).encode("utf-8"))
    h.update(("|".join([str(x) for x in values])).encode("utf-8"))
    return h.hexdigest()

def already_processed(spreadsheet_id: str, sheet_name: str, row_index: int, r_hash: str) -> bool:
    cur = DB.execute(
        "SELECT 1 FROM events WHERE spreadsheet_id=? AND sheet_name=? AND row_index=? AND row_hash=? LIMIT 1",
        (spreadsheet_id, sheet_name, row_index, r_hash)
    )
    return cur.fetchone() is not None

def log_event(spreadsheet_id: str, sheet_name: str, row_index: int, r_hash: str,
              action: str, hubspot_id: Optional[str] = "", detail: str = ""):
    DB.execute(
        "INSERT INTO events (spreadsheet_id, sheet_name, row_index, row_hash, hubspot_id, action, detail) "
        "VALUES (?,?,?,?,?,?,?)",
        (spreadsheet_id, sheet_name, row_index, r_hash, hubspot_id or "", action, detail[:2000])
    )
    DB.commit()

# ────────────────────────────────────────────────────────────────────────────────
# Health / env / logs
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/")
def home():
    return {"message": "SheetSync AI API running"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/env-check")
def env_check():
    return {
        "has_hubspot_token": bool(HUBSPOT_ACCESS_TOKEN),
        "has_bridge_secret": bool(BRIDGE_SECRET),
    }

@app.get("/logs/recent")
def logs_recent(limit: int = 30):
    cur = DB.execute(
        "SELECT spreadsheet_id, sheet_name, row_index, hubspot_id, action, detail, ts "
        "FROM events ORDER BY id DESC LIMIT ?",
        (limit,)
    )
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {"ok": True, "events": rows}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers: cleaning + header mapping
# ────────────────────────────────────────────────────────────────────────────────
HEADER_ALIASES = {
    "email": {"email", "e-mail", "mail"},
    "firstname": {"first name", "firstname", "first_name", "given name"},
    "lastname": {"last name", "lastname", "last_name", "surname"},
    "phone": {"phone", "phone number", "mobile", "mobile phone"},
    "company": {"company", "account", "organisation", "organization"},
}

def _norm(s: str) -> str:
    return "".join(ch for ch in str(s).strip().lower() if ch.isalnum())

def map_row_to_contact(headers: List[str],
                       values: List[Optional[str]],
                       mapping: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    vals = ["" if v is None else str(v) for v in values]
    contact = {"email": "", "firstname": "", "lastname": "", "phone": "", "company": ""}

    if mapping:  # explicit mapping from sidebar
        lower_map = {k: _norm(v) for k, v in mapping.items() if k in contact}
        hnorm = [_norm(h) for h in headers]
        for prop, wanted in lower_map.items():
            if wanted in hnorm:
                idx = hnorm.index(wanted)
                if idx < len(vals): contact[prop] = vals[idx]
    else:
        # heuristic aliases
        hnorm = [_norm(h) for h in headers]
        for prop, aliases in HEADER_ALIASES.items():
            for i, h in enumerate(hnorm):
                if h in {_norm(a) for a in aliases}:
                    if i < len(vals): contact[prop] = vals[i]
                    break

    contact["email"] = contact["email"].strip().lower()
    # drop empties so we don't blank HubSpot fields
    return {k: v for k, v in contact.items() if v}

# ────────────────────────────────────────────────────────────────────────────────
# HubSpot client with retries
# ────────────────────────────────────────────────────────────────────────────────
def _hs_headers() -> Dict[str, str]:
    if not HUBSPOT_ACCESS_TOKEN:
        raise HTTPException(status_code=400, detail="HUBSPOT_ACCESS_TOKEN missing")
    return {"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}", "Content-Type": "application/json"}

def _request_retry(method: str, url: str, **kw) -> requests.Response:
    # backoff: 0.5, 1, 2, 4 (seconds)
    for attempt in range(5):
        r = requests.request(method, url, timeout=30, **kw)
        if r.status_code < 500 and r.status_code != 429:
            return r
        time.sleep(min(0.5 * (2 ** attempt), 6))
    return r

def hubspot_find_contact_by_email(email: str) -> Optional[str]:
    url = f"{HUBSPOT_API_BASE}/crm/v3/objects/contacts/search"
    payload = {"filterGroups":[{"filters":[{"propertyName":"email","operator":"EQ","value":email}]}],
               "properties":["email"]}
    r = _request_retry("POST", url, headers=_hs_headers(), json=payload)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"HubSpot search error: {r.text}")
    results = r.json().get("results", [])
    return results[0].get("id") if results else None

def hubspot_create_contact(props: Dict[str, Any]) -> str:
    url = f"{HUBSPOT_API_BASE}/crm/v3/objects/contacts"
    r = _request_retry("POST", url, headers=_hs_headers(), json={"properties": props})
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"HubSpot create error: {r.text}")
    return r.json().get("id", "")

def hubspot_update_contact(contact_id: str, props: Dict[str, Any]) -> None:
    url = f"{HUBSPOT_API_BASE}/crm/v3/objects/contacts/{contact_id}"
    r = _request_retry("PATCH", url, headers=_hs_headers(), json={"properties": props})
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"HubSpot update error: {r.text}")

def upsert_contact_to_hubspot(contact: Dict[str, Any]) -> Dict[str, Any]:
    email = (contact.get("email") or "").strip().lower()
    if not email:
        return {"skipped": True, "reason": "missing email"}
    props = {k: v for k, v in contact.items() if v not in (None, "")}
    cid = hubspot_find_contact_by_email(email)
    if cid:
        hubspot_update_contact(cid, props)
        return {"updated": True, "id": cid}
    else:
        new_id = hubspot_create_contact(props)
        return {"created": True, "id": new_id}

# ────────────────────────────────────────────────────────────────────────────────
# File preview endpoints (unchanged, handy for manual tests)
# ────────────────────────────────────────────────────────────────────────────────
@app.post("/preview")
async def preview_endpoint(file: UploadFile = File(...), mapping: str = Form(...)):
    data = await file.read()
    return {"ok": True, "filename": file.filename, "bytes": len(data), "mapping_sample": mapping[:120]}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), mapping: str = Form(...)):
    content = await file.read()
    try:
        if file.filename.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(content))
        elif file.filename.lower().endswith(".csv"):
            df = pd.read_csv(StringIO(content.decode("utf-8", errors="ignore")))
        else:
            return JSONResponse({"error": "Unsupported file type"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": f"Error reading file: {e}"}, status_code=400)
    return {"filename": file.filename, "preview": {"columns": list(map(str, df.columns))}}

# ────────────────────────────────────────────────────────────────────────────────
# Google Sheets row ingest
# ────────────────────────────────────────────────────────────────────────────────
# ---- model for single-row ingest ----
class IngestRowPayload(BaseModel):
    spreadsheetId: str
    sheetName: str
    rowIndex: int
    headers: Optional[List[str]] = None
    values: List[Optional[str]]
    mapping: Optional[Dict[str, str]] = None

# ---- single-row ingest endpoint ----
@app.post("/ingest/rows")
def ingest_rows(payload: IngestRowPayload, x_bridge_secret: str = Header(None)):
    if x_bridge_secret != BRIDGE_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    headers = payload.headers or []
    values  = payload.values or []

    r_hash = row_hash(headers, values)
    if already_processed(payload.spreadsheetId, payload.sheetName, payload.rowIndex, r_hash):
        log_event(payload.spreadsheetId, payload.sheetName, payload.rowIndex, r_hash, "duplicate")
        return {"ok": True, "duplicate": True}

    try:
        contact = map_row_to_contact(headers, values, payload.mapping)
        if not contact.get("email"):
            log_event(payload.spreadsheetId, payload.sheetName, payload.rowIndex, r_hash,
                      "skipped", "", "missing email")
            return {"ok": True, "skipped": True, "reason": "missing email"}

        res = upsert_contact_to_hubspot(contact)
        action = "updated" if res.get("updated") else "created" if res.get("created") else "unknown"
        log_event(payload.spreadsheetId, payload.sheetName, payload.rowIndex, r_hash,
                  action, res.get("id", ""), str(contact))
        return {"ok": True, "upsert": res}

    except Exception as e:
        log_event(payload.spreadsheetId, payload.sheetName, payload.rowIndex, r_hash, "error", "", repr(e))
        raise
