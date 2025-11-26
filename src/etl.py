import os
import sqlite3
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd

# ==== PATH CONFIG ====
BASE_DIR   = Path(__file__).resolve().parent.parent
DATA_DIR   = BASE_DIR / "data"
RAW_DIR    = DATA_DIR / "raw"
DWH_DIR    = DATA_DIR / "dwh"

CSV_PATH   = RAW_DIR / "log_parsed.csv"
DB_PATH    = DWH_DIR / "mini_dwh.db"
SCHEMA_SQL = BASE_DIR / "src" / "schema.sql"
OUT_CSV    = DWH_DIR / "dwh_requests.csv"


# ==== HELPER ====

def init_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DWH_DIR.mkdir(parents=True, exist_ok=True)


def init_db():
    """Tạo DB mới và apply schema.sql"""
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        with open(SCHEMA_SQL, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()


def load_staging():
    """Nạp CSV vào bảng stg_logs"""
    df = pd.read_csv(CSV_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("stg_logs", conn, if_exists="append", index=False)


def run_etl():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT row_id, time, method, url, status, mimeType, wait_ms FROM stg_logs")
    rows = cur.fetchall()

    def record_issue(stg_row_id, issue_type, detail):
        cur.execute(
            "INSERT INTO dq_issues (stg_row_id, issue_type, detail) VALUES (?, ?, ?)",
            (stg_row_id, issue_type, detail),
        )

    def get_or_create_dim_time(ts: str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None
        date_str = dt.date().isoformat()
        hour = dt.hour
        minute = dt.minute

        cur.execute(
            "INSERT OR IGNORE INTO dim_time (ts, date, hour, minute) VALUES (?, ?, ?, ?)",
            (ts, date_str, hour, minute),
        )
        cur.execute("SELECT time_id FROM dim_time WHERE ts = ?", (ts,))
        row = cur.fetchone()
        return row[0] if row else None

    def get_or_create_dim_url(url: str):
        try:
            p = urlparse(url)
        except Exception:
            return None
        cur.execute(
            "INSERT OR IGNORE INTO dim_url (url, domain, path, query) VALUES (?, ?, ?, ?)",
            (url, p.netloc, p.path, p.query),
        )
        cur.execute("SELECT url_id FROM dim_url WHERE url = ?", (url,))
        row = cur.fetchone()
        return row[0] if row else None

    def get_or_create_dim_status(status: int):
        status_type = f"{str(status)[0]}xx" if 100 <= status <= 599 else "other"
        cur.execute(
            "INSERT OR IGNORE INTO dim_status (status_code, status_type) VALUES (?, ?)",
            (status, status_type),
        )
        cur.execute("SELECT status_id FROM dim_status WHERE status_code = ?", (status,))
        row = cur.fetchone()
        return row[0] if row else None

    valid = invalid = 0

    for row_id, ts, method, url, status, mime, wait_ms in rows:
        bad = False

        # ----- DQ CHECKS -----
        # 1. time
        if not isinstance(ts, str) or not ts.strip():
            record_issue(row_id, "missing_time", "Empty timestamp")
            bad = True
        else:
            try:
                datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                record_issue(row_id, "invalid_time", f"Unparseable time: {ts}")
                bad = True

        # 2. url
        if not isinstance(url, str) or not url.startswith("http"):
            record_issue(row_id, "invalid_url", f"Bad url: {url}")
            bad = True

        # 3. status
        try:
            s_int = int(status)
        except Exception:
            record_issue(row_id, "invalid_status", f"Not int: {status}")
            bad = True
        else:
            if not (100 <= s_int <= 599):
                record_issue(row_id, "invalid_status", f"Out of range: {s_int}")
                bad = True

        if bad:
            invalid += 1
            continue

        # ----- DIM INSERT -----
        time_id = get_or_create_dim_time(ts)
        url_id = get_or_create_dim_url(url)
        status_id = get_or_create_dim_status(s_int)

        if None in (time_id, url_id, status_id):
            record_issue(row_id, "dim_error", "Could not get dim IDs")
            invalid += 1
            continue

        # ----- FACT INSERT -----
        cur.execute(
            """
            INSERT INTO fact_requests
                (time_id, url_id, status_id, method, mime_type, wait_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (time_id, url_id, status_id, method, mime, float(wait_ms) if wait_ms is not None else None),
        )
        valid += 1

    conn.commit()
    conn.close()

    print(f"Loaded into fact: {valid}, rejected rows: {invalid}")


def export_for_looker():
    """Join fact + dim và xuất ra CSV final cho Looker"""
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT
      f.request_id,
      t.ts        AS time,
      t.date      AS date,
      t.hour      AS hour,
      t.minute    AS minute,
      u.url       AS url,
      u.domain    AS domain,
      u.path      AS path,
      u.query     AS query,
      s.status_code,
      s.status_type,
      f.method,
      f.mime_type,
      f.wait_ms
    FROM fact_requests f
    JOIN dim_time   t ON f.time_id = t.time_id
    JOIN dim_url    u ON f.url_id = u.url_id
    JOIN dim_status s ON f.status_id = s.status_id
    ORDER BY f.request_id
    """

    df_final = pd.read_sql_query(query, conn)
    df_final.to_csv(OUT_CSV, index=False)
    conn.close()
    print(f"Exported final DWH file to: {OUT_CSV}")


if __name__ == "__main__":
    init_dirs()
    init_db()
    load_staging()
    run_etl()
    export_for_looker()
