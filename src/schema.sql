-- STAGING: log thô đã parse từ CSV
DROP TABLE IF EXISTS stg_logs;
CREATE TABLE stg_logs (
    row_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    time      TEXT,
    method    TEXT,
    url       TEXT,
    status    INTEGER,
    mimeType  TEXT,
    wait_ms   REAL
);

-- Bảng ghi lỗi chất lượng dữ liệu
DROP TABLE IF EXISTS dq_issues;
CREATE TABLE dq_issues (
    issue_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    stg_row_id INTEGER,
    issue_type TEXT,
    detail     TEXT
);

-- Dimension time
DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      TEXT UNIQUE,   -- original timestamp
    date    TEXT,
    hour    INTEGER,
    minute  INTEGER
);

-- Dimension url
DROP TABLE IF EXISTS dim_url;
CREATE TABLE dim_url (
    url_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    url     TEXT UNIQUE,
    domain  TEXT,
    path    TEXT,
    query   TEXT
);

-- Dimension status
DROP TABLE IF EXISTS dim_status;
CREATE TABLE dim_status (
    status_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    status_code INTEGER UNIQUE,
    status_type TEXT          -- 2xx / 3xx / 4xx / 5xx
);

-- Fact table
DROP TABLE IF EXISTS fact_requests;
CREATE TABLE fact_requests (
    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_id    INTEGER,
    url_id     INTEGER,
    status_id  INTEGER,
    method     TEXT,
    mime_type  TEXT,
    wait_ms    REAL
);
