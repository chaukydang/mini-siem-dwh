from mitmproxy import http
import csv, os
from datetime import datetime

LOG_PATH = "data/raw/log_parsed.csv"

# Tạo file CSV nếu chưa có
if not os.path.exists(LOG_PATH):
    with open(LOG_PATH, "w", newline="", encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "method", "url", "status", "mimeType", "wait_ms"])

def response(flow: http.HTTPFlow):
    ts = datetime.utcnow().isoformat() + "Z"
    method = flow.request.method
    url = flow.request.pretty_url
    status = flow.response.status_code if flow.response else 0
    mime = flow.response.headers.get("Content-Type", "x-unknown") if flow.response else "error"

    wait_ms = 0
    if flow.response and flow.response.timestamp_start and flow.response.timestamp_end:
        wait_ms = (flow.response.timestamp_end - flow.response.timestamp_start) * 1000

    with open(LOG_PATH, "a", newline="", encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow([ts, method, url, status, mime, wait_ms])
