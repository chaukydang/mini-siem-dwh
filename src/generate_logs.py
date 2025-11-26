import csv
import time
from datetime import datetime
from pathlib import Path

import requests

OUT_CSV = Path("data/raw/log_parsed.csv")

# Một số URL để tạo “traffic” giả lập
TARGET_URLS = [
    "https://shopee.vn/",
    "https://shopee.vn/search?keyword=iphone",
    "https://shopee.vn/search?keyword=laptop",
    "https://shopee.vn/cart",
    "https://shopee.vn/flash_sale",    
    "https://shopee.vn/collections",
    "https://shopee.vn/mall",
    "https://shopee.vn/brands",
    "https://shopee.vn/helpcenter",
]

def ensure_header():
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["time", "method", "url", "status", "mimeType", "wait_ms"])

def main():
    ensure_header()

    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for i in range(100):  # tạo 100 request demo, muốn nhiều tăng lên
            for url in TARGET_URLS:
                start = time.time()
                try:
                    r = requests.get(url, timeout=10)
                    elapsed_ms = (time.time() - start) * 1000.0
                    ts = datetime.utcnow().isoformat() + "Z"
                    mime = r.headers.get("Content-Type", "x-unknown")
                    status = r.status_code
                except Exception as e:
                    # Nếu lỗi kết nối -> ghi status 0, mimeType error
                    ts = datetime.utcnow().isoformat() + "Z"
                    elapsed_ms = (time.time() - start) * 1000.0
                    status = 0
                    mime = "error"

                writer.writerow([ts, "GET", url, status, mime, elapsed_ms])
                print(f"Logged: {url} - {status} - {elapsed_ms:.1f} ms")

            # Nghỉ xíu để tránh spam server người ta
            time.sleep(5)

if __name__ == "__main__":
    main()
