import csv
import time
from datetime import datetime
from pathlib import Path
import requests

LOG_PATH = Path("data/raw/log_parsed.csv")
COOKIE_PATH = Path("data/cookie/shopee_cookie.txt")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

TARGET_URLS = [
    "https://shopee.vn/",
    "https://shopee.vn/search?keyword=iphone",
    "https://shopee.vn/search?keyword=giay",
    "https://shopee.vn/this-page-should-not-exist-404",
    "https://shopee.vn/cart",
    "https://shopee.vn/flash_sale",    
    "https://shopee.vn/collections",
    "https://shopee.vn/mall",
    "https://shopee.vn/brands",
    "https://shopee.vn/helpcenter",
]

# Số vòng lặp muốn chạy (mỗi vòng quét hết TARGET_URLS một lần)
N_ROUNDS = 30          # đổi tuỳ ý, ví dụ 30 vòng = 30 * len(URL) request
SLEEP_BETWEEN_REQ = 1  # giây nghỉ giữa 2 request


def load_cookie():
    if not COOKIE_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy file cookie: {COOKIE_PATH}")

    raw = COOKIE_PATH.read_text().strip()
    parts = [line.strip() for line in raw.splitlines() if line.strip()]
    cookie = "; ".join(parts)
    return cookie


def ensure_header():
    if not LOG_PATH.exists():
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("w", newline="", encoding="utf8") as f:
            w = csv.writer(f)
            w.writerow(["time", "method", "url", "status", "mimeType", "wait_ms"])


def log_request(method, url, status, mime, elapsed_ms):
    ts = datetime.utcnow().isoformat() + "Z"
    with LOG_PATH.open("a", newline="", encoding="utf8") as f:
        w = csv.writer(f)
        w.writerow([ts, method, url, status, mime, elapsed_ms])


def main():
    ensure_header()

    cookie = load_cookie()
    print("✔ Cookie loaded (rút gọn):", cookie[:50], "...")

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Cookie": cookie,
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    })

    print(f"🚀 Bắt đầu thu thập log Shopee trong {N_ROUNDS} vòng…")

    for round_idx in range(N_ROUNDS):
        print(f"\n🔁 Vòng {round_idx + 1}/{N_ROUNDS}")
        for url in TARGET_URLS:
            start = time.time()
            try:
                r = session.get(url, timeout=8)  # timeout 8 giây cho nhanh
                status = r.status_code
                mime = r.headers.get("Content-Type", "x-unknown")
            except requests.exceptions.Timeout:
                status = 0
                mime = "timeout"
            except Exception as e:
                status = 0
                mime = f"error:{type(e).__name__}"

            elapsed_ms = (time.time() - start) * 1000.0
            log_request("GET", url, status, mime, elapsed_ms)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] {url} -> {status} ({elapsed_ms:.1f} ms)")

            time.sleep(SLEEP_BETWEEN_REQ)

    print("\n✅ Xong, đã tạo đủ log. Thoát script.")


if __name__ == "__main__":
    main()
