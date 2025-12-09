import csv
import random
import time
from datetime import datetime
from pathlib import Path

LOG_PATH = Path("data/raw/log_parsed.csv")

# Các URL bình thường (lấy từ script Shopee của ông)
NORMAL_URLS = [
    "https://shopee.vn/",
    "https://shopee.vn/search?keyword=iphone",
    "https://shopee.vn/search?keyword=giay",
    "https://shopee.vn/cart",
    "https://shopee.vn/flash_sale",
    "https://shopee.vn/collections",
    "https://shopee.vn/mall",
    "https://shopee.vn/brands",
    "https://shopee.vn/helpcenter",
]

# URL lỗi / không tồn tại
ERROR_URLS = [
    "https://shopee.vn/this-page-should-not-exist-404",
    "https://shopee.vn/admin",
    "https://shopee.vn/.git/config",
    "https://shopee.vn/config.php.bak",
]

# URL mô phỏng tấn công SQLi / XSS / LFI
ATTACK_URLS = [
    "https://shopee.vn/search?keyword=' OR 1=1--",
    "https://shopee.vn/search?keyword=' UNION SELECT null,null--",
    "https://shopee.vn/search?keyword=<script>alert(1)</script>",
    "https://shopee.vn/search?keyword=%3Csvg%20onload%3Dalert(1)%3E",
    "https://shopee.vn/download?file=../../../../etc/passwd",
]

TOTAL_EVENTS = 800        # số dòng log muốn sinh thêm
SLEEP_BETWEEN_EVENTS = 0  # để 0 cho nhanh, hoặc 0.05 cho giống realtime


def ensure_header():
    if not LOG_PATH.exists():
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("w", newline="", encoding="utf8") as f:
            w = csv.writer(f)
            w.writerow(["time", "method", "url", "status", "mimeType", "wait_ms"])


def write_log_row(method, url, status, mime, wait_ms):
    ts = datetime.utcnow().isoformat() + "Z"
    with LOG_PATH.open("a", newline="", encoding="utf8") as f:
        w = csv.writer(f)
        w.writerow([ts, method, url, status, mime, wait_ms])


def simulate_event():
    """
    Sinh 1 dòng log:
      - lựa chọn loại traffic (normal / error / attack)
      - chọn URL phù hợp
      - random status code & thời gian đáp ứng
    """
    r = random.random()

    # 60% normal, 20% error, 20% attack
    if r < 0.6:
        url = random.choice(NORMAL_URLS)
        status = random.choice([200, 200, 200, 301])   # đa số 200, thi thoảng 301
        wait_ms = random.gauss(180, 60)               # trung bình ~180ms
        mime = "text/html; charset=utf-8"
    elif r < 0.8:
        url = random.choice(ERROR_URLS)
        status = random.choice([403, 404, 404, 500])
        wait_ms = random.gauss(250, 100)
        mime = "text/html; charset=utf-8"
    else:
        url = random.choice(ATTACK_URLS)
        status = random.choice([200, 400, 403, 404, 500])
        wait_ms = random.gauss(350, 150)              # nặng hơn bình thường
        mime = "text/html; charset=utf-8"

    # ép giới hạn wait_ms > 0
    wait_ms = max(10, abs(wait_ms))

    write_log_row("GET", url, status, mime, wait_ms)


def main():
    ensure_header()
    print(f"🚀 Attack logger (offline) – sinh thêm {TOTAL_EVENTS} events vào {LOG_PATH}")

    for i in range(TOTAL_EVENTS):
        simulate_event()
        if (i + 1) % 100 == 0:
            print(f"  → Đã sinh {i + 1} events")
        if SLEEP_BETWEEN_EVENTS > 0:
            time.sleep(SLEEP_BETWEEN_EVENTS)

    print("Hoàn tất sinh log attack giả lập.")


if __name__ == "__main__":
    main()
