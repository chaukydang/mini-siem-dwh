import random
from pathlib import Path
import pandas as pd

IN_CSV  = Path("data/dwh/dwh_requests.csv")
OUT_CSV = Path("data/dwh/dwh_requests_balanced_big.csv")

# 🔥 target riêng cho từng nhóm (ông chỉnh tùy ý)
TARGET_PER_CLASS = {
    "2xx": 8000,
    "3xx": 3000,
    "4xx": 2500,
    "5xx": 2000,
}

print("Đọc dữ liệu từ:", IN_CSV)
df = pd.read_csv(IN_CSV, parse_dates=["time"])

blocks = []

def build_block(df_src, label, target_n):
    if df_src.empty:
        print(f"- Không có bản ghi {label}, bỏ qua.")
        return None

    if len(df_src) >= target_n:
        sampled = df_src.sample(target_n, random_state=42)
        print(f"- {label}: sample {target_n} / {len(df_src)} (không lặp)")
    else:
        sampled = df_src.sample(target_n, replace=True, random_state=42)
        print(f"- {label}: oversample {target_n} từ {len(df_src)} (có lặp)")

    sampled = sampled.copy()

    # jitter wait_ms ±20%
    if "wait_ms" in sampled.columns:
        sampled["wait_ms"] = sampled["wait_ms"].apply(
            lambda x: max(1.0, x * random.uniform(0.8, 1.2))
        )

    # jitter time ± 0–30 phút
    jitter_secs = [random.randint(-30 * 60, 30 * 60) for _ in range(len(sampled))]
    sampled["time"] = sampled["time"] + pd.to_timedelta(jitter_secs, unit="s")

    return sampled

for label in ["2xx", "3xx", "4xx", "5xx"]:
    target_n = TARGET_PER_CLASS.get(label)
    if not target_n:
        continue

    df_label = df[df["status_type"] == label]
    block = build_block(df_label, label, target_n)
    if block is not None:
        blocks.append(block)

df_big = pd.concat(blocks, ignore_index=True)
df_big.to_csv(OUT_CSV, index=False)

print("\n✅ Đã lưu:", OUT_CSV)
print("Số dòng:", len(df_big))
print(df_big["status_type"].value_counts())
