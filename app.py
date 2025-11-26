# import streamlit as st
# st.set_page_config(page_title="Mini SIEM – Dashboard", layout="wide")

# import pandas as pd
# import plotly.express as px


# # ============================
# # LOAD DATA
# # ============================

# @st.cache_data
# def load_data():
#     df = pd.read_csv("data/dwh/dwh_requests.csv")
#     df["time"] = pd.to_datetime(df["time"], errors="coerce")
#     return df

# df = load_data()

import streamlit as st
import pandas as pd
import plotly.express as px 

st.set_page_config(page_title="Mini SIEM – Dashboard", layout="wide")

DATA_OPTION = st.sidebar.selectbox(
    "Chọn dataset",
    ["DWH gốc (log thật)", "DWH cân bằng lớn (augment)"]
)

if DATA_OPTION == "DWH gốc (log thật)":
    df = pd.read_csv("data/dwh/dwh_requests.csv", parse_dates=["time"])
else:
    df = pd.read_csv("data/dwh/dwh_requests_balanced_big.csv", parse_dates=["time"])

# ============================
# KPI SECTION
# ============================

st.title("MINI-SIEM – Dashboard Giám Sát An Ninh Hệ Thống TMĐT")

col1, col2, col3, col4 = st.columns(4)

total_req = len(df)
error_4xx = len(df[df["status_type"] == "4xx"])
error_5xx = len(df[df["status_type"] == "5xx"])
avg_wait = df["wait_ms"].mean()

col1.metric("Tổng Request", f"{total_req:,}")
col2.metric("Tỷ lệ lỗi 4xx", f"{error_4xx / total_req:.2%}")
col3.metric("Tỷ lệ lỗi 5xx", f"{error_5xx / total_req:.2%}")
col4.metric("Wait Time trung bình (ms)", f"{avg_wait:.1f} ms")

st.markdown("---")

# ============================
# TRAFFIC OVER TIME
# ============================

st.subheader("Traffic theo thời gian")

df_time = df.groupby(pd.Grouper(key="time", freq="1min")).size().reset_index(name="count")

fig_traffic = px.line(
    df_time, x="time", y="count",
    title="Lưu lượng Request theo phút",
    markers=True
)
st.plotly_chart(fig_traffic, use_container_width=True)

# ============================
# STATUS BREAKDOWN
# ============================

st.subheader("Trạng thái trả về (Status Breakdown)")

fig_status = px.bar(
    df.groupby("status_type").size().reset_index(name="count"),
    x="status_type", y="count",
    color="status_type",
    title="Phân bố status"
)
st.plotly_chart(fig_status, use_container_width=True)

col5, col6 = st.columns(2)

# ============================
# TOP SUSPICIOUS URL (4xx)
# ============================

with col5:
    st.subheader("URL nghi ngờ – nhiều lỗi 4xx")

    df_susp = df[df["status_type"] == "4xx"]
    top_susp = df_susp["path"].value_counts().reset_index()
    top_susp.columns = ["path", "count"]

    fig_susp = px.bar(top_susp.head(10), x="count", y="path",
                      orientation="h", title="Top 10 suspicious URL (4xx)")
    st.plotly_chart(fig_susp, use_container_width=True)

# ============================
# SLOW ENDPOINTS
# ============================

with col6:
    st.subheader("Endpoint chậm – Wait Time cao")

    df_slow = df.groupby("path")["wait_ms"].mean().sort_values(ascending=False).reset_index()

    fig_slow = px.bar(df_slow.head(10), x="wait_ms", y="path",
                      orientation="h", title="Top 10 slow endpoints")
    st.plotly_chart(fig_slow, use_container_width=True)

st.markdown("---")

# ============================
# RAW LOG TABLE
# ============================

st.subheader("Bảng request chi tiết")

st.dataframe(df.head(500), use_container_width=True)

# ============================
# ANOMALY DETECTION (optional)
# ============================

st.subheader("Phát hiện bất thường (Simple Rules)")

traffic_threshold = df_time["count"].mean() * 3

st.write(f"- Ngưỡng spike traffic: > {traffic_threshold:.1f} request/min")

anomalies = df_time[df_time["count"] > traffic_threshold]

if len(anomalies) > 0:
    st.error("PHÁT HIỆN TRAFFIC SPiKE!")
    st.dataframe(anomalies)
else:
    st.success("Không phát hiện spike bất thường.")
