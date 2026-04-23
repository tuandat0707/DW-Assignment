"""
00_explore_data.py — Bước EDA (Exploratory Data Analysis)
=========================================================
Chạy script này SAU KHI đã import SQL file vào Docker SQL Server.
Mục đích:
  1. Kiểm tra số lượng dòng, cột null, phân phối dữ liệu
  2. Phân tích phân phối arr_delay (target)
  3. Phân tích từng feature candidate
  4. Xuất báo cáo EDA ra file reports/eda_report.txt
  5. Vẽ các biểu đồ lưu vào reports/plots/

Yêu cầu:
  pip install pandas sqlalchemy pyodbc matplotlib seaborn tabulate
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Headless mode

import seaborn as sns
import pyodbc
import warnings
import os
import sys
from pathlib import Path

warnings.filterwarnings('ignore')

# ── Setup output dirs ───────────────────────────────────────────────
REPORTS_DIR = Path(__file__).parent / "reports"
PLOTS_DIR = REPORTS_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# ── DB Config ───────────────────────────────────────────────────────
SERVER   = "127.0.0.1,1433"
PASSWORD = "FlightDW@2024"
SCHEMA   = "DataWarehouse"

_DRIVERS = ["ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server"]

def get_conn(database="DataWarehouse"):
    for drv in _DRIVERS:
        try:
            return pyodbc.connect(
                f"DRIVER={{{drv}}};SERVER={SERVER};DATABASE={database};"
                f"UID=SA;PWD={PASSWORD};TrustServerCertificate=yes;Encrypt=no;"
            )
        except:
            continue

    for drv in _DRIVERS:
        try:
            return pyodbc.connect(
                f"DRIVER={{{drv}}};SERVER={SERVER};DATABASE={database};"
                f"UID=SA;PWD={PASSWORD};"
            )
        except:
            continue
    raise RuntimeError("Cannot connect to SQL Server. Check docker container is running.")

def run_query(sql, conn=None):
    """Run a query and return a pandas DataFrame."""
    close_after = conn is None
    if conn is None:
        conn = get_conn()
    df = pd.read_sql(sql, conn)
    if close_after:
        conn.close()
    return df


# ══════════════════════════════════════════════════════════════════
# 1. KẾT NỐI VÀ KIỂM TRA DỮ LIỆU CƠ BẢN
# ══════════════════════════════════════════════════════════════════
print("=" * 60)
print("BƯỚC 0: EDA - Khám phá dữ liệu Data Warehouse")
print("=" * 60)

print("\n[1/6] Ket noi SQL Server...")
try:
    conn_test = get_conn()
    cur_test = conn_test.cursor()
    cur_test.execute("SELECT 1")
    conn_test.close()
    print("  OK Ket noi thanh cong!")
except Exception as e:
    print(f"  LOI ket noi: {e}")
    print("\n  TIP: Kiem tra lai:")
    print("    - Docker container dang chay: docker ps")
    print("    - Port 1433 da duoc map")
    sys.exit(1)


# ── 1.1 Dem row tung bang ─────────────────────────────────────────
print("\n[2/6] Thong ke so dong trong Data Warehouse...")
tables = ["Fact_Flight", "Dim_Date", "Dim_Airline", "Dim_Airport",
          "Dim_Time", "Dim_Delay_Category", "Dim_Cancellation"]

row_counts = {}
conn2 = get_conn()
cur2 = conn2.cursor()
for tbl in tables:
    try:
        cur2.execute(f"SELECT COUNT(*) FROM [{SCHEMA}].[{tbl}]")
        count = cur2.fetchone()[0]
        row_counts[tbl] = count
        print(f"  {tbl:<30} {count:>10,} rows")
    except Exception as e:
        row_counts[tbl] = 0
        print(f"  {tbl:<30} ERROR: {e}")
conn2.close()

total_fact = row_counts.get("Fact_Flight", 0)
print(f"\n  Total Fact_Flight: {total_fact:,} rows")


# ══════════════════════════════════════════════════════════════════
# 2. LOAD DỮ LIỆU MẪU ĐỂ PHÂN TÍCH (sample nếu data quá lớn)
# ══════════════════════════════════════════════════════════════════
print("\n[3/6] Load dữ liệu phân tích...")

SAMPLE_SIZE = 300_000

query = f"""
SELECT TOP {SAMPLE_SIZE}
    f.flight_sk,

    -- Thời gian
    d.month,
    d.day_of_week,
    d.day_of_month,
    d.quarter,
    d.is_weekend,
    d.year,

    -- Giờ bay
    t.hour           AS dep_hour,
    t.period         AS dep_period,

    -- Hãng bay
    a.op_unique_carrier  AS carrier,

    -- Sân bay
    ap_o.airport_code AS origin_airport,
    ap_o.state_nm     AS origin_state,
    ap_d.airport_code AS dest_airport,
    ap_d.state_nm     AS dest_state,

    -- Measures
    f.distance,
    f.dep_delay,
    f.arr_delay,
    f.cancelled,

    -- Nguyên nhân trễ (để phân tích)
    f.carrier_delay,
    f.weather_delay,
    f.nas_delay,
    f.security_delay,
    f.late_aircraft_delay

FROM [{SCHEMA}].[Fact_Flight] f
JOIN [{SCHEMA}].[Dim_Date]    d    ON f.date_sk           = d.date_sk
JOIN [{SCHEMA}].[Dim_Time]    t    ON f.dep_time_sk       = t.time_sk
JOIN [{SCHEMA}].[Dim_Airline] a    ON f.airline_sk        = a.airline_sk
JOIN [{SCHEMA}].[Dim_Airport] ap_o ON f.origin_airport_sk = ap_o.airport_sk
JOIN [{SCHEMA}].[Dim_Airport] ap_d ON f.dest_airport_sk   = ap_d.airport_sk
WHERE f.cancelled = 0
  AND f.arr_delay IS NOT NULL
ORDER BY NEWID()   -- Random sample
"""

print(f"  Dang query {SAMPLE_SIZE:,} rows ngau nhien (bo qua cancelled)...")
df = run_query(query)
print(f"  Load xong: {df.shape[0]:,} rows x {df.shape[1]} columns")


# ══════════════════════════════════════════════════════════════════
# 3. PHÂN TÍCH NULL & DATA QUALITY
# ══════════════════════════════════════════════════════════════════
print("\n[4/6] Kiểm tra chất lượng dữ liệu...")

report_lines = []
report_lines.append("=" * 70)
report_lines.append("EDA REPORT — FLIGHT DELAY DATA WAREHOUSE")
report_lines.append(f"Sample size: {df.shape[0]:,} rows | Columns: {df.shape[1]}")
report_lines.append("=" * 70)

# Row counts
report_lines.append("\nSỐ DÒNG TỪNG BẢNG:")
for tbl, cnt in row_counts.items():
    report_lines.append(f"  {tbl:<30} {cnt:>10,}")

# Null analysis
report_lines.append("\n\nPHÂN TÍCH NULL VALUES:")
null_df = df.isnull().sum().reset_index()
null_df.columns = ['column', 'null_count']
null_df['null_pct'] = (null_df['null_count'] / len(df) * 100).round(2)
null_df = null_df[null_df['null_count'] > 0].sort_values('null_pct', ascending=False)

if null_df.empty:
    report_lines.append("  Không có NULL values!")
else:
    for _, row in null_df.iterrows():
        flag = "⚠️ " if row['null_pct'] > 20 else "  "
        report_lines.append(f"{flag}{row['column']:<30} {row['null_count']:>8,} ({row['null_pct']:>5.1f}%)")

# Basic stats
report_lines.append("\n\nTHỐNG KÊ arr_delay (TARGET VARIABLE):")
delay_stats = df['arr_delay'].describe()
for stat, val in delay_stats.items():
    report_lines.append(f"  {stat:<12} {val:>10.2f} phút")


# ══════════════════════════════════════════════════════════════════
# 4. PHÂN TÍCH TARGET: arr_delay
# ══════════════════════════════════════════════════════════════════
# Tạo nhãn phân loại
df['is_delayed'] = (df['arr_delay'] > 15).astype(int)
delay_rate = df['is_delayed'].mean() * 100

report_lines.append(f"\n\nPHÂN BỐ NHÃN (is_delayed = arr_delay > 15 phút):")
report_lines.append(f"  Chuyến bay ĐÚNG GIỜ (0): {(df['is_delayed']==0).sum():>8,}  ({100-delay_rate:.1f}%)")
report_lines.append(f"  Chuyến bay TRỄ     (1): {(df['is_delayed']==1).sum():>8,}  ({delay_rate:.1f}%)")

if delay_rate < 25 or delay_rate > 75:
    report_lines.append(f"\n  Mất cân bằng nhãn! Cần dùng scale_pos_weight trong XGBoost")
    report_lines.append(f"     scale_pos_weight = {(1-delay_rate/100)/(delay_rate/100):.2f}")
else:
    report_lines.append("  Nhãn cân bằng tốt, không cần oversampling")


# ══════════════════════════════════════════════════════════════════
# 5. PHÂN TÍCH TỪNG FEATURE
# ══════════════════════════════════════════════════════════════════
report_lines.append("\n\nPHÂN TÍCH FEATURES:")

# Số hãng bay
n_carriers = df['carrier'].nunique()
report_lines.append(f"\n  Số hãng bay (carrier): {n_carriers}")
carrier_delay = df.groupby('carrier')['is_delayed'].mean().sort_values(ascending=False)
report_lines.append("  Top 5 hãng trễ nhiều nhất:")
for c, r in carrier_delay.head(5).items():
    report_lines.append(f"    {c}: {r*100:.1f}%")

# Số state
n_origins = df['origin_state'].nunique()
n_dests = df['dest_state'].nunique()
report_lines.append(f"\n  Số bang xuất phát (origin_state): {n_origins}")
report_lines.append(f"  Số bang đến (dest_state): {n_dests}")

# Phân phối distance
report_lines.append(f"\n  Khoảng cách (distance):")
report_lines.append(f"    Min:  {df['distance'].min():>8.0f} dặm")
report_lines.append(f"    Max:  {df['distance'].max():>8.0f} dặm")
report_lines.append(f"    Mean: {df['distance'].mean():>8.0f} dặm")

# Tỉ lệ trễ theo tháng
report_lines.append(f"\n  Tỉ lệ trễ theo tháng:")
month_delay = df.groupby('month')['is_delayed'].mean() * 100
for m, r in month_delay.items():
    bar = "█" * int(r / 3)
    report_lines.append(f"    Tháng {m:>2}: {r:>5.1f}% {bar}")

# Tỉ lệ trễ theo giờ cất cánh
report_lines.append(f"\n  Tỉ lệ trễ theo giờ cất cánh (dep_hour):")
hour_delay = df.groupby('dep_hour')['is_delayed'].mean() * 100
for h, r in hour_delay.items():
    bar = "█" * int(r / 3)
    report_lines.append(f"    {h:>2}h: {r:>5.1f}% {bar}")


# ══════════════════════════════════════════════════════════════════
# 6. VẼ BIỂU ĐỒ EDA
# ══════════════════════════════════════════════════════════════════
print("[5/6] Vẽ biểu đồ EDA...")

plt.style.use('seaborn-v0_8-darkgrid')
fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle('EDA — Flight Delay Data Warehouse', fontsize=16, fontweight='bold')

# Plot 1: Phân phối arr_delay
ax = axes[0, 0]
clip_df = df[df['arr_delay'].between(-60, 300)]
ax.hist(clip_df['arr_delay'], bins=80, color='steelblue', alpha=0.7, edgecolor='white')
ax.axvline(x=15, color='red', linestyle='--', label='Ngưỡng trễ (15 phút)')
ax.set_title('Phân phối arr_delay')
ax.set_xlabel('Phút trễ')
ax.set_ylabel('Số chuyến')
ax.legend()

# Plot 2: Tỉ lệ trễ theo tháng
ax = axes[0, 1]
month_delay.plot(kind='bar', ax=ax, color='coral')
ax.set_title('Tỉ lệ trễ theo Tháng (%)')
ax.set_xlabel('Tháng')
ax.set_ylabel('% trễ')

# Plot 3: Tỉ lệ trễ theo giờ
ax = axes[0, 2]
hour_delay.plot(kind='bar', ax=ax, color='mediumpurple')
ax.set_title('Tỉ lệ trễ theo Giờ cất cánh (%)')
ax.set_xlabel('Giờ')
ax.set_ylabel('% trễ')

# Plot 4: Tỉ lệ trễ theo hãng bay
ax = axes[1, 0]
carrier_delay.plot(kind='bar', ax=ax, color='teal')
ax.set_title('Tỉ lệ trễ theo Hãng bay (%)')
ax.set_xlabel('Hãng bay')
ax.set_ylabel('% trễ')

# Plot 5: Tỉ lệ trễ theo thứ
ax = axes[1, 1]
dow_delay = df.groupby('day_of_week')['is_delayed'].mean() * 100
dow_labels = ['Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7', 'CN']
dow_delay.index = dow_labels[:len(dow_delay)]
dow_delay.plot(kind='bar', ax=ax, color='goldenrod')
ax.set_title('Tỉ lệ trễ theo Thứ (%)')
ax.set_xlabel('Ngày trong tuần')
ax.set_ylabel('% trễ')

# Plot 6: Phân bố nhãn (Pie)
ax = axes[1, 2]
label_counts = df['is_delayed'].value_counts()
ax.pie(label_counts.values,
       labels=['Đúng giờ', 'Trễ'],
       colors=['#2ecc71', '#e74c3c'],
       autopct='%1.1f%%',
       startangle=90)
ax.set_title('Phân bố nhãn is_delayed')

plt.tight_layout()
plot_path = PLOTS_DIR / "eda_overview.png"
plt.savefig(plot_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"  ✅ Đã lưu biểu đồ: {plot_path}")


# ══════════════════════════════════════════════════════════════════
# 7. KẾT LUẬN & GỢI Ý PREPROCESSING
# ══════════════════════════════════════════════════════════════════
report_lines.append("\n\n💡 GỢI Ý PREPROCESSING CHO TRAINING:")
report_lines.append("  Features sẽ dùng:")
report_lines.append("    - month, day_of_week, quarter, is_weekend  → Numeric")
report_lines.append("    - dep_hour                                  → Numeric")
report_lines.append("    - distance                                  → Numeric")
report_lines.append("    - carrier                                   → Label Encode")
report_lines.append("    - origin_state, dest_state                  → Label Encode")
report_lines.append("\n  Xử lý null:")
for _, row in null_df.iterrows():
    if row['null_pct'] > 30:
        report_lines.append(f"    {row['column']}: drop column (null {row['null_pct']:.1f}%)")
    else:
        report_lines.append(f"    {row['column']}: fillna(0) hoặc fillna(median)")

spw = (1 - delay_rate/100) / (delay_rate/100) if delay_rate > 0 else 1.0
report_lines.append(f"\n  XGBoost params đề xuất:")
report_lines.append(f"    scale_pos_weight = {spw:.2f}")
report_lines.append(f"    n_estimators     = 300")
report_lines.append(f"    max_depth        = 6")
report_lines.append(f"    learning_rate    = 0.1")

# Ghi report
report_path = REPORTS_DIR / "eda_report.txt"
with open(report_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(report_lines))

print(f"\n[6/6] Đã lưu báo cáo EDA: {report_path}")
print("\n" + "=" * 60)
print("✅ EDA HOÀN THÀNH! Xem kết quả trong thư mục reports/")
print("   → Chạy tiếp: python 01_train_model.py")
print("=" * 60)
