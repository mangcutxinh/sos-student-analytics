# Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên

> **Môn học:** Kiến Trúc Hướng Dịch Vụ & Điện Toán Đám Mây  
> **Nền tảng Cloud:** Databricks Community Edition  
> **Báo cáo:** Tiến độ giữa kỳ

---

## Mô tả dự án

Hệ thống áp dụng kiến trúc **SOA (Service-Oriented Architecture)** để thu thập, xử lý và phân tích kết quả học tập của sinh viên. Dữ liệu được xử lý qua **ETL pipeline 4 bước** trên Databricks, lưu trữ theo mô hình **Medallion Architecture** (Bronze → Silver → Gold) với Delta Lake.

### Chức năng chính
- Thu thập điểm số từ nhiều thành phần (quiz, midterm, final)
- Làm sạch và chuẩn hóa dữ liệu tự động
- Tính điểm tổng kết, xếp loại học lực (A/B/C/D/F)
- Phát hiện sinh viên có nguy cơ học yếu (`at_risk`)
- Phân tích tương quan điểm danh – kết quả học tập

---

## Thành viên nhóm

| STT | Họ và Tên | MSSV | Vai Trò | Nhiệm Vụ |
|-----|-----------|------|---------|----------|
| 1 | [Tên thành viên 1] | [MSSV] | Nhóm trưởng | Kiến trúc SOA, API Gateway, quản lý repo |
| 2 | [Tên thành viên 2] | [MSSV] | Thành viên | Databricks setup, Data ingestion |
| 3 | [Tên thành viên 3] | [MSSV] | Thành viên | ETL pipeline, Bronze/Silver layer |
| 4 | [Tên thành viên 4] | [MSSV] | Thành viên | Gold layer, Analytics service |
| 5 | [Tên thành viên 5] | [MSSV] | Thành viên | Dashboard, Documentation |

---

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────┐
│                   Client Layer                       │
│          Web App  │  Mobile App  │  LMS Portal       │
└──────────────────────┬──────────────────────────────┘
                       │ HTTP/REST
┌──────────────────────▼──────────────────────────────┐
│              API Gateway (JWT Auth, Rate-limit)      │
└──────────┬───────────┬──────────────┬───────────────┘
           │           │              │
    ┌──────▼─┐  ┌──────▼─┐   ┌───────▼──┐  ┌──────────┐
    │Student │  │ Score  │   │Analytics │  │Notif.    │
    │Service │  │Service │   │Service   │  │Service   │
    └──────┬─┘  └──────┬─┘   └───────┬──┘  └──────────┘
           └───────────┴─────────────┘
                       │ JDBC / REST
┌──────────────────────▼──────────────────────────────┐
│           Databricks (Spark ETL + ML)                │
│    Ingestion → Bronze → Silver → Gold Pipeline       │
└──────────────────────┬──────────────────────────────┘
                       │ Delta Lake
┌──────────────────────▼──────────────────────────────┐
│         Storage Layer – Delta Lake (DBFS)            │
│  /delta/bronze/  │  /delta/silver/  │  /delta/gold/  │
└─────────────────────────────────────────────────────┘
```

---

## Cấu trúc Repository

```
soa-student-analytics/
│
├── 📁 databricks/
│   ├── 📁 notebooks/
│   │   ├── 01_data_ingestion.py     ← Đọc CSV, validate
│   │   ├── 02_bronze_layer.py       ← Lưu raw → Delta Bronze
│   │   ├── 03_silver_layer.py       ← Làm sạch → Delta Silver
│   │   ├── 04_gold_analytics.py     ← Tính GPA, phân tích → Gold
│   │   └── 05_pipeline_runner.py    ← Orchestrator (dùng cho Job)
│   └── 📁 jobs/
│       └── etl_pipeline_job.json    ← Cấu hình Databricks Job
│
├── 📁 data/
│   └── 📁 mock/
│       └── student_score_dataset.csv  ← 300 sinh viên, 12 cột
│
├── 📁 services/                        ← Microservices (WIP – tuần 13)
│   ├── student-service/
│   ├── score-service/
│   ├── analytics-service/
│   └── notification-service/
│
├── 📁 docs/
│   └── 📁 architecture/
│       └── soa_architecture_diagram.png
│
└── README.md
```

---

## Dataset

**File:** `data/mock/student_score_dataset.csv`

| Cột | Mô tả | Kiểu | Phạm vi |
|-----|-------|------|---------|
| `student_id` | Mã sinh viên | int | 1–300 |
| `name` | Họ tên | string | — |
| `age` | Tuổi | int | 18–25 |
| `gender` | Giới tính | string | Male/Female |
| `quiz1_marks` | Điểm quiz 1 | float | 0–10 |
| `quiz2_marks` | Điểm quiz 2 | float | 0–10 |
| `quiz3_marks` | Điểm quiz 3 | float | 0–10 |
| `midterm_marks` | Điểm giữa kỳ | float | 0–30 |
| `final_marks` | Điểm cuối kỳ | float | 0–50 |
| `previous_gpa` | GPA kỳ trước | float | 0–4 |
| `lectures_attended` | Số buổi lý thuyết | int | 0–10 |
| `labs_attended` | Số buổi thực hành | int | 0–10 |

**Công thức tính điểm tổng kết (thang 100):**
```
quiz_total   = (quiz1 + quiz2 + quiz3) / 30 × 30    → 30%
midterm_pct  = midterm / 30 × 20                     → 20%
final_pct    = final / 50 × 50                       → 50%
total_score  = quiz_total + midterm_pct + final_pct  → 100 điểm
grade_10     = total_score / 10                      → thang 10
```

---

## Hướng dẫn chạy ETL trên Databricks

### Bước 1 – Chuẩn bị Workspace
```
1. Đăng nhập community.cloud.databricks.com
2. Compute → Create Cluster
   - Name    : soa-student-analytics
   - Runtime : 14.3 LTS (Spark 3.5, Scala 2.12)
3. Workspace → Home → Create Folder: soa-student-analytics/
   Tạo subfolder: 01_ingestion/ 02_bronze/ 03_silver/ 04_gold/
```

### Bước 2 – Upload data
```
Catalog → Add Data → DBFS → Upload File
→ Upload: data/mock/student_score_dataset.csv
→ Đường dẫn: /FileStore/tables/student_score_dataset.csv
```

### Bước 3 – Import notebooks
```
Mỗi folder: chuột phải → Import → chọn file .py tương ứng
01_ingestion/ → 01_data_ingestion.py
02_bronze/    → 02_bronze_layer.py
03_silver/    → 03_silver_layer.py
04_gold/      → 04_gold_analytics.py
```

### Bước 4 – Chạy từng bước (hoặc dùng Job)
```python
# Chạy thủ công theo thứ tự:
01_data_ingestion.py   # Validate dữ liệu
02_bronze_layer.py     # /delta/bronze/students
03_silver_layer.py     # /delta/silver/students_clean
04_gold_analytics.py   # /delta/gold/student_gpa
                       # /delta/gold/score_distribution
                       # /delta/gold/attendance_impact
```

### Bước 5 – Tạo Databricks Job (Workflows)
```
Workflows → Create Job → Tên: ETL_Student_Analytics_Pipeline
Task 1: 01_data_ingestion  (Notebook)
Task 2: 02_bronze_layer    (depends on Task 1)
Task 3: 03_silver_layer    (depends on Task 2)
Task 4: 04_gold_analytics  (depends on Task 3)
→ Run Now
```

---

## Delta Lake Tables

Sau khi chạy pipeline, 4 Delta tables được tạo:

| Table | Path | Mô tả |
|-------|------|-------|
| Bronze raw | `/delta/bronze/students` | Dữ liệu gốc + metadata |
| Silver clean | `/delta/silver/students_clean` | Đã làm sạch + tính điểm |
| Gold GPA | `/delta/gold/student_gpa` | Xếp loại từng SV |
| Gold Stats | `/delta/gold/score_distribution` | Thống kê tổng hợp |
| Gold Attendance | `/delta/gold/attendance_impact` | Tương quan điểm danh |

---

## Tiến độ dự án

### Giữa kỳ (hoàn thành)
- [x] Thiết kế kiến trúc SOA (5 layers)
- [x] Dataset 300 sinh viên, 12 cột điểm số
- [x] Databricks Workspace & Cluster setup
- [x] Notebook 01 – Data ingestion & validation
- [x] Notebook 02 – Bronze Delta layer
- [x] Notebook 03 – Silver layer (ETL, tính điểm)
- [x] Notebook 04 – Gold layer (GPA, xếp loại, at_risk)
- [x] Databricks Job / Workflow pipeline
- [x] GitHub repository

### Cuối kỳ (kế hoạch)
- [ ] Tuần 13: Student Service, Score Service, Analytics Service (REST API)
- [ ] Tuần 13: API Gateway (FastAPI / Spring Boot)
- [ ] Tuần 14: Dashboard trực quan
- [ ] Tuần 14: Notification Service (cảnh báo at_risk)
- [ ] Tuần 15: End-to-end testing + tài liệu hoàn chỉnh

---

## Môi trường

- **Databricks:** Community Edition (Spark 3.5, Python 3.11)
- **Delta Lake:** 3.x (built-in với Databricks Runtime 14+)
- **Ngôn ngữ:** PySpark, Python 3

---

*Dự án được thực hiện trong khuôn khổ học phần Kiến Trúc Hướng Dịch Vụ & Điện Toán Đám Mây.*
