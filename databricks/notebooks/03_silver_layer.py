# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Silver Layer (Cleaned & Transformed)
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Silver layer thực hiện:
# MAGIC - Loại bỏ dữ liệu null / không hợp lệ
# MAGIC - Chuẩn hóa kiểu dữ liệu
# MAGIC - Tính **total_score** (thang 100) từ các thành phần
# MAGIC - Tính **grade_10** (quy về thang 10 để so sánh với GPA)

# COMMAND ----------

# MAGIC %md ## 1. Đọc từ Bronze

# COMMAND ----------

from pyspark.sql.functions import (
    col, round as spark_round, when, trim, upper,
    current_timestamp, lit
)
from pyspark.sql.types import DoubleType

df_bronze = spark.read.format("delta").load("/delta/bronze/students")
print(f"📦 Bronze input: {df_bronze.count()} rows")

# COMMAND ----------

# MAGIC %md ## 2. Làm sạch dữ liệu

# COMMAND ----------

# Bỏ null ở các cột điểm quan trọng
df_clean = df_bronze.dropna(subset=[
    "student_id", "midterm_marks", "final_marks",
    "quiz1_marks", "quiz2_marks", "quiz3_marks"
])

# Ép kiểu số
num_cols = ["quiz1_marks","quiz2_marks","quiz3_marks",
            "midterm_marks","final_marks","previous_gpa",
            "lectures_attended","labs_attended"]

for c in num_cols:
    df_clean = df_clean.withColumn(c, col(c).cast(DoubleType()))

# Lọc phạm vi hợp lệ
df_clean = df_clean.filter(
    (col("quiz1_marks")   >= 0) & (col("quiz1_marks")   <= 10) &
    (col("quiz2_marks")   >= 0) & (col("quiz2_marks")   <= 10) &
    (col("quiz3_marks")   >= 0) & (col("quiz3_marks")   <= 10) &
    (col("midterm_marks") >= 0) & (col("midterm_marks") <= 30) &
    (col("final_marks")   >= 0) & (col("final_marks")   <= 50)
)

print(f"✅ Sau khi làm sạch: {df_clean.count()} rows")

# COMMAND ----------

# MAGIC %md ## 3. Tính điểm tổng kết (thang 100)
# MAGIC
# MAGIC | Thành phần | Tỷ lệ | Điểm tối đa gốc | Quy về % |
# MAGIC |---|---|---|---|
# MAGIC | Quiz 1+2+3 | 30% | 10 mỗi quiz | (q1+q2+q3)/30 × 30 |
# MAGIC | Midterm    | 20% | 30 điểm      | midterm/30 × 20    |
# MAGIC | Final      | 50% | 50 điểm      | final/50 × 50      |

# COMMAND ----------

df_silver = df_clean.withColumn(
    "quiz_total",
    spark_round((col("quiz1_marks") + col("quiz2_marks") + col("quiz3_marks")) / 30.0 * 30, 2)
).withColumn(
    "midterm_pct",
    spark_round(col("midterm_marks") / 30.0 * 20, 2)
).withColumn(
    "final_pct",
    spark_round(col("final_marks") / 50.0 * 50, 2)
).withColumn(
    "total_score",
    spark_round(
        col("quiz_total") + col("midterm_pct") + col("final_pct"),
        2
    )
).withColumn(
    "grade_10",
    spark_round(col("total_score") / 10.0, 2)
).withColumn("_processed_at", current_timestamp()) \
 .drop("_ingested_at", "_source")

print(f"✅ Silver records: {df_silver.count()}")
display(df_silver.select(
    "student_id","name",
    "quiz1_marks","quiz2_marks","quiz3_marks","quiz_total",
    "midterm_marks","midterm_pct",
    "final_marks","final_pct",
    "total_score","grade_10"
).limit(10))

# COMMAND ----------

# MAGIC %md ## 4. Lưu Silver layer

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/delta/silver/students_clean")

print("✅ Silver saved to: /delta/silver/students_clean")

# COMMAND ----------

# Verify
silver_check = spark.read.format("delta").load("/delta/silver/students_clean")
print(f"📦 Silver row count: {silver_check.count()}")
display(silver_check.describe(["total_score","grade_10","previous_gpa"]))

# COMMAND ----------

from delta.tables import DeltaTable
dt_silver = DeltaTable.forPath(spark, "/delta/silver/students_clean")
display(dt_silver.history())

# COMMAND ----------

print("✅ Silver layer hoàn thành!")
print("➡️  Tiếp theo: chạy notebook 04_gold_analytics.py")
