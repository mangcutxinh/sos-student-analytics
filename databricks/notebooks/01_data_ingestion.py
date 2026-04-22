# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 01 – Data Ingestion
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Notebook này thực hiện:
# MAGIC - Đọc file CSV từ DBFS
# MAGIC - Kiểm tra schema và chất lượng dữ liệu
# MAGIC - Preview dữ liệu đầu vào

# COMMAND ----------

# MAGIC %md ## 1. Đọc dữ liệu từ DBFS

# COMMAND ----------

file_path = "/Volumes/main/default/sos_data/student_score_dataset.csv"
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

print(f"✅ Đọc thành công: {df_raw.count()} sinh viên")
print(f"📋 Số cột: {len(df_raw.columns)}")

# COMMAND ----------

# MAGIC %md ## 2. Kiểm tra schema

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

# MAGIC %md ## 3. Preview dữ liệu

# COMMAND ----------

display(df_raw)

# COMMAND ----------

# MAGIC %md ## 4. Kiểm tra chất lượng dữ liệu (null, kiểu dữ liệu)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Đếm null từng cột
null_counts = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_raw.columns
])
print("🔍 Số lượng NULL theo cột:")
display(null_counts)

# COMMAND ----------

# Thống kê mô tả
print("📊 Thống kê mô tả các cột số:")
display(df_raw.describe())

# COMMAND ----------

# Kiểm tra phạm vi điểm hợp lệ
invalid_quiz   = df_raw.filter((col("quiz1_marks") < 0) | (col("quiz1_marks") > 10))
invalid_mid    = df_raw.filter((col("midterm_marks") < 0) | (col("midterm_marks") > 30))
invalid_final  = df_raw.filter((col("final_marks") < 0) | (col("final_marks") > 50))

print(f"⚠️  Quiz không hợp lệ  : {invalid_quiz.count()} dòng")
print(f"⚠️  Midterm không hợp lệ: {invalid_mid.count()} dòng")
print(f"⚠️  Final không hợp lệ  : {invalid_final.count()} dòng")

# COMMAND ----------

# MAGIC %md ## 5. Lưu dữ liệu tạm để các notebook sau sử dụng

# COMMAND ----------

# Lưu vào view tạm thời để dùng trong session
df_raw.createOrReplaceTempView("raw_students")
print("✅ Đã tạo temp view: raw_students")
print("➡️  Tiếp theo: chạy notebook 02_bronze_layer.py")
