# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Bronze Layer (Raw Delta Table)
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Bronze layer lưu **dữ liệu thô** từ nguồn vào Delta Lake.
# MAGIC Không biến đổi, chỉ thêm metadata (ingestion timestamp).

# COMMAND ----------

# MAGIC %md ## 1. Đọc lại từ DBFS và thêm metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/student_score_dataset.csv")

# Thêm cột metadata
df_bronze = df_raw \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("student_score_dataset.csv"))

print(f"✅ Bronze records: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## 2. Lưu vào Delta Lake – Bronze

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/delta/bronze/students")

print("✅ Bronze layer saved to: /delta/bronze/students")

# COMMAND ----------

# MAGIC %md ## 3. Xác minh Delta table

# COMMAND ----------

bronze_check = spark.read.format("delta").load("/delta/bronze/students")
print(f"📦 Bronze row count: {bronze_check.count()}")
print(f"📋 Columns: {bronze_check.columns}")
display(bronze_check.limit(10))

# COMMAND ----------

# MAGIC %md ## 4. Xem Delta history

# COMMAND ----------

from delta.tables import DeltaTable

dt_bronze = DeltaTable.forPath(spark, "/delta/bronze/students")
display(dt_bronze.history())

# COMMAND ----------

print("✅ Bronze layer hoàn thành!")
print("➡️  Tiếp theo: chạy notebook 03_silver_layer.py")
