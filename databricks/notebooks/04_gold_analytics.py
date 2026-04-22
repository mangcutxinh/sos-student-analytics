# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Gold Layer (Analytics & GPA)
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Gold layer tạo các bảng phân tích sẵn sàng cho API:
# MAGIC - **student_gpa**: điểm tổng kết, xếp loại, cờ cảnh báo học yếu
# MAGIC - **score_distribution**: phân phối điểm theo xếp loại
# MAGIC - **attendance_impact**: tương quan điểm danh vs điểm số

# COMMAND ----------

# MAGIC %md ## 1. Đọc Silver

# COMMAND ----------

from pyspark.sql.functions import (
    col, round as spark_round, when, avg, count,
    min, max, stddev, corr, sum as spark_sum,
    current_timestamp
)

df_silver = spark.read.format("delta").load("/delta/silver/students_clean")
print(f"📦 Silver input: {df_silver.count()} rows")

# COMMAND ----------

# MAGIC %md ## 2. Bảng Gold 1 – student_gpa (xếp loại từng sinh viên)

# COMMAND ----------

df_gpa = df_silver.withColumn(
    "letter_grade",
    when(col("grade_10") >= 8.5, "A - Xuất sắc")
   .when(col("grade_10") >= 7.0, "B - Giỏi")
   .when(col("grade_10") >= 5.5, "C - Khá")
   .when(col("grade_10") >= 4.0, "D - Trung bình")
   .otherwise("F - Yếu / Không đạt")
).withColumn(
    "pass_fail",
    when(col("grade_10") >= 4.0, "Pass").otherwise("Fail")
).withColumn(
    "at_risk",
    when(
        (col("grade_10") < 5.0) |
        (col("lectures_attended") < 3) |
        (col("labs_attended") < 2),
        True
    ).otherwise(False)
).withColumn(
    "gpa_trend",
    when(col("grade_10") > col("previous_gpa"), "↑ Cải thiện")
   .when(col("grade_10") < col("previous_gpa"), "↓ Giảm sút")
   .otherwise("→ Ổn định")
).select(
    "student_id","name","age","gender",
    "quiz_total","midterm_pct","final_pct","total_score","grade_10",
    "previous_gpa","letter_grade","pass_fail","at_risk","gpa_trend",
    "lectures_attended","labs_attended"
)

df_gpa.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save("/delta/gold/student_gpa")

print(f"✅ student_gpa saved: {df_gpa.count()} rows")
display(df_gpa.limit(15))

# COMMAND ----------

# Tóm tắt phân loại
print("📊 Phân bổ sinh viên theo xếp loại:")
display(
    df_gpa.groupBy("letter_grade")
          .agg(count("*").alias("so_sinh_vien"))
          .orderBy("letter_grade")
)

print("\n🚨 Sinh viên có nguy cơ học yếu (at_risk = True):")
at_risk_count = df_gpa.filter(col("at_risk") == True).count()
total = df_gpa.count()
print(f"   {at_risk_count} / {total} sinh viên ({round(at_risk_count/total*100,1)}%)")

# COMMAND ----------

# MAGIC %md ## 3. Bảng Gold 2 – score_distribution (thống kê tổng hợp)

# COMMAND ----------

df_dist = df_gpa.agg(
    count("*").alias("total_students"),
    spark_round(avg("grade_10"), 2).alias("avg_grade"),
    spark_round(min("grade_10"), 2).alias("min_grade"),
    spark_round(max("grade_10"), 2).alias("max_grade"),
    spark_round(stddev("grade_10"), 2).alias("stddev_grade"),
    count(when(col("pass_fail") == "Pass", 1)).alias("pass_count"),
    count(when(col("pass_fail") == "Fail", 1)).alias("fail_count"),
    count(when(col("at_risk") == True, 1)).alias("at_risk_count"),
)

df_dist.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save("/delta/gold/score_distribution")

print("✅ score_distribution saved")
display(df_dist)

# COMMAND ----------

# MAGIC %md ## 4. Bảng Gold 3 – attendance_impact (tương quan điểm danh vs điểm)

# COMMAND ----------

df_att = df_silver.groupBy("lectures_attended","labs_attended").agg(
    count("*").alias("student_count"),
    spark_round(avg("grade_10"), 2).alias("avg_grade"),
    spark_round(avg("total_score"), 2).alias("avg_total_score"),
)

df_att.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save("/delta/gold/attendance_impact")

print("✅ attendance_impact saved")

# Hệ số tương quan
corr_lec = df_silver.stat.corr("lectures_attended", "grade_10")
corr_lab = df_silver.stat.corr("labs_attended", "grade_10")
print(f"\n📈 Tương quan lectures_attended ↔ grade_10 : {round(corr_lec,3)}")
print(f"📈 Tương quan labs_attended    ↔ grade_10 : {round(corr_lab,3)}")

display(df_att.orderBy("avg_grade", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md ## 5. Xác minh toàn bộ Gold tables

# COMMAND ----------

for path, name in [
    ("/delta/gold/student_gpa",       "student_gpa"),
    ("/delta/gold/score_distribution","score_distribution"),
    ("/delta/gold/attendance_impact", "attendance_impact"),
]:
    df_check = spark.read.format("delta").load(path)
    print(f"✅ {name}: {df_check.count()} rows, {len(df_check.columns)} cols")

# COMMAND ----------

from delta.tables import DeltaTable
dt_gold = DeltaTable.forPath(spark, "/delta/gold/student_gpa")
display(dt_gold.history())

# COMMAND ----------

print("🎉 GOLD LAYER HOÀN THÀNH!")
print("=" * 50)
print("Các bảng đã tạo:")
print("  /delta/gold/student_gpa        → xếp loại từng SV")
print("  /delta/gold/score_distribution → thống kê tổng hợp")
print("  /delta/gold/attendance_impact  → tương quan điểm danh")
