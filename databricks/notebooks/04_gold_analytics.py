# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold Analytics Layer
# MAGIC **Pipeline:** Silver → Aggregations → ML Features → Delta Gold
# MAGIC
# MAGIC Gold tables are consumption-ready:  BI dashboards, API queries, ML models.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("04_gold_analytics")

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
SILVER_PATH      = "dbfs:/delta/silver/student_scores"
GOLD_PATH_GPA    = "dbfs:/delta/gold/student_gpa_summary"
GOLD_PATH_MAJOR  = "dbfs:/delta/gold/major_analytics"
GOLD_PATH_SUBJ   = "dbfs:/delta/gold/subject_analytics"
GOLD_PATH_ML     = "dbfs:/delta/gold/ml_features"

# COMMAND ----------
# MAGIC %md ## 1 · Read Silver

# COMMAND ----------
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"✅ Silver rows: {silver.count()}")

# COMMAND ----------
# MAGIC %md ## 2 · Gold Table A — Student GPA Summary

# COMMAND ----------
# Per-student across all subjects in semester
student_window = Window.partitionBy("student_id","semester")

gpa_summary = (
    silver
    .groupBy("student_id","full_name","major","year_of_study","semester")
    .agg(
        F.round(F.avg("gpa_calculated"),    2).alias("avg_gpa"),
        F.round(F.avg("midterm_score"),     2).alias("avg_midterm"),
        F.round(F.avg("final_score"),       2).alias("avg_final"),
        F.round(F.avg("attendance_rate"),   2).alias("avg_attendance"),
        F.count("subject")                   .alias("subjects_count"),
        F.sum(F.col("is_improved").cast("int")).alias("subjects_improved"),
        F.collect_list("grade")              .alias("all_grades"),
    )
    .withColumn("overall_grade",
        F.when(F.col("avg_gpa") >= 8.5, "A")
        .when(F.col("avg_gpa") >= 7.0,  "B")
        .when(F.col("avg_gpa") >= 5.5,  "C")
        .when(F.col("avg_gpa") >= 4.0,  "D")
        .otherwise("F")
    )
    .withColumn("rank_in_semester",
        F.rank().over(
            Window.partitionBy("semester").orderBy(F.col("avg_gpa").desc())
        )
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

gpa_summary.write.format("delta").mode("overwrite") \
           .option("overwriteSchema","true").save(GOLD_PATH_GPA)
print(f"✅ Gold GPA summary → {GOLD_PATH_GPA}  ({gpa_summary.count()} rows)")
gpa_summary.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ## 3 · Gold Table B — Major Analytics

# COMMAND ----------
major_analytics = (
    silver
    .groupBy("major","semester")
    .agg(
        F.count("student_id")                .alias("student_count"),
        F.round(F.avg("gpa_calculated"),2)   .alias("avg_gpa"),
        F.round(F.max("gpa_calculated"),2)   .alias("max_gpa"),
        F.round(F.min("gpa_calculated"),2)   .alias("min_gpa"),
        F.round(F.stddev("gpa_calculated"),2).alias("gpa_stddev"),
        F.round(F.avg("attendance_rate"),2)  .alias("avg_attendance"),
        # grade distribution
        F.round(F.sum(F.when(F.col("grade")=="A",1).otherwise(0)) / F.count("*") * 100, 1).alias("pct_grade_A"),
        F.round(F.sum(F.when(F.col("grade")=="F",1).otherwise(0)) / F.count("*") * 100, 1).alias("pct_fail"),
        F.round(F.sum(F.col("is_improved").cast("int")) / F.count("*") * 100, 1).alias("pct_improved"),
    )
    .withColumn("performance_rank",
        F.rank().over(Window.partitionBy("semester").orderBy(F.col("avg_gpa").desc()))
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

major_analytics.write.format("delta").mode("overwrite") \
               .option("overwriteSchema","true").save(GOLD_PATH_MAJOR)
print(f"✅ Gold major analytics → {GOLD_PATH_MAJOR}")
major_analytics.orderBy(F.col("avg_gpa").desc()).show(10)

# COMMAND ----------
# MAGIC %md ## 4 · Gold Table C — Subject Analytics

# COMMAND ----------
subject_analytics = (
    silver
    .groupBy("subject","semester")
    .agg(
        F.count("student_id")                .alias("enrollments"),
        F.round(F.avg("gpa_calculated"),2)   .alias("avg_gpa"),
        F.round(F.avg("midterm_score"),2)    .alias("avg_midterm"),
        F.round(F.avg("final_score"),2)      .alias("avg_final"),
        F.round(F.avg("attendance_rate"),2)  .alias("avg_attendance"),
        F.round(F.sum(F.when(F.col("grade")=="A",1).otherwise(0)) / F.count("*") * 100, 1).alias("pass_A_rate"),
        F.round(F.sum(F.when(F.col("grade")=="F",1).otherwise(0)) / F.count("*") * 100, 1).alias("fail_rate"),
        F.round(F.corr("midterm_score","final_score"),3).alias("mid_final_corr"),
    )
    .withColumn("difficulty_label",
        F.when(F.col("fail_rate") >= 30, "Hard")
        .when(F.col("fail_rate") >= 15,  "Medium")
        .otherwise("Easy")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

subject_analytics.write.format("delta").mode("overwrite") \
                 .option("overwriteSchema","true").save(GOLD_PATH_SUBJ)
print(f"✅ Gold subject analytics → {GOLD_PATH_SUBJ}")
subject_analytics.orderBy("fail_rate", ascending=False).show()

# COMMAND ----------
# MAGIC %md ## 5 · Gold Table D — ML Feature Store

# COMMAND ----------
# Aggregate per-student features for ML (e.g., dropout prediction)
ml_features = (
    silver
    .groupBy("student_id","semester")
    .agg(
        F.avg("midterm_score")                       .alias("feat_avg_midterm"),
        F.avg("final_score")                         .alias("feat_avg_final"),
        F.avg("attendance_rate")                     .alias("feat_avg_attendance"),
        F.avg("gpa_calculated")                      .alias("feat_avg_gpa"),
        F.stddev("gpa_calculated")                   .alias("feat_gpa_stddev"),
        F.count("subject")                           .alias("feat_subject_count"),
        F.sum(F.col("is_improved").cast("int"))      .alias("feat_improved_count"),
        F.sum(F.when(F.col("grade")=="F",1).otherwise(0)).alias("feat_fail_count"),
        F.max("year_of_study")                       .alias("feat_year_of_study"),
        F.first("major")                             .alias("major"),
    )
    # ── label: at-risk if avg_gpa < 5.5 OR fail_count >= 2 ───────────────────
    .withColumn("label_at_risk",
        ((F.col("feat_avg_gpa") < 5.5) | (F.col("feat_fail_count") >= 2)).cast("int")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

ml_features.write.format("delta").mode("overwrite") \
           .option("overwriteSchema","true").save(GOLD_PATH_ML)
print(f"✅ Gold ML features → {GOLD_PATH_ML}")

at_risk = ml_features.filter(F.col("label_at_risk")==1).count()
total   = ml_features.count()
print(f"   At-risk students : {at_risk} / {total}  ({round(at_risk/total*100,1)}%)")

# COMMAND ----------
# MAGIC %md ## 6 · Register all Gold Tables

# COMMAND ----------
tables = {
    "gold_student_gpa_summary":  GOLD_PATH_GPA,
    "gold_major_analytics":      GOLD_PATH_MAJOR,
    "gold_subject_analytics":    GOLD_PATH_SUBJ,
    "gold_ml_features":          GOLD_PATH_ML,
}
for tbl_name, path in tables.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tbl_name}
        USING DELTA LOCATION '{path}'
    """)
    print(f"  ✅ Registered: {tbl_name}")

# COMMAND ----------
# MAGIC %md ## 7 · Final Summary Dashboard Query

# COMMAND ----------
print("\n══════════════════════════════════════════════")
print("  GOLD LAYER  —  FINAL SUMMARY")
print("══════════════════════════════════════════════")

spark.sql("""
    SELECT
        overall_grade,
        COUNT(*)                      AS students,
        ROUND(AVG(avg_gpa),2)        AS mean_gpa,
        ROUND(AVG(avg_attendance),2) AS mean_attendance
    FROM gold_student_gpa_summary
    GROUP BY overall_grade
    ORDER BY overall_grade
""").show()

print("\n── Top 5 Majors by GPA ──")
spark.sql("""
    SELECT major, avg_gpa, student_count, pct_fail
    FROM gold_major_analytics
    ORDER BY avg_gpa DESC
    LIMIT 5
""").show()

print("\n✅ 04_gold_analytics  DONE")
