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
GOLD_PATH_COHORT = "dbfs:/delta/gold/cohort_analytics"
GOLD_PATH_ATTEND = "dbfs:/delta/gold/attendance_analytics"
GOLD_PATH_ML     = "dbfs:/delta/gold/ml_features"

# COMMAND ----------
# MAGIC %md ## 1 · Read Silver

# COMMAND ----------
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"✅ Silver rows: {silver.count()}")

# COMMAND ----------
# MAGIC %md ## 2 · Gold Table A — Student Score Summary

# COMMAND ----------
# Per-student summary for the semester
gpa_summary = (
    silver
    .groupBy("student_id", "name", "gender", "age", "semester")
    .agg(
        F.round(F.avg("grade_10"),          2).alias("avg_grade_10"),
        F.round(F.avg("total_score_100"),   2).alias("avg_total_score"),
        F.round(F.avg("quiz_total_30"),     2).alias("avg_quiz_total"),
        F.round(F.avg("midterm_marks"),     2).alias("avg_midterm"),
        F.round(F.avg("final_marks"),       2).alias("avg_final"),
        F.round(F.avg("attendance_ratio"),  2).alias("avg_attendance_ratio"),
        F.round(F.avg("gpa_est_4"),         2).alias("avg_gpa_est_4"),
        F.round(F.avg("gpa_delta"),         2).alias("avg_gpa_delta"),
        F.first("previous_gpa")               .alias("previous_gpa"),
        F.first("performance_tier")           .alias("performance_tier"),
        F.first("letter_grade")               .alias("overall_letter_grade"),
        F.sum(F.col("is_improved").cast("int")).alias("semesters_improved"),
    )
    .withColumn("overall_grade",
        F.when(F.col("avg_grade_10") >= 8.5, "A")
        .when(F.col("avg_grade_10") >= 7.0,  "B")
        .when(F.col("avg_grade_10") >= 5.5,  "C")
        .when(F.col("avg_grade_10") >= 4.0,  "D")
        .otherwise("F")
    )
    .withColumn("rank_in_semester",
        F.rank().over(
            Window.partitionBy("semester").orderBy(F.col("avg_grade_10").desc())
        )
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

gpa_summary.write.format("delta").mode("overwrite") \
           .option("overwriteSchema","true").save(GOLD_PATH_GPA)
print(f"✅ Gold student summary → {GOLD_PATH_GPA}  ({gpa_summary.count()} rows)")
gpa_summary.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ## 3 · Gold Table B — Cohort Analytics (semester × gender × age band)

# COMMAND ----------
cohort_analytics = (
    silver
    .withColumn("age_band",
        F.when(F.col("age") <= 19, "≤19")
        .when(F.col("age") <= 21, "20-21")
        .when(F.col("age") <= 23, "22-23")
        .otherwise("24+")
    )
    .groupBy("semester", "gender", "age_band")
    .agg(
        F.count("student_id")                   .alias("student_count"),
        F.round(F.avg("grade_10"),          2)  .alias("avg_grade_10"),
        F.round(F.max("grade_10"),          2)  .alias("max_grade_10"),
        F.round(F.min("grade_10"),          2)  .alias("min_grade_10"),
        F.round(F.stddev("grade_10"),       2)  .alias("grade_10_stddev"),
        F.round(F.avg("attendance_ratio"),  2)  .alias("avg_attendance_ratio"),
        F.round(F.avg("gpa_delta"),         2)  .alias("avg_gpa_delta"),
        # pass / fail rates
        F.round(F.sum(F.when(F.col("letter_grade") == "A", 1).otherwise(0))
                / F.count("*") * 100, 1)        .alias("pct_grade_A"),
        F.round(F.sum(F.when(F.col("letter_grade") == "F", 1).otherwise(0))
                / F.count("*") * 100, 1)        .alias("pct_fail"),
        F.round(F.sum(F.col("is_improved").cast("int"))
                / F.count("*") * 100, 1)        .alias("pct_improved"),
    )
    .withColumn("performance_rank",
        F.rank().over(Window.partitionBy("semester").orderBy(F.col("avg_grade_10").desc()))
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

cohort_analytics.write.format("delta").mode("overwrite") \
                .option("overwriteSchema","true").save(GOLD_PATH_COHORT)
print(f"✅ Gold cohort analytics → {GOLD_PATH_COHORT}")
cohort_analytics.orderBy(F.col("avg_grade_10").desc()).show(10)

# COMMAND ----------
# MAGIC %md ## 4 · Gold Table C — Attendance-Performance Analytics

# COMMAND ----------
attendance_analytics = (
    silver
    .withColumn("attendance_bin",
        F.when(F.col("attendance_ratio") >= 0.9,  "90-100%")
        .when(F.col("attendance_ratio") >= 0.75, "75-89%")
        .when(F.col("attendance_ratio") >= 0.5,  "50-74%")
        .otherwise("<50%")
    )
    .groupBy("semester", "attendance_bin")
    .agg(
        F.count("student_id")                  .alias("student_count"),
        F.round(F.avg("grade_10"),         2)  .alias("avg_grade_10"),
        F.round(F.avg("total_score_100"),  2)  .alias("avg_total_score"),
        F.round(F.avg("quiz_total_30"),    2)  .alias("avg_quiz_total"),
        F.round(F.avg("midterm_marks"),    2)  .alias("avg_midterm"),
        F.round(F.avg("final_marks"),      2)  .alias("avg_final"),
        F.round(F.sum(F.when(F.col("letter_grade") == "A", 1).otherwise(0))
                / F.count("*") * 100, 1)       .alias("pass_A_rate"),
        F.round(F.sum(F.when(F.col("letter_grade") == "F", 1).otherwise(0))
                / F.count("*") * 100, 1)       .alias("fail_rate"),
        F.round(F.corr("attendance_ratio", "grade_10"), 3).alias("attendance_grade_corr"),
    )
    .withColumn("difficulty_label",
        F.when(F.col("fail_rate") >= 30, "High Risk")
        .when(F.col("fail_rate") >= 15,  "Medium Risk")
        .otherwise("Low Risk")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

attendance_analytics.write.format("delta").mode("overwrite") \
                    .option("overwriteSchema","true").save(GOLD_PATH_ATTEND)
print(f"✅ Gold attendance analytics → {GOLD_PATH_ATTEND}")
attendance_analytics.orderBy("semester", "attendance_bin").show()

# COMMAND ----------
# MAGIC %md ## 5 · Gold Table D — ML Feature Store

# COMMAND ----------
# Per-student features for ML (e.g., at-risk / dropout prediction)
ml_features = (
    silver
    .select(
        "student_id", "semester", "name", "age", "gender",
        "quiz1_marks", "quiz2_marks", "quiz3_marks",
        "midterm_marks", "final_marks",
        "quiz_total_30", "midterm_component_20", "final_component_50",
        "total_score_100", "grade_10", "gpa_est_4",
        "previous_gpa", "gpa_delta",
        "lectures_attended", "labs_attended",
        "attendance_total", "attendance_ratio",
        "letter_grade", "performance_tier",
        "is_improved",
    )
    # ── label: at-risk if grade_10 < 5.5 (failing threshold) ─────────────────
    .withColumn("label_at_risk",
        (F.col("grade_10") < 5.5).cast("int")
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
    "gold_cohort_analytics":     GOLD_PATH_COHORT,
    "gold_attendance_analytics": GOLD_PATH_ATTEND,
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
        COUNT(*)                          AS students,
        ROUND(AVG(avg_grade_10),    2)   AS mean_grade_10,
        ROUND(AVG(avg_attendance_ratio),2) AS mean_attendance_ratio
    FROM gold_student_gpa_summary
    GROUP BY overall_grade
    ORDER BY overall_grade
""").show()

print("\n── Cohort GPA by Gender & Age Band ──")
spark.sql("""
    SELECT gender, age_band, avg_grade_10, student_count, pct_fail
    FROM gold_cohort_analytics
    ORDER BY avg_grade_10 DESC
    LIMIT 10
""").show()

print("\n✅ 04_gold_analytics  DONE")
