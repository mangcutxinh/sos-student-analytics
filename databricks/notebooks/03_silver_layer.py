# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver Layer
# MAGIC **Pipeline:** Delta Bronze → Clean / Enrich → Delta Silver
# MAGIC
# MAGIC Silver = deduplicated, typed, business-rule enriched data.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# (DateType no longer needed after schema alignment)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("03_silver_layer")

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
BRONZE_PATH = "dbfs:/delta/bronze/student_scores"
SILVER_PATH = "dbfs:/delta/silver/student_scores"
SILVER_TABLE = "silver_student_scores"

# COMMAND ----------
# MAGIC %md ## 1 · Read Bronze

# COMMAND ----------
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"✅ Bronze rows: {bronze_df.count()}")

# COMMAND ----------
# MAGIC %md ## 2 · Deduplication

# COMMAND ----------
from pyspark.sql.window import Window

dedup_window = Window.partitionBy("student_id", "semester") \
                     .orderBy(F.col("bronze_loaded_at").desc())

deduped_df = (
    bronze_df
    .withColumn("_rank", F.row_number().over(dedup_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

removed = bronze_df.count() - deduped_df.count()
print(f"  Duplicates removed : {removed}")
print(f"  Rows after dedup   : {deduped_df.count()}")

# COMMAND ----------
# MAGIC %md ## 3 · Type Casting & Cleaning

# COMMAND ----------
cleaned_df = (
    deduped_df
    # ── normalise strings ──────────────────────────────────────────────────────
    .withColumn("name",   F.trim(F.initcap(F.col("name"))))
    .withColumn("gender", F.trim(F.initcap(F.col("gender"))))   # Male / Female

    # ── clamp marks to documented ranges ──────────────────────────────────────
    .withColumn("quiz1_marks",       F.least(F.greatest(F.col("quiz1_marks"),       F.lit(0.0)), F.lit(10.0)))
    .withColumn("quiz2_marks",       F.least(F.greatest(F.col("quiz2_marks"),       F.lit(0.0)), F.lit(10.0)))
    .withColumn("quiz3_marks",       F.least(F.greatest(F.col("quiz3_marks"),       F.lit(0.0)), F.lit(10.0)))
    .withColumn("midterm_marks",     F.least(F.greatest(F.col("midterm_marks"),     F.lit(0.0)), F.lit(30.0)))
    .withColumn("final_marks",       F.least(F.greatest(F.col("final_marks"),       F.lit(0.0)), F.lit(50.0)))
    .withColumn("previous_gpa",      F.least(F.greatest(F.col("previous_gpa"),      F.lit(0.0)), F.lit(4.0)))
    .withColumn("lectures_attended", F.greatest(F.col("lectures_attended").cast("int"), F.lit(0)))
    .withColumn("labs_attended",     F.greatest(F.col("labs_attended").cast("int"),     F.lit(0)))

    # ── drop raw staging meta ──────────────────────────────────────────────────
    .drop("_ingestion_ts", "_source_file", "_pipeline_stage", "_semester")
)

# COMMAND ----------
# MAGIC %md ## 4 · Business Enrichment

# COMMAND ----------
# ── constants ──────────────────────────────────────────────────────────────────
MAX_SESSIONS = 20   # 10 lecture sessions + 10 lab sessions per semester; adjust to match your curriculum

enriched_df = (
    cleaned_df

    # ── quiz total (max 30) ────────────────────────────────────────────────────
    .withColumn("quiz_total_30",
        F.round(F.col("quiz1_marks") + F.col("quiz2_marks") + F.col("quiz3_marks"), 2)
    )

    # ── weighted score components ──────────────────────────────────────────────
    .withColumn("midterm_component_20",
        F.round((F.col("midterm_marks") / 30.0) * 20.0, 2)
    )
    .withColumn("final_component_50",
        F.round((F.col("final_marks") / 50.0) * 50.0, 2)
    )

    # ── total score (0–100) and grade on 10-point scale ───────────────────────
    .withColumn("total_score_100",
        F.round(
            F.col("quiz_total_30") + F.col("midterm_component_20") + F.col("final_component_50"),
            2
        )
    )
    .withColumn("grade_10",
        F.round(F.col("total_score_100") / 10.0, 2)
    )

    # ── estimated 4.0-scale GPA and delta vs previous semester ────────────────
    .withColumn("gpa_est_4",
        F.round((F.col("grade_10") / 10.0) * 4.0, 2)
    )
    .withColumn("gpa_delta",
        F.round(F.col("gpa_est_4") - F.col("previous_gpa"), 2)
    )

    # ── attendance features ────────────────────────────────────────────────────
    .withColumn("attendance_total",
        F.col("lectures_attended") + F.col("labs_attended")
    )
    .withColumn("attendance_ratio",
        F.round(F.col("attendance_total") / F.lit(MAX_SESSIONS), 3)
    )

    # ── letter grade from grade_10 ─────────────────────────────────────────────
    .withColumn("letter_grade",
        F.when(F.col("grade_10") >= 8.5, "A")
        .when(F.col("grade_10") >= 7.0,  "B")
        .when(F.col("grade_10") >= 5.5,  "C")
        .when(F.col("grade_10") >= 4.0,  "D")
        .otherwise("F")
    )

    # ── performance tier ───────────────────────────────────────────────────────
    .withColumn("performance_tier",
        F.when(F.col("grade_10") >= 8.5, "Excellent")
        .when(F.col("grade_10") >= 7.0,  "Good")
        .when(F.col("grade_10") >= 5.5,  "Average")
        .when(F.col("grade_10") >= 4.0,  "Below Average")
        .otherwise("Fail")
    )

    # ── attendance category ────────────────────────────────────────────────────
    .withColumn("attendance_category",
        F.when(F.col("attendance_ratio") >= 0.9,  "Full")
        .when(F.col("attendance_ratio") >= 0.75, "Regular")
        .when(F.col("attendance_ratio") >= 0.5,  "Irregular")
        .otherwise("Absent Risk")
    )

    # ── improvement flag (current estimated GPA exceeds previous semester GPA) ─
    .withColumn("is_improved", F.col("gpa_delta") > 0)

    # ── silver metadata ────────────────────────────────────────────────────────
    .withColumn("silver_processed_at", F.current_timestamp())
)

print(f"✅ Enriched rows: {enriched_df.count()}")
enriched_df.select(
    "student_id", "name", "total_score_100", "grade_10",
    "performance_tier", "attendance_category", "is_improved"
).show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## 5 · Write Delta Silver (SCD Type 1 MERGE)

# COMMAND ----------
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("tgt")
        .merge(
            enriched_df.alias("src"),
            "tgt.student_id = src.student_id AND tgt.semester = src.semester"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ Silver MERGE complete")
else:
    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("semester", "letter_grade")
        .save(SILVER_PATH)
    )
    print(f"✅ Silver table created: {SILVER_PATH}")

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA LOCATION '{SILVER_PATH}'
""")

# COMMAND ----------
# MAGIC %md ## 6 · Quality Checks

# COMMAND ----------
silver_df = spark.read.format("delta").load(SILVER_PATH)

print("\n── Silver Stats ─────────────────────────────")
print(f"  Total rows        : {silver_df.count()}")

print("\n── Performance Tier Distribution ──")
silver_df.groupBy("performance_tier").count() \
         .orderBy(F.col("count").desc()).show()

print("\n── Grade 10 Descriptive Stats ──")
silver_df.select(
    F.round(F.min("grade_10"),    2).alias("min_grade_10"),
    F.round(F.avg("grade_10"),    2).alias("avg_grade_10"),
    F.round(F.max("grade_10"),    2).alias("max_grade_10"),
    F.round(F.stddev("grade_10"), 2).alias("stddev_grade_10"),
).show()

print("\n✅ 03_silver_layer  DONE")
