# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver Layer
# MAGIC **Pipeline:** Delta Bronze → Clean / Enrich → Delta Silver
# MAGIC
# MAGIC Silver = deduplicated, typed, business-rule enriched data.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
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

dedup_window = Window.partitionBy("student_id","subject","semester") \
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
    # ── cast date ──────────────────────────────────────────────────────────────
    .withColumn("exam_date", F.to_date(F.col("exam_date"), "yyyy-MM-dd"))

    # ── normalise strings ──────────────────────────────────────────────────────
    .withColumn("full_name", F.trim(F.initcap(F.col("full_name"))))
    .withColumn("major",     F.trim(F.upper(F.col("major"))))
    .withColumn("subject",   F.trim(F.initcap(F.col("subject"))))
    .withColumn("grade",     F.trim(F.upper(F.col("grade"))))

    # ── clamp scores to [0, 10] ────────────────────────────────────────────────
    .withColumn("midterm_score",   F.least(F.greatest(F.col("midterm_score"),   F.lit(0.0)), F.lit(10.0)))
    .withColumn("final_score",     F.least(F.greatest(F.col("final_score"),     F.lit(0.0)), F.lit(10.0)))
    .withColumn("attendance_rate", F.least(F.greatest(F.col("attendance_rate"), F.lit(0.0)), F.lit(1.0)))

    # ── drop raw staging meta ──────────────────────────────────────────────────
    .drop("_ingestion_ts","_source_file","_pipeline_stage","_semester")
)

# COMMAND ----------
# MAGIC %md ## 4 · Business Enrichment

# COMMAND ----------
enriched_df = (
    cleaned_df

    # ── re-calculate GPA (authoritative formula) ───────────────────────────────
    .withColumn("gpa_calculated",
        F.round(
            (F.col("midterm_score") * 0.30 + F.col("final_score") * 0.70)
            * F.col("attendance_rate"),
            2
        )
    )

    # ── performance tier ───────────────────────────────────────────────────────
    .withColumn("performance_tier",
        F.when(F.col("gpa_calculated") >= 8.5, "Excellent")
        .when(F.col("gpa_calculated") >= 7.0,  "Good")
        .when(F.col("gpa_calculated") >= 5.5,  "Average")
        .when(F.col("gpa_calculated") >= 4.0,  "Below Average")
        .otherwise("Fail")
    )

    # ── attendance category ────────────────────────────────────────────────────
    .withColumn("attendance_category",
        F.when(F.col("attendance_rate") >= 0.9, "Full")
        .when(F.col("attendance_rate") >= 0.75, "Regular")
        .when(F.col("attendance_rate") >= 0.5,  "Irregular")
        .otherwise("Absent Risk")
    )

    # ── improvement flag (final > midterm by 1.5+ points) ─────────────────────
    .withColumn("is_improved",
        (F.col("final_score") - F.col("midterm_score")) >= 1.5
    )

    # ── exam month / year for time analysis ───────────────────────────────────
    .withColumn("exam_month", F.month(F.col("exam_date")))
    .withColumn("exam_year",  F.year(F.col("exam_date")))

    # ── silver metadata ────────────────────────────────────────────────────────
    .withColumn("silver_processed_at", F.current_timestamp())
)

print(f"✅ Enriched rows: {enriched_df.count()}")
enriched_df.select(
    "student_id","full_name","subject","gpa_calculated",
    "performance_tier","attendance_category","is_improved"
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
            """tgt.student_id = src.student_id
               AND tgt.subject = src.subject
               AND tgt.semester = src.semester"""
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
        .partitionBy("semester", "performance_tier")
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
print(f"  Total rows     : {silver_df.count()}")
print(f"  Null exam_date : {silver_df.filter(F.col('exam_date').isNull()).count()}")

print("\n── Performance Tier Distribution ──")
silver_df.groupBy("performance_tier").count() \
         .orderBy(F.col("count").desc()).show()

print("\n── GPA Descriptive Stats ──")
silver_df.select(
    F.round(F.min("gpa_calculated"),2).alias("min_gpa"),
    F.round(F.avg("gpa_calculated"),2).alias("avg_gpa"),
    F.round(F.max("gpa_calculated"),2).alias("max_gpa"),
    F.round(F.stddev("gpa_calculated"),2).alias("stddev_gpa"),
).show()

print("\n✅ 03_silver_layer  DONE")
