# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Data Ingestion
# MAGIC **Pipeline:** Read CSV → Validate → Stage for Bronze
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Mount / locate source CSV |
# MAGIC | 2 | Read with explicit schema |
# MAGIC | 3 | Row-level validation |
# MAGIC | 4 | Write staging Parquet → DBFS |

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("01_data_ingestion")

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------
# MAGIC %md ## Config

# COMMAND ----------
# ── paths ──────────────────────────────────────────────────────────────────────
# On Databricks Community Edition:  upload the CSV to DBFS via UI first, then set:
#   SOURCE_PATH = "dbfs:/FileStore/student_score_dataset.csv"
# Locally (unit tests / CI):
#   SOURCE_PATH = "data/mock/student_score_dataset.csv"

SOURCE_PATH   = "dbfs:/FileStore/student_score_dataset.csv"   # ← change if needed
STAGING_PATH  = "dbfs:/delta/staging/student_scores"
SEMESTER      = "2024-1"

# COMMAND ----------
# MAGIC %md ## 1 · Explicit Schema

# COMMAND ----------
SCHEMA = StructType([
    StructField("student_id",      StringType(),  nullable=False),
    StructField("full_name",       StringType(),  nullable=True),
    StructField("major",           StringType(),  nullable=True),
    StructField("year_of_study",   IntegerType(), nullable=True),
    StructField("subject",         StringType(),  nullable=True),
    StructField("midterm_score",   DoubleType(),  nullable=True),
    StructField("final_score",     DoubleType(),  nullable=True),
    StructField("attendance_rate", DoubleType(),  nullable=True),
    StructField("gpa",             DoubleType(),  nullable=True),
    StructField("grade",           StringType(),  nullable=True),
    StructField("exam_date",       StringType(),  nullable=True),   # cast later
    StructField("semester",        StringType(),  nullable=True),
])

# COMMAND ----------
# MAGIC %md ## 2 · Read CSV

# COMMAND ----------
logger.info(f"Reading CSV from: {SOURCE_PATH}")

raw_df = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-8")
    .schema(SCHEMA)
    .csv(SOURCE_PATH)
)

total_rows = raw_df.count()
logger.info(f"Total rows read: {total_rows}")
print(f"✅ Loaded {total_rows} rows")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ## 3 · Validation

# COMMAND ----------
def validate(df):
    """Return (valid_df, invalid_df, summary_dict)."""
    # ── rules ──────────────────────────────────────────────────────────────────
    validated = df.withColumn(
        "_errors",
        F.array(
            F.when(F.col("student_id").isNull(),                        F.lit("missing student_id")),
            F.when(F.col("midterm_score").isNull() |
                   (F.col("midterm_score") < 0) |
                   (F.col("midterm_score") > 10),                        F.lit("invalid midterm_score")),
            F.when(F.col("final_score").isNull() |
                   (F.col("final_score") < 0) |
                   (F.col("final_score") > 10),                          F.lit("invalid final_score")),
            F.when(F.col("attendance_rate").isNull() |
                   (F.col("attendance_rate") < 0) |
                   (F.col("attendance_rate") > 1),                       F.lit("invalid attendance_rate")),
            F.when(F.col("year_of_study").isNull() |
                   (F.col("year_of_study") < 1) |
                   (F.col("year_of_study") > 6),                         F.lit("invalid year_of_study")),
            F.when(~F.col("grade").isin("A","B","C","D","F"),            F.lit("invalid grade")),
        )
    ).withColumn(
        "_errors", F.array_compact(F.col("_errors"))   # drop nulls
    ).withColumn(
        "_is_valid", F.size(F.col("_errors")) == 0
    )

    valid_df   = validated.filter(F.col("_is_valid")).drop("_errors","_is_valid")
    invalid_df = validated.filter(~F.col("_is_valid"))

    v_count = valid_df.count()
    i_count = invalid_df.count()
    summary = {
        "total":   total_rows,
        "valid":   v_count,
        "invalid": i_count,
        "pass_rate": round(v_count / total_rows * 100, 2) if total_rows else 0,
    }
    return valid_df, invalid_df, summary

valid_df, invalid_df, summary = validate(raw_df)

print("=" * 50)
print("VALIDATION SUMMARY")
print("=" * 50)
for k, v in summary.items():
    print(f"  {k:<12}: {v}")
print("=" * 50)

if summary["invalid"] > 0:
    print("\n⚠️  Invalid rows sample:")
    invalid_df.select("student_id","_errors").show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## 4 · Add Ingestion Metadata & Write Staging

# COMMAND ----------
from datetime import datetime

staged_df = valid_df.withColumns({
    "_ingestion_ts":   F.lit(datetime.utcnow().isoformat()),
    "_source_file":    F.lit(SOURCE_PATH),
    "_semester":       F.lit(SEMESTER),
    "_pipeline_stage": F.lit("staging"),
})

(
    staged_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(STAGING_PATH)
)

logger.info(f"Staging written to {STAGING_PATH}")
print(f"\n✅ Staging complete → {STAGING_PATH}")
print(f"   Rows written: {summary['valid']}")

# COMMAND ----------
# MAGIC %md ## 5 · Quick Profile

# COMMAND ----------
print("\n── Score distribution ──")
valid_df.groupBy("grade").count().orderBy("grade").show()

print("── Missing values ──")
for col_name in valid_df.columns:
    null_count = valid_df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} nulls")

print("\n✅ 01_data_ingestion  DONE")
