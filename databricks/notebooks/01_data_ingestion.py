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
    IntegerType, DoubleType
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
#   SOURCE_PATH = "/Volumes/workspace/default/datasets/student_score_dataset.csv"
# Locally (unit tests / CI):
#   SOURCE_PATH = "data/mock/student_score_dataset.csv"

# ── runtime overrides via Databricks widgets (graceful fallback outside Databricks) ───
try:
    SOURCE_PATH = dbutils.widgets.get("source_path")
except Exception:
    SOURCE_PATH = "/Volumes/workspace/default/datasets/student_score_dataset.csv"

try:
    SEMESTER = dbutils.widgets.get("semester")
except Exception:
    SEMESTER = "2024-1"

STAGING_PATH = "/Volumes/workspace/default/staging/student_scores"

# COMMAND ----------

# MAGIC %md ## 1 · Explicit Schema

# COMMAND ----------

SCHEMA = StructType([
    StructField("student_id",        IntegerType(), nullable=False),
    StructField("name",              StringType(),  nullable=True),
    StructField("age",               IntegerType(), nullable=True),
    StructField("gender",            StringType(),  nullable=True),
    StructField("quiz1_marks",       DoubleType(),  nullable=True),
    StructField("quiz2_marks",       DoubleType(),  nullable=True),
    StructField("quiz3_marks",       DoubleType(),  nullable=True),
    StructField("midterm_marks",     DoubleType(),  nullable=True),  # 0–30
    StructField("final_marks",       DoubleType(),  nullable=True),  # 0–50
    StructField("previous_gpa",      DoubleType(),  nullable=True),  # 0–4
    StructField("lectures_attended", IntegerType(), nullable=True),
    StructField("labs_attended",     IntegerType(), nullable=True),
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
            F.when(F.col("student_id").isNull(),
                   F.lit("missing student_id")),
            F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""),
                   F.lit("missing name")),
            F.when(F.col("quiz1_marks").isNull() |
                   (F.col("quiz1_marks") < 0) | (F.col("quiz1_marks") > 10),
                   F.lit("invalid quiz1_marks")),
            F.when(F.col("quiz2_marks").isNull() |
                   (F.col("quiz2_marks") < 0) | (F.col("quiz2_marks") > 10),
                   F.lit("invalid quiz2_marks")),
            F.when(F.col("quiz3_marks").isNull() |
                   (F.col("quiz3_marks") < 0) | (F.col("quiz3_marks") > 10),
                   F.lit("invalid quiz3_marks")),
            F.when(F.col("midterm_marks").isNull() |
                   (F.col("midterm_marks") < 0) | (F.col("midterm_marks") > 30),
                   F.lit("invalid midterm_marks")),
            F.when(F.col("final_marks").isNull() |
                   (F.col("final_marks") < 0) | (F.col("final_marks") > 50),
                   F.lit("invalid final_marks")),
            F.when(F.col("previous_gpa").isNull() |
                   (F.col("previous_gpa") < 0) | (F.col("previous_gpa") > 4),
                   F.lit("invalid previous_gpa")),
            F.when(F.col("lectures_attended").isNull() |
                   (F.col("lectures_attended") < 0),
                   F.lit("invalid lectures_attended")),
            F.when(F.col("labs_attended").isNull() |
                   (F.col("labs_attended") < 0),
                   F.lit("invalid labs_attended")),
        )
    ).withColumn(
        "_errors", F.array_compact(F.col("_errors"))   # drop nulls
    ).withColumn(
        "_is_valid", F.size(F.col("_errors")) == 0
    )

    valid_df   = validated.filter(F.col("_is_valid")).drop("_errors", "_is_valid")
    invalid_df = validated.filter(~F.col("_is_valid"))

    v_count = valid_df.count()
    i_count = invalid_df.count()
    summary = {
        "total":    total_rows,
        "valid":    v_count,
        "invalid":  i_count,
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

# DBTITLE 1,Write to Unity Catalog staging table
from datetime import datetime

staged_df = valid_df.withColumns({
    "semester":        F.lit(SEMESTER),           # canonical column for downstream joins
    "_ingestion_ts":   F.lit(datetime.utcnow().isoformat()),
    "_source_file":    F.lit(SOURCE_PATH),
    "_semester":       F.lit(SEMESTER),
    "_pipeline_stage": F.lit("staging"),
})

(
    staged_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.student_scores_staging")
)

logger.info(f"Staging written to workspace.default.student_scores_staging")
print(f"\n✅ Staging complete → workspace.default.student_scores_staging")
print(f"   Rows written: {summary['valid']}")

# COMMAND ----------

# MAGIC %md ## 5 · Quick Profile

# COMMAND ----------

print("\n── Score distribution (letter grade) ──")
valid_df.withColumn(
    "_grade_10_preview",
    F.round(
        (F.col("quiz1_marks") + F.col("quiz2_marks") + F.col("quiz3_marks"))
        + (F.col("midterm_marks") / 30.0) * 20.0
        + F.col("final_marks"),
        2
    ) / 10.0
).withColumn(
    "letter_grade",
    F.when(F.col("_grade_10_preview") >= 8.5, "A")
    .when(F.col("_grade_10_preview") >= 7.0,  "B")
    .when(F.col("_grade_10_preview") >= 5.5,  "C")
    .when(F.col("_grade_10_preview") >= 4.0,  "D")
    .otherwise("F")
).groupBy("letter_grade").count().orderBy("letter_grade").show()

print("── Missing values ──")
for col_name in valid_df.columns:
    null_count = valid_df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} nulls")

print("\n✅ 01_data_ingestion  DONE")
