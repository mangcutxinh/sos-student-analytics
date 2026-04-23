# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze Layer
# MAGIC **Pipeline:** Staging Parquet → Delta Bronze (raw, append-only)
# MAGIC
# MAGIC Bronze = exact copy of source data + metadata columns. No transformation.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("02_bronze_layer")

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# MAGIC %md ## Config

# COMMAND ----------
STAGING_PATH = "dbfs:/delta/staging/student_scores"
BRONZE_PATH  = "dbfs:/delta/bronze/student_scores"
BRONZE_TABLE = "bronze_student_scores"

# COMMAND ----------
# MAGIC %md ## 1 · Read Staging

# COMMAND ----------
logger.info(f"Reading staging from: {STAGING_PATH}")
staged_df = spark.read.parquet(STAGING_PATH)

row_count = staged_df.count()
print(f"✅ Staging rows: {row_count}")
staged_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 2 · Add Bronze Metadata

# COMMAND ----------
from datetime import datetime

bronze_df = staged_df.withColumns({
    "bronze_loaded_at":   F.current_timestamp(),
    "bronze_loaded_date": F.current_date(),
    "bronze_batch_id":    F.lit(datetime.utcnow().strftime("%Y%m%d_%H%M%S")),
    # business_key: stable identity per student × semester
    "business_key":       F.md5(F.concat_ws("|",
                              F.col("student_id").cast("string"),
                              F.col("semester"),
                          )),
    # record_hash: detects any change in source values (used to skip true duplicates)
    "record_hash":        F.md5(F.concat_ws("|",
                              F.col("student_id").cast("string"),
                              F.col("name"),
                              F.col("age").cast("string"),
                              F.col("gender"),
                              F.col("quiz1_marks").cast("string"),
                              F.col("quiz2_marks").cast("string"),
                              F.col("quiz3_marks").cast("string"),
                              F.col("midterm_marks").cast("string"),
                              F.col("final_marks").cast("string"),
                              F.col("previous_gpa").cast("string"),
                              F.col("lectures_attended").cast("string"),
                              F.col("labs_attended").cast("string"),
                              F.col("semester"),
                          )),
})

# COMMAND ----------
# MAGIC %md ## 3 · Write to Delta Bronze (append idempotent via MERGE)

# COMMAND ----------
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    logger.info("Bronze table exists — MERGE (upsert) by row_hash")
    bronze_table = DeltaTable.forPath(spark, BRONZE_PATH)

    (
        bronze_table.alias("tgt")
        .merge(
            bronze_df.alias("src"),
            "tgt.business_key = src.business_key AND tgt.record_hash = src.record_hash"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ MERGE complete (new rows inserted, duplicates skipped)")

else:
    logger.info("Bronze table does not exist — creating fresh")
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("semester", "bronze_loaded_date")
        .save(BRONZE_PATH)
    )
    print(f"✅ Bronze table created at {BRONZE_PATH}")

# COMMAND ----------
# MAGIC %md ## 4 · Register in Hive Metastore

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")
print(f"✅ Table registered: {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5 · Verify + Stats

# COMMAND ----------
bronze_verify = spark.read.format("delta").load(BRONZE_PATH)
total = bronze_verify.count()
print(f"\n── Bronze Table Stats ──────────────────────")
print(f"  Total rows   : {total}")
print(f"  Partitions   : semester, bronze_loaded_date")
print(f"  Path         : {BRONZE_PATH}")

bronze_verify.groupBy("semester").count().orderBy("semester").show()

# Delta history
print("\n── Delta History (last 5) ──")
spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`").show(5, truncate=False)

print("\n✅ 02_bronze_layer  DONE")
