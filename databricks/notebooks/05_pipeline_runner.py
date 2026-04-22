# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline Runner (Orchestrator)
# MAGIC
# MAGIC Runs the full ETL pipeline in sequence. Used as the **entry-point notebook** for a Databricks Job.
# MAGIC
# MAGIC ```
# MAGIC 01_data_ingestion  →  02_bronze_layer  →  03_silver_layer  →  04_gold_analytics
# MAGIC ```
# MAGIC
# MAGIC Each notebook is called via `%run` (same cluster) so variables and SparkSession are shared.

# COMMAND ----------
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md ## Pipeline Config

# COMMAND ----------
# ── These can be overridden by Databricks Job Parameters ──────────────────────
dbutils.widgets.text("source_path",  "dbfs:/FileStore/student_score_dataset.csv", "Source CSV Path")
dbutils.widgets.text("semester",     "2024-1",                                    "Semester")
dbutils.widgets.text("run_mode",     "full",                                      "Run Mode: full | bronze_only | gold_only")
dbutils.widgets.text("notify_email", "",                                           "Notification Email")

SOURCE_PATH   = dbutils.widgets.get("source_path")
SEMESTER      = dbutils.widgets.get("semester")
RUN_MODE      = dbutils.widgets.get("run_mode")
NOTIFY_EMAIL  = dbutils.widgets.get("notify_email")

NOTEBOOKS_DIR = "/Repos/<your-username>/soa-student-analytics/databricks/notebooks"
# ↑ Change <your-username> to your Databricks username after linking the repo

print(f"╔══════════════════════════════════════════════════╗")
print(f"║   SOA Student Analytics  —  Pipeline Runner      ║")
print(f"╠══════════════════════════════════════════════════╣")
print(f"║  source_path : {SOURCE_PATH:<34}║")
print(f"║  semester    : {SEMESTER:<34}║")
print(f"║  run_mode    : {RUN_MODE:<34}║")
print(f"║  started_at  : {datetime.utcnow().isoformat():<34}║")
print(f"╚══════════════════════════════════════════════════╝")

# COMMAND ----------
# MAGIC %md ## Stage Execution

# COMMAND ----------
pipeline_log = []

def run_stage(name: str, notebook_path: str, params: dict = None, timeout: int = 1800):
    """Run a notebook stage and record result."""
    params = params or {}
    start  = time.time()
    status = "SUCCESS"
    error  = None
    try:
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=timeout,
            arguments=params
        )
        print(f"✅  {name}  ({round(time.time()-start, 1)}s)")
    except Exception as e:
        status = "FAILED"
        error  = str(e)
        print(f"❌  {name}  FAILED  →  {error}")
        raise

    pipeline_log.append({
        "stage":      name,
        "status":     status,
        "duration_s": round(time.time()-start, 1),
        "error":      error,
    })

# COMMAND ----------
pipeline_start = time.time()
failed_stages  = []

try:
    if RUN_MODE in ("full", "ingest_only", "bronze_only"):
        run_stage(
            "01_data_ingestion",
            f"{NOTEBOOKS_DIR}/01_data_ingestion",
            {"source_path": SOURCE_PATH, "semester": SEMESTER},
        )

    if RUN_MODE in ("full", "bronze_only"):
        run_stage(
            "02_bronze_layer",
            f"{NOTEBOOKS_DIR}/02_bronze_layer",
        )

    if RUN_MODE in ("full", "silver_only"):
        run_stage(
            "03_silver_layer",
            f"{NOTEBOOKS_DIR}/03_silver_layer",
        )

    if RUN_MODE in ("full", "gold_only"):
        run_stage(
            "04_gold_analytics",
            f"{NOTEBOOKS_DIR}/04_gold_analytics",
        )

    pipeline_status = "SUCCESS"

except Exception as e:
    pipeline_status = "FAILED"
    failed_stages.append(str(e))

# COMMAND ----------
# MAGIC %md ## Pipeline Summary

# COMMAND ----------
total_duration = round(time.time() - pipeline_start, 1)

print("\n╔══════════════════════════════════════════════════════╗")
print(f"║  PIPELINE {pipeline_status:<42}║")
print(f"╠══════════════════════════════════════════════════════╣")
for entry in pipeline_log:
    icon = "✅" if entry["status"] == "SUCCESS" else "❌"
    print(f"║  {icon}  {entry['stage']:<28} {entry['duration_s']:>6}s  ║")
print(f"╠══════════════════════════════════════════════════════╣")
print(f"║  Total duration : {total_duration:<35}║")
print(f"╚══════════════════════════════════════════════════════╝")

# COMMAND ----------
# MAGIC %md ## Write Run Log to Delta

# COMMAND ----------
from pyspark.sql import Row

log_row = Row(
    run_id          = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString(),
    pipeline_status = pipeline_status,
    semester        = SEMESTER,
    source_path     = SOURCE_PATH,
    run_mode        = RUN_MODE,
    total_duration  = total_duration,
    stages          = json.dumps(pipeline_log),
    run_timestamp   = datetime.utcnow().isoformat(),
)

log_df = spark.createDataFrame([log_row])
log_df.write.format("delta").mode("append").save("dbfs:/delta/pipeline_runs")
print("✅ Run log written to dbfs:/delta/pipeline_runs")

# COMMAND ----------
if pipeline_status == "FAILED":
    raise Exception(f"Pipeline failed at stages: {failed_stages}")

print("\n🎉 Pipeline completed successfully!")
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "duration_s": total_duration}))
