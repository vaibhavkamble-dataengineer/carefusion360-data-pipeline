# ============================================================
# CareFusion360 - Pipeline Logger Utility
# Shared Metadata Logging for All Jobs
# ============================================================

from pyspark.sql import SparkSession
from datetime import datetime


def generate_run_id():
    return "run_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def log_pipeline_run(
    spark,
    job_name,
    run_id,
    records_processed,
    records_failed,
    start_time,
    status,
    metadata_path="/home/salvador/carefusion360/metadata/pipeline_run_log"
):

    end_time = datetime.now()

    metrics_data = [
        (
            run_id,
            job_name,
            records_processed,
            records_failed,
            start_time,
            end_time,
            status
        )
    ]

    columns = [
        "run_id",
        "job_name",
        "records_processed",
        "records_failed",
        "start_time",
        "end_time",
        "status"
    ]

    metrics_df = spark.createDataFrame(metrics_data, columns)

    metrics_df.write \
        .mode("append") \
        .parquet(metadata_path)

    print(f"Pipeline metrics logged for {job_name}")