# ============================================================
# CareFusion_360 - Job 1 (Production Grade)
# S3 Historical Member + Migration Pipeline
# Strict DQ + Quarantine + PHI Masking
# ============================================================

import sys
sys.path.append("/home/salvador/carefusion360")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    row_number,
    sha2,
    current_date,
    year,
    current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime
from utils.pipeline_logger import log_pipeline_run, generate_run_id

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("CareFusion360_Job1_S3_Member_Pipeline_Prod") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Started...")

run_id = generate_run_id()
start_time = datetime.now()
status = "SUCCESS"

# ============================================================
# 2. DEFINE PATHS
# ============================================================

historical_path = "/home/salvador/carefusion360/raw_data/s3/historical_member_policy"
migrated_path   = "/home/salvador/carefusion360/raw_data/s3/migrated_members"

output_path     = "/home/salvador/carefusion360/intermediate_data/job1_member_dimension"
quarantine_path = "/home/salvador/carefusion360/quarantine/job1_bad_records"

# ============================================================
# 3. READ RAW DATA
# ============================================================

historical_df = spark.read.option("header","true").option("inferSchema","true").csv(historical_path)
migrated_df   = spark.read.option("header","true").option("inferSchema","true").csv(migrated_path)

print("Raw Historical Count:", historical_df.count())

# ============================================================
# 4. REMOVE EXACT DUPLICATES
# ============================================================

historical_df = historical_df.dropDuplicates()

print("After Exact Dedup:", historical_df.count())

# ============================================================
# 5. STRICT DATA QUALITY RULES
# ============================================================

current_year_col = year(current_date())

valid_df = historical_df.filter(
    (col("legacy_member_id").isNotNull()) &
    (col("effective_date").isNotNull()) &
    (col("birth_year").isNotNull()) &
    (col("birth_year").between(1900, 2025)) &
    (col("gender").isin("M","F")) &
    (
        (col("coverage_end_date").isNull()) |
        (col("coverage_start_date") <= col("coverage_end_date"))
    )
)

quarantine_df = historical_df.subtract(valid_df)

print("Valid Records:", valid_df.count())
print("Quarantined Records:", quarantine_df.count())
records_processed = valid_df.count()
records_failed = quarantine_df.count()

quarantine_df.write.mode("overwrite").parquet(quarantine_path)

# ============================================================
# 6. LATE-ARRIVAL SAFE SCD LOGIC
# ============================================================

window_spec = Window.partitionBy("legacy_member_id") \
    .orderBy(
        col("effective_date").desc(),
        col("record_updated_ts").desc()
    )

scd_df = valid_df.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

print("After SCD Latest Selection:", scd_df.count())

# ============================================================
# 7. JOIN WITH MIGRATED TABLE
# ============================================================

joined_df = scd_df.join(
    migrated_df,
    on="legacy_member_id",
    how="inner"
)

print("After Join:", joined_df.count())

# ============================================================
# 8. DERIVED COLUMNS
# ============================================================

joined_df = joined_df.withColumn(
    "age",
    current_year_col - col("birth_year")
)

joined_df = joined_df.withColumn(
    "age_band",
    when(col("age").between(18,24), "18-24")
    .when(col("age").between(25,34), "25-34")
    .when(col("age").between(35,44), "35-44")
    .when(col("age").between(45,54), "45-54")
    .otherwise("55+")
)

joined_df = joined_df.withColumn(
    "active_flag",
    when(
        (col("coverage_end_date").isNull()) |
        (col("coverage_end_date") > current_date()),
        "Active"
    ).otherwise("Terminated")
)

# ============================================================
# 9. PHI MASKING
# ============================================================

joined_df = joined_df.withColumn(
    "masked_ssn",
    sha2(col("ssn_raw"), 256)
)

joined_df = joined_df.drop(
    "patient_first_name",
    "patient_last_name",
    "email",
    "phone_number",
    "address_line",
    "city",
    "zip_code",
    "ssn_raw"
)

# ============================================================
# 10. MEMBER_KEY GENERATION
# ============================================================

joined_df = joined_df.withColumn(
    "member_key",
    sha2(col("member_id"), 256)
)

# ============================================================
# 11. ADD AUDIT COLUMNS
# ============================================================

final_df = joined_df.withColumn(
    "processing_timestamp",
    current_timestamp()
).withColumn(
    "record_status",
    lit("ACTIVE")
).withColumn(
    "dq_flag",
    lit("VALID")
)

# ============================================================
# NEW: ADD PROCESSING DATE
# ============================================================

final_df = final_df.withColumn(
    "processing_date",
    current_date()
)

# ============================================================
# 12. SELECT FINAL COLUMNS
# ============================================================

final_df = final_df.select(
    "member_key",
    "member_id",
    "legacy_member_id",
    "age_band",
    "gender",
    "state",
    "enrollment_date",
    "policy_id",
    "policy_type",
    "coverage_start_date",
    "coverage_end_date",
    "member_segment",
    "active_flag",
    "migration_flag",
    "masked_ssn",
    "processing_timestamp",
    "record_status",
    "dq_flag",
    "processing_date"
)

# ============================================================
# 13. WRITE PARTITIONED OUTPUT
# ============================================================

final_df.write \
    .mode("overwrite") \
    .partitionBy("processing_date","active_flag") \
    .parquet(output_path)

print("Job 1 Completed Successfully ✅")
print("Output Written To:", output_path)

# ==============================================================
# log_pipeline_run
# ==============================================================

log_pipeline_run(
    spark=spark,
    job_name="job1_member_pipeline",
    run_id=run_id,
    records_processed=records_processed,
    records_failed=records_failed,
    start_time=start_time,
    status="SUCCESS"
)

spark.stop()