# ============================================================
# CareFusion_360 - Job 2 (Production Grade - Strict Mode)
# Snowflake Domain Pipeline
# ============================================================

import sys
sys.path.append("/home/salvador/carefusion360")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    row_number,
    current_timestamp,
    lit,
    year
)
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from datetime import datetime
from utils.pipeline_logger import log_pipeline_run, generate_run_id

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("CareFusion360_Job2_Snowflake_Pipeline_Prod") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Started...")

run_id = generate_run_id()
start_time = datetime.now()
status = "SUCCESS"

records_processed = 0
records_failed = 0

# ============================================================
# 2. PATHS
# ============================================================

base_path = "/home/salvador/carefusion360/raw_data/snowflake"

members_path     = f"{base_path}/members_current"
policies_path    = f"{base_path}/policies"
claims_path      = f"{base_path}/claims"
adjust_path      = f"{base_path}/claims_adjustments"
billing_path     = f"{base_path}/billing"
revenue_path     = f"{base_path}/revenue"
violations_path  = f"{base_path}/violations"

output_base      = "/home/salvador/carefusion360/intermediate_data"
quarantine_base  = "/home/salvador/carefusion360/quarantine/job2"

claims_output     = f"{output_base}/job2_claims_fact"
revenue_output    = f"{output_base}/job2_revenue"
violations_output = f"{output_base}/job2_violations"

bad_claims_path        = f"{quarantine_base}/bad_claims"
bad_adjustments_path   = f"{quarantine_base}/bad_adjustments"

# ============================================================
# 3. READ DATA
# ============================================================

members_df = spark.read.option("header","true").option("inferSchema","true").csv(members_path)
claims_df  = spark.read.option("header","true").option("inferSchema","true").csv(claims_path)
adjust_df  = spark.read.option("header","true").option("inferSchema","true").csv(adjust_path)
revenue_df = spark.read.option("header","true").option("inferSchema","true").csv(revenue_path)
violations_df = spark.read.option("header","true").option("inferSchema","true").csv(violations_path)

print("Claims Raw Count:", claims_df.count())

# ============================================================
# 4. REMOVE EXACT DUPLICATE CLAIMS
# ============================================================

claims_df = claims_df.dropDuplicates()

# ============================================================
# 5. STRICT CLAIM VALIDATION
# ============================================================

valid_claims = claims_df \
    .join(broadcast(members_df.select("member_id")), on="member_id", how="inner") \
    .filter(
        (col("claim_id").isNotNull()) &
        (col("claim_date").isNotNull()) &
        (col("claim_amount") >= 0) &
        (col("claim_amount") <= 75000)
    )

bad_claims = claims_df.subtract(valid_claims)

print("Valid Claims:", valid_claims.count())
print("Bad Claims:", bad_claims.count())

bad_claims.write.mode("overwrite").parquet(bad_claims_path)

# ============================================================
# 6. ADJUSTMENT VALIDATION
# ============================================================

adjust_with_claim = adjust_df.join(
    valid_claims.select("claim_id", "claim_date"),
    on="claim_id",
    how="inner"
)

valid_adjust = adjust_with_claim.filter(
    (col("original_claim_amount") + col("adjustment_amount") == col("adjusted_final_amount")) &
    (col("adjustment_date") >= col("claim_date"))
).drop("claim_date")

bad_adjust = adjust_df.join(
    valid_adjust.select("adjustment_id"),
    on="adjustment_id",
    how="left_anti"
)

print("Valid Adjustments:", valid_adjust.count())
print("Bad Adjustments:", bad_adjust.count())
records_processed = valid_adjust.count()
records_failed = bad_adjust.count()

bad_adjust.write.mode("overwrite").parquet(bad_adjustments_path)

# ============================================================
# 7. BUILD CLEAN CLAIM FACT
# ============================================================

claims_fact = valid_claims.join(
    valid_adjust,
    on=["claim_id","member_id"],
    how="left"
)

claims_fact = claims_fact.withColumn(
    "final_claim_amount",
    when(col("adjusted_final_amount").isNull(),
         col("claim_amount"))
    .otherwise(col("adjusted_final_amount"))
)

claims_fact = claims_fact.withColumn(
    "processing_timestamp",
    current_timestamp()
).withColumn(
    "dq_flag",
    lit("VALID")
)

# ============================================================
# NEW: ADD CLAIM YEAR FOR PARTITIONING
# ============================================================

claims_fact = claims_fact.withColumn(
    "claim_year",
    year(col("claim_date"))
)

print("Final Clean Claims:", claims_fact.count())

# ============================================================
# 8. CLEAN REVENUE
# ============================================================

revenue_clean = revenue_df \
    .filter(col("premium_collected") >= 0) \
    .withColumn("processing_timestamp", current_timestamp())

# ============================================================
# 9. CLEAN VIOLATIONS
# ============================================================

violations_clean = violations_df \
    .dropDuplicates() \
    .withColumn("processing_timestamp", current_timestamp())

# ============================================================
# 10. WRITE OUTPUT
# ============================================================

claims_fact.write \
    .mode("overwrite") \
    .partitionBy("claim_year") \
    .parquet(claims_output)

revenue_clean.write.mode("overwrite").parquet(revenue_output)
violations_clean.write.mode("overwrite").parquet(violations_output)

print("Job 2 Completed Successfully (Strict Mode) ✅")

# ==============================================================
# log_pipeline_run
# ==============================================================

log_pipeline_run(
    spark=spark,
    job_name="job2_claims_pipeline",
    run_id=run_id,
    records_processed=records_processed,
    records_failed=records_failed,
    start_time=start_time,
    status="SUCCESS"
)

spark.stop()