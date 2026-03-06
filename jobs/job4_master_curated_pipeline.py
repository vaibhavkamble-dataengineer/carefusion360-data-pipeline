# ============================================================
# CareFusion_360 - Job 4
# Gold Layer - Member 360 (Active Members Only)
# ============================================================

import sys
sys.path.append("/home/salvador/carefusion360")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, max as spark_max,
    datediff, current_date, year,
    when, lit, log, current_timestamp
)
from pyspark.sql.functions import current_date
from datetime import datetime
from utils.pipeline_logger import log_pipeline_run, generate_run_id


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("CareFusion360_Job4_Member360_Gold") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Started...")

run_id = generate_run_id()
start_time = datetime.now()
status = "SUCCESS"
records_processed = 0
records_failed = 0

BASE_PATH = "/home/salvador/carefusion360/intermediate_data"
OUTPUT_PATH = "/home/salvador/carefusion360/gold/member_360"

# ============================================================
# 2. LOAD ACTIVE MEMBER DIMENSION (Job1)
# ============================================================

member_dim = spark.read.parquet(
    f"{BASE_PATH}/job1_member_dimension"
).filter(col("active_flag") == "Active")

# ============================================================
# 3. FINANCIAL PRE-AGGREGATION (Job2)
# ============================================================

claims_df = spark.read.parquet(f"{BASE_PATH}/job2_claims_fact")
revenue_df = spark.read.parquet(f"{BASE_PATH}/job2_revenue")
violations_df = spark.read.parquet(f"{BASE_PATH}/job2_violations")

claims_agg = claims_df.groupBy("member_id").agg(
    spark_sum("final_claim_amount").alias("lifetime_claim_amount"),
    count("claim_id").alias("total_claim_count")
)

revenue_agg = revenue_df.groupBy("member_id").agg(
    spark_sum("premium_collected").alias("lifetime_revenue")
)

violations_agg = violations_df.groupBy("member_id").agg(
    count("violation_id").alias("violation_count")
)

# ============================================================
# 4. STREAMING PRE-AGGREGATION (Job3)
# ============================================================

job3_claims = spark.read.parquet(f"{BASE_PATH}/job3_claims")
job3_billing = spark.read.parquet(f"{BASE_PATH}/job3_billing")
job3_policy = spark.read.parquet(f"{BASE_PATH}/job3_policy")
job3_member = spark.read.parquet(f"{BASE_PATH}/job3_member")

claims_stream_agg = job3_claims.groupBy("member_id").agg(
    count("event_id").alias("streaming_claim_events"),
    spark_max("event_id").alias("last_claim_event_id")
)

billing_stream_agg = job3_billing.groupBy("member_id").agg(
    count("event_id").alias("streaming_payment_events")
)

policy_stream_agg = job3_policy.groupBy("member_id").agg(
    count("event_id").alias("policy_change_count")
)

member_stream_agg = job3_member.groupBy("member_id").agg(
    count("event_id").alias("member_update_count")
)

# ============================================================
# 5. JOIN ALL AGGREGATES
# ============================================================

member_360 = member_dim \
    .join(claims_agg, "member_id", "left") \
    .join(revenue_agg, "member_id", "left") \
    .join(violations_agg, "member_id", "left") \
    .join(claims_stream_agg, "member_id", "left") \
    .join(billing_stream_agg, "member_id", "left") \
    .join(policy_stream_agg, "member_id", "left") \
    .join(member_stream_agg, "member_id", "left")

# Fill nulls with zero for numeric fields
member_360 = member_360.fillna({
    "lifetime_claim_amount": 0,
    "total_claim_count": 0,
    "lifetime_revenue": 0,
    "violation_count": 0,
    "streaming_claim_events": 0,
    "streaming_payment_events": 0,
    "policy_change_count": 0,
    "member_update_count": 0
})

# ============================================================
# 6. DERIVED METRICS
# ============================================================

member_360 = member_360.withColumn(
    "tenure_days",
    datediff(current_date(), col("enrollment_date"))
)

member_360 = member_360.withColumn(
    "lifetime_value",
    col("lifetime_revenue") - col("lifetime_claim_amount")
)

member_360 = member_360.withColumn(
    "engagement_score",
    log(col("streaming_claim_events") +
        col("streaming_payment_events") + lit(1))
)

member_360 = member_360.withColumn(
    "risk_tier",
    when(
        (col("violation_count") > 5) |
        (col("lifetime_claim_amount") > 100000),
        "HIGH"
    ).when(
        col("violation_count") > 0,
        "MEDIUM"
    ).otherwise("LOW")
)

member_360 = member_360.withColumn(
    "churn_flag",
    when(
        (col("streaming_claim_events") == 0) &
        (col("streaming_payment_events") == 0),
        "HIGH_CHURN_RISK"
    ).otherwise("STABLE")
)

member_360 = member_360.withColumn(
    "processing_timestamp",
    current_timestamp()
)

# ============================================================
# 7. FINAL SELECT
# ============================================================

final_df = member_360.select(
    "member_id",
    "member_key",
    "age_band",
    "state",
    "tenure_days",

    "lifetime_claim_amount",
    "lifetime_revenue",
    "total_claim_count",
    "violation_count",

    "streaming_claim_events",
    "streaming_payment_events",
    "policy_change_count",
    "member_update_count",

    "engagement_score",
    "lifetime_value",
    "risk_tier",
    "churn_flag",
    "processing_timestamp"
)

# ============================================================
# 8. WRITE GOLD OUTPUT
# ============================================================

# ============================================================
# ADD SNAPSHOT RUN DATE
# ============================================================

final_df = final_df.withColumn("run_date", current_date())

# ============================================================
# SMART REPARTITIONING
# ============================================================

# Repartition by state to control file sizes
final_df = final_df.repartition(8, "state")

# ============================================================
# WRITE SNAPSHOT APPEND MODE
# ============================================================

final_df.write \
    .mode("append") \
    .partitionBy("run_date", "state") \
    .parquet(OUTPUT_PATH)

# ==============================================================
# log_pipeline_run
# ==============================================================

log_pipeline_run(
    spark=spark,
    job_name="job4_member360_pipeline",
    run_id=run_id,
    records_processed=records_processed,
    records_failed=records_failed,
    start_time=start_time,
    status=status
)

print("Member 360 Gold Snapshot Written Successfully ✅")

spark.stop()