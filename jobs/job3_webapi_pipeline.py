# ============================================================
# CareFusion_360 - Job 3
# Nested + Schema Evolution + Explode + DQ + Quarantine
# ============================================================

import sys
sys.path.append("/home/salvador/carefusion360")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, when, lit, current_timestamp
)
from pyspark.sql.types import DoubleType
import requests
from datetime import datetime
from utils.pipeline_logger import log_pipeline_run, generate_run_id

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("CareFusion360_Job3_Production") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session Started...")

run_id = generate_run_id()
start_time = datetime.now()
status = "SUCCESS"

records_processed = 0
records_failed = 0

# ============================================================
# 2. CALL WEB API
# ============================================================

response = requests.get("http://localhost:5000/events")

if response.status_code != 200:
    raise Exception("API call failed")

raw_df = spark.read.json(
    spark.sparkContext.parallelize([response.text])
)

print("Raw Events:", raw_df.count())

# ============================================================
# 3. BASIC VALIDATION
# ============================================================

valid_df = raw_df.filter(
    col("event_id").isNotNull() &
    col("member_id").isNotNull() &
    col("payload").isNotNull()
)

print("After Basic Validation:", valid_df.count())

# ============================================================
# 4. CLAIMS DOMAIN (v2 nested)
# ============================================================

claims_v2 = valid_df.filter(
    (col("event_category") == "CLAIMS") &
    (col("schema_version") == "v2")
).withColumn(
    "claim_id", col("payload.claim.id")
).withColumn(
    "claim_amount", col("payload.claim.amount").cast(DoubleType())
).withColumn(
    "provider_id", col("payload.provider.id")
).withColumn(
    "diagnosis_code",
    explode(col("payload.claim.diagnosis_codes"))
)

# ============================================================
# 5. CLAIMS DOMAIN (v1 flat)
# ============================================================

claims_v1 = valid_df.filter(
    (col("event_category") == "CLAIMS") &
    (col("schema_version") == "v1")
).withColumn(
    "claim_id", col("payload.claim_id")
).withColumn(
    "claim_amount", col("payload.claim_amount").cast(DoubleType())
).withColumn(
    "provider_id", col("payload.provider_id")
).withColumn(
    "diagnosis_code", lit(None)
)

claims_normalized = claims_v2.select(
    "event_id",
    "event_timestamp",
    "member_id",
    "claim_id",
    "claim_amount",
    "provider_id",
    "diagnosis_code"
).unionByName(
    claims_v1.select(
        "event_id",
        "event_timestamp",
        "member_id",
        "claim_id",
        "claim_amount",
        "provider_id",
        "diagnosis_code"
    )
)

# ============================================================
# 6. BILLING DOMAIN
# ============================================================

billing_v2 = valid_df.filter(
    (col("event_category") == "BILLING") &
    (col("schema_version") == "v2")
).withColumn(
    "payment_amount",
    col("payload.payment.amount").cast(DoubleType())
).withColumn(
    "payment_method",
    explode(col("payload.payment.methods"))
)

billing_v1 = valid_df.filter(
    (col("event_category") == "BILLING") &
    (col("schema_version") == "v1")
).withColumn(
    "payment_amount",
    col("payload.payment_amount").cast(DoubleType())
).withColumn(
    "payment_method", lit(None)
)

billing_normalized = billing_v2.select(
    "event_id",
    "event_timestamp",
    "member_id",
    "payment_amount",
    "payment_method"
).unionByName(
    billing_v1.select(
        "event_id",
        "event_timestamp",
        "member_id",
        "payment_amount",
        "payment_method"
    )
)

# ============================================================
# 7. POLICY DOMAIN
# ============================================================

policy_v2 = valid_df.filter(
    (col("event_category") == "POLICY") &
    (col("schema_version") == "v2")
).withColumn(
    "policy_id", col("payload.policy.id")
).withColumn(
    "start_date", col("payload.policy.coverage.start_date")
).withColumn(
    "end_date", col("payload.policy.coverage.end_date")
)

policy_v1 = valid_df.filter(
    (col("event_category") == "POLICY") &
    (col("schema_version") == "v1")
).withColumn(
    "policy_id", col("payload.policy_id")
).withColumn(
    "start_date", lit(None)
).withColumn(
    "end_date", lit(None)
)

policy_normalized = policy_v2.select(
    "event_id",
    "event_timestamp",
    "member_id",
    "policy_id",
    "start_date",
    "end_date"
).unionByName(
    policy_v1.select(
        "event_id",
        "event_timestamp",
        "member_id",
        "policy_id",
        "start_date",
        "end_date"
    )
)

# ============================================================
# 8. MEMBER DOMAIN
# ============================================================

member_v2 = valid_df.filter(
    (col("event_category") == "MEMBER") &
    (col("schema_version") == "v2")
).withColumn(
    "first_name", col("payload.profile.first_name")
).withColumn(
    "last_name", col("payload.profile.last_name")
).withColumn(
    "email", col("payload.profile.email")
).withColumn(
    "city", col("payload.profile.address.city")
)

member_v1 = valid_df.filter(
    (col("event_category") == "MEMBER") &
    (col("schema_version") == "v1")
).withColumn(
    "first_name", col("payload.first_name")
).withColumn(
    "last_name", col("payload.last_name")
).withColumn(
    "email", col("payload.email")
).withColumn(
    "city", col("payload.city")
)

member_normalized = member_v2.select(
    "event_id",
    "event_timestamp",
    "member_id",
    "first_name",
    "last_name",
    "email",
    "city"
).unionByName(
    member_v1.select(
        "event_id",
        "event_timestamp",
        "member_id",
        "first_name",
        "last_name",
        "email",
        "city"
    )
)

# ============================================================
# 9. DOMAIN DATA QUALITY CHECKS
# ============================================================

# CLAIM DQ
claims_valid = claims_normalized.filter(
    col("claim_id").isNotNull() &
    col("claim_amount").isNotNull() &
    (col("claim_amount") > 0) &
    col("provider_id").isNotNull()
)

claims_bad = claims_normalized.subtract(claims_valid)

# BILLING DQ
billing_valid = billing_normalized.filter(
    col("payment_amount").isNotNull() &
    (col("payment_amount") > 0)
)

billing_bad = billing_normalized.subtract(billing_valid)

# POLICY DQ
policy_valid = policy_normalized.filter(
    col("policy_id").isNotNull()
)

policy_bad = policy_normalized.subtract(policy_valid)

# MEMBER DQ
member_valid = member_normalized.filter(
    col("first_name").isNotNull() &
    col("last_name").isNotNull()
)

member_bad = member_normalized.subtract(member_valid)

print("Valid Claims:", claims_valid.count())
print("Bad Claims:", claims_bad.count())

print("Valid Billing:", billing_valid.count())
print("Bad Billing:", billing_bad.count())

print("Valid Policy:", policy_valid.count())
print("Bad Policy:", policy_bad.count())

print("Valid Member:", member_valid.count())
print("Bad Member:", member_bad.count())

# ----------------------------
# METRICS FOR PIPELINE LOGGER
# ----------------------------

claims_count = claims_valid.count()
billing_count = billing_valid.count()
policy_count = policy_valid.count()
member_count = member_valid.count()

records_processed = (
    claims_count +
    billing_count +
    policy_count +
    member_count
)

records_failed = (
    claims_bad.count() +
    billing_bad.count() +
    policy_bad.count() +
    member_bad.count()
)

# ============================================================
# 10. WRITE OUTPUTS
# ============================================================

base_path = "/home/salvador/carefusion360/intermediate_data"
quarantine_path = "/home/salvador/carefusion360/quarantine"


claims_valid.write.mode("overwrite").parquet(f"{base_path}/job3_claims")
billing_valid.write.mode("overwrite").parquet(f"{base_path}/job3_billing")
policy_valid.write.mode("overwrite").parquet(f"{base_path}/job3_policy")
member_valid.write.mode("overwrite").parquet(f"{base_path}/job3_member")

claims_bad.write.mode("overwrite").parquet(f"{quarantine_path}/job3_claims_bad")
billing_bad.write.mode("overwrite").parquet(f"{quarantine_path}/job3_billing_bad")
policy_bad.write.mode("overwrite").parquet(f"{quarantine_path}/job3_policy_bad")
member_bad.write.mode("overwrite").parquet(f"{quarantine_path}/job3_member_bad")

print("Job 3 Completed Successfully (Production Mode) ✅")

# ==============================================================
# log_pipeline_run
# ==============================================================

log_pipeline_run(
    spark=spark,
    job_name="job3_events_pipeline",
    run_id=run_id,
    records_processed=records_processed,
    records_failed=records_failed,
    start_time=start_time,
    status=status
)

spark.stop()