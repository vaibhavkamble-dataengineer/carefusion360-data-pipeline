import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# =========================
# 1. CONSTANTS
# =========================

TOTAL_MEMBERS = 50000
MIN_VERSIONS = 2
MAX_VERSIONS = 3

BASE_OUTPUT_PATH = "/home/salvador/carefusion360/raw_data/s3"
HISTORICAL_PATH = os.path.join(BASE_OUTPUT_PATH, "historical_member_policy")
MIGRATED_PATH = os.path.join(BASE_OUTPUT_PATH, "migrated_members")

# =========================
# DATA CORRUPTION CONTROLS
# =========================

NULL_BIRTH_YEAR_PCT = 0.01
INVALID_GENDER_PCT = 0.01
DUPLICATE_RECORD_PCT = 0.02
OVERLAP_PCT = 0.02
LATE_UPDATE_PCT = 0.03
HIGH_VERSION_MEMBER_PCT = 0.05

random.seed(42)
np.random.seed(42)

# =========================
# 2. STATIC REFERENCE DATA
# =========================

US_STATES = ["CA","TX","NY","FL","IL","WA","AZ","PA"]
POLICY_TYPES = ["HMO", "PPO", "EPO"]
SEGMENTS = ["Individual", "Family", "Corporate"]
TIERS = ["Silver", "Gold", "Platinum"]
SOURCE_SYSTEMS = ["legacy_A", "legacy_B"]

# =========================
# 3. HELPER FUNCTIONS
# =========================

def generate_legacy_id(i):
    return f"LEG_{str(i).zfill(6)}"

def generate_member_id(i):
    return f"MBR_{str(i).zfill(5)}"

def random_date(start_year=2015, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_string(length=8):
    return ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=length))

def random_phone():
    return f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def random_zip():
    return str(random.randint(10000, 99999))

def random_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"

# =========================
# 4. MASTER MEMBER POOL (WITH PHI)
# =========================

print("Generating master member pool...")

members = []

for i in range(1, TOTAL_MEMBERS + 1):

    source_system = random.choices(
        SOURCE_SYSTEMS,
        weights=[0.7, 0.3]  # legacy_B more dirty later
    )[0]

    members.append({
        "legacy_member_id": generate_legacy_id(i),
        "patient_first_name": random_string(6),
        "patient_last_name": random_string(8),
        "email": f"{random_string(5)}@mail.com",
        "phone_number": random_phone(),
        "address_line": random_string(12),
        "city": random_string(6),
        "zip_code": random_zip(),
        "ssn_raw": random_ssn(),
        "birth_year": random.randint(1960, 2005),
        "gender": random.choice(["M", "F"]),
        "state": random.choice(US_STATES),
        "enrollment_date": random_date(2015, 2023),
        "source_system": source_system
    })

members_df = pd.DataFrame(members)

# =========================
# 5. INJECT CORRUPTION
# =========================

# Null birth_year
null_indices = members_df.sample(frac=NULL_BIRTH_YEAR_PCT, random_state=42).index
members_df.loc[null_indices, "birth_year"] = None

# Invalid gender
invalid_indices = members_df.sample(frac=INVALID_GENDER_PCT, random_state=24).index
members_df.loc[invalid_indices, "gender"] = random.choice(["X", "UNKNOWN", None])

# =========================
# 6. HISTORICAL SCD DATA
# =========================

print("Generating historical member policy data...")

historical_records = []

for _, row in members_df.iterrows():

    # High version skew
    if random.random() < HIGH_VERSION_MEMBER_PCT:
        versions = random.randint(4, 6)
    else:
        versions = random.randint(MIN_VERSIONS, MAX_VERSIONS)

    base_effective_date = random_date(2018, 2021)

    for v in range(versions):

        effective_date = base_effective_date + timedelta(days=365 * v)
        coverage_start = effective_date
        coverage_end = None if v == versions - 1 else effective_date + timedelta(days=364)

        # Inject overlap
        if random.random() < OVERLAP_PCT and v > 0:
            coverage_start = coverage_start - timedelta(days=random.randint(30,90))

        # Late arriving update logic
        record_updated_ts = effective_date + timedelta(days=random.randint(1,30))

        if random.random() < LATE_UPDATE_PCT:
            record_updated_ts = datetime(2025,1,1) + timedelta(days=random.randint(1,300))

        historical_records.append({
            **row.to_dict(),
            "policy_id": f"POL_{random.randint(1000,9999)}",
            "policy_type": random.choice(POLICY_TYPES),
            "coverage_start_date": coverage_start,
            "coverage_end_date": coverage_end,
            "member_segment": random.choice(SEGMENTS),
            "member_tier": random.choice(TIERS),
            "effective_date": effective_date,
            "record_updated_ts": record_updated_ts
        })

historical_df = pd.DataFrame(historical_records)

# =========================
# 7. DUPLICATE RECORDS
# =========================

dup_sample = historical_df.sample(frac=DUPLICATE_RECORD_PCT, random_state=99)
historical_df = pd.concat([historical_df, dup_sample], ignore_index=True)

# =========================
# 8. INGESTION METADATA
# =========================

historical_df["batch_id"] = f"BATCH_{random.randint(1,10)}"
historical_df["ingestion_timestamp"] = datetime.now()
historical_df["source_file_name"] = "historical_member_policy.csv"

# =========================
# 9. MIGRATED MEMBERS DATA
# =========================

print("Generating migrated members data...")

migrated_records = []

for i, row in members_df.iterrows():
    migrated_records.append({
        "member_id": generate_member_id(i+1),
        "legacy_member_id": row["legacy_member_id"],
        "migration_batch_id": f"BATCH_{random.randint(1,5)}",
        "migration_date": random_date(2023, 2025),
        "migration_status": random.choice(["Completed", "Failed", "Reprocessed"]),
        "migration_flag": random.choice(["Y","N"]),
        "migration_source_system": row["source_system"]
    })

migrated_df = pd.DataFrame(migrated_records)

# =========================
# 10. WRITE TO CSV
# =========================

print("Writing files...")

os.makedirs(HISTORICAL_PATH, exist_ok=True)
os.makedirs(MIGRATED_PATH, exist_ok=True)

historical_df.to_csv(os.path.join(HISTORICAL_PATH, "historical_member_policy.csv"), index=False)
migrated_df.to_csv(os.path.join(MIGRATED_PATH, "migrated_members.csv"), index=False)

print("Done ✅")
print(f"Historical rows: {len(historical_df)}")
print(f"Migrated rows: {len(migrated_df)}")