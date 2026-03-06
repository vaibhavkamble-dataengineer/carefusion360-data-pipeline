import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# =========================
# CONSTANTS
# =========================

TOTAL_MEMBERS = 50000
CLAIMS_PER_MEMBER_MIN = 5
CLAIMS_PER_MEMBER_MAX = 10

BASE_OUTPUT_PATH = "/home/salvador/carefusion360/raw_data/snowflake"

random.seed(42)
np.random.seed(42)

# Chaos Controls
ORPHAN_CLAIM_PCT = 0.03
DUPLICATE_CLAIM_PCT = 0.02
NEGATIVE_CLAIM_PCT = 0.02
OUTLIER_CLAIM_PCT = 0.01
ORPHAN_ADJUST_PCT = 0.03
BAD_ADJUST_MATH_PCT = 0.02
NEGATIVE_REVENUE_PCT = 0.02
DUPLICATE_VIOLATION_PCT = 0.02

# =========================
# HELPERS
# =========================

def generate_member_id(i):
    return f"MBR_{str(i).zfill(5)}"

def random_date(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

# =========================
# MEMBERS_CURRENT
# =========================

members_current = []

for i in range(1, TOTAL_MEMBERS + 1):
    members_current.append({
        "member_id": generate_member_id(i),
        "legacy_member_id": f"LEG_{str(i).zfill(6)}",
        "birth_year": random.randint(1960, 2005),
        "gender": random.choice(["M", "F"]),
        "state": random.choice(["CA","TX","NY","FL","IL"]),
        "enrollment_date": random_date(2018, 2023),
        "current_policy_id": f"POL_{random.randint(1000,9999)}",
        "member_status": random.choice(["Active","Suspended","Terminated"])
    })

members_current_df = pd.DataFrame(members_current)

# =========================
# POLICIES
# =========================

policies = []

for pid in members_current_df["current_policy_id"].unique():
    start_date = random_date(2022, 2024)
    policies.append({
        "policy_id": pid,
        "policy_type": random.choice(["HMO","PPO","EPO"]),
        "premium_amount": random.randint(200,1000),
        "coverage_start_date": start_date,
        "coverage_end_date": start_date + timedelta(days=365),
        "policy_status": random.choice(["Active","Expired"])
    })

policies_df = pd.DataFrame(policies)

# =========================
# PROVIDER SKEW
# =========================

heavy_providers = [f"PRV_{i}" for i in range(100,110)]
normal_providers = [f"PRV_{i}" for i in range(110,999)]

# =========================
# CLAIMS
# =========================

claims = []
claim_counter = 1

for i in range(1, TOTAL_MEMBERS + 1):
    member_id = generate_member_id(i)
    claim_count = random.randint(CLAIMS_PER_MEMBER_MIN, CLAIMS_PER_MEMBER_MAX)

    for _ in range(claim_count):

        if random.random() < ORPHAN_CLAIM_PCT:
            member_id_use = f"MBR_{random.randint(60000,70000)}"
        else:
            member_id_use = member_id

        claim_amount = random.randint(500,15000)

        if random.random() < NEGATIVE_CLAIM_PCT:
            claim_amount = -abs(claim_amount)

        if random.random() < OUTLIER_CLAIM_PCT:
            claim_amount = random.randint(100000,200000)

        provider_id = random.choice(heavy_providers) if random.random() < 0.6 else random.choice(normal_providers)

        claims.append({
            "claim_id": f"CLM_{claim_counter}",
            "member_id": member_id_use,
            "provider_id": provider_id,
            "policy_id": f"POL_{random.randint(1000,9999)}",
            "claim_date": random_date(2022, 2025),
            "claim_amount": claim_amount,
            "claim_status": random.choice(["Approved","Pending","Denied"])
        })

        claim_counter += 1

claims_df = pd.DataFrame(claims)

dup_claims = claims_df.sample(frac=DUPLICATE_CLAIM_PCT, random_state=99)
claims_df = pd.concat([claims_df, dup_claims], ignore_index=True)

# =========================
# CLAIM ADJUSTMENTS
# =========================

adjustments = []

for _, row in claims_df.iterrows():
    if random.random() < 0.25:

        claim_id_use = row["claim_id"]

        if random.random() < ORPHAN_ADJUST_PCT:
            claim_id_use = f"CLM_{random.randint(900000,999999)}"

        adjustment_amount = random.randint(-500,500)
        adjusted_final = row["claim_amount"] + adjustment_amount

        if random.random() < BAD_ADJUST_MATH_PCT:
            adjusted_final += random.randint(1,100)

        adjustments.append({
            "adjustment_id": f"ADJ_{claim_id_use}",
            "claim_id": claim_id_use,
            "member_id": row["member_id"],
            "original_claim_amount": row["claim_amount"],
            "adjustment_amount": adjustment_amount,
            "adjusted_final_amount": adjusted_final,
            "adjustment_date": row["claim_date"] - timedelta(days=random.randint(1,10))
        })

adjustments_df = pd.DataFrame(adjustments)

# =========================
# BILLING
# =========================

billing = []

for i in range(1, TOTAL_MEMBERS + 1):
    billing.append({
        "billing_id": f"BILL_{i}",
        "member_id": generate_member_id(i),
        "billing_date": random_date(2023, 2025),
        "billed_amount": random.randint(200,2000),
        "paid_amount": random.randint(100,2000),
        "payment_status": random.choice(["Paid","Partial","Pending"])
    })

billing_df = pd.DataFrame(billing)

# =========================
# REVENUE
# =========================

revenue = []

for i in range(1, TOTAL_MEMBERS + 1):
    member_id = generate_member_id(i)

    for month in range(1,13):
        premium = random.randint(200,1000)

        if random.random() < NEGATIVE_REVENUE_PCT:
            premium = -abs(premium)

        revenue.append({
            "revenue_id": f"REV_{member_id}_{month}",
            "member_id": member_id,
            "revenue_date": datetime(2024, month, 1),
            "premium_collected": premium,
            "commission_paid": random.randint(50,200)
        })

revenue_df = pd.DataFrame(revenue)

# =========================
# VIOLATIONS
# =========================

violations = []

for i in random.sample(range(1, TOTAL_MEMBERS + 1), int(TOTAL_MEMBERS * 0.08)):
    violations.append({
        "violation_id": f"VIO_{i}",
        "member_id": generate_member_id(i),
        "violation_type": random.choice(["Fraud","Late Payment","Policy Abuse"]),
        "violation_date": random_date(2023, 2025),
        "severity_level": random.choice(["Low","Medium","High"])
    })

violations_df = pd.DataFrame(violations)

dup_vio = violations_df.sample(frac=DUPLICATE_VIOLATION_PCT, random_state=88)
violations_df = pd.concat([violations_df, dup_vio], ignore_index=True)

# =========================
# WRITE ALL TABLES
# =========================

tables = {
    "members_current": members_current_df,
    "policies": policies_df,
    "claims": claims_df,
    "claims_adjustments": adjustments_df,
    "billing": billing_df,
    "revenue": revenue_df,
    "violations": violations_df
}

for table, df in tables.items():
    path = os.path.join(BASE_OUTPUT_PATH, table)
    os.makedirs(path, exist_ok=True)
    df.to_csv(os.path.join(path, f"{table}.csv"), index=False)

print("Heavy Chaos Snowflake Data Generated ✅")