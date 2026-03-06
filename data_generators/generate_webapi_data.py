from flask import Flask, jsonify
import random
from datetime import datetime, timedelta
import uuid

app = Flask(__name__)

TOTAL_MEMBERS = 50000

# =========================
# CHAOS CONTROLS
# =========================

DUPLICATE_EVENT_PCT = 0.02
NULL_MEMBER_PCT = 0.01
CORRUPT_PAYLOAD_PCT = 0.02
UNKNOWN_EVENT_PCT = 0.01
LATE_EVENT_PCT = 0.05

# =========================
# EVENT TYPES
# =========================

MEMBER_EVENTS = [
    "MemberEnrolled",
    "MemberUpdated",
    "MemberSuspended",
    "MemberReactivated"
]

CLAIM_EVENTS = [
    "ClaimSubmitted",
    "ClaimApproved",
    "ClaimDenied",
    "ClaimEscalated"
]

POLICY_EVENTS = [
    "PolicyIssued",
    "PolicyRenewed",
    "PolicyCancelled"
]

BILLING_EVENTS = [
    "PaymentReceived",
    "PaymentFailed",
    "RefundIssued"
]

SOURCE_SYSTEMS = [
    "MemberService",
    "ClaimsEngine",
    "PolicyAdmin",
    "BillingSystem"
]

# =========================
# HELPERS
# =========================

def generate_member_id():
    return f"MBR_{str(random.randint(1, TOTAL_MEMBERS)).zfill(5)}"

def random_timestamp():
    now = datetime.utcnow()
    return now - timedelta(seconds=random.randint(0, 86400))

def random_email(first, last):
    return f"{first.lower()}.{last.lower()}@healthmail.com"

def random_name():
    first = random.choice(["John","Jane","David","Emma","Chris","Olivia"])
    last = random.choice(["Smith","Brown","Taylor","Wilson","Johnson"])
    return first, last

def random_address():
    return {
        "line1": f"{random.randint(100,999)} Main St",
        "city": random.choice(["Dallas","Austin","Miami","Chicago"]),
        "state": random.choice(["TX","FL","IL"]),
        "zip": str(random.randint(10000,99999))
    }

# =========================
# MAIN ENDPOINT
# =========================

@app.route("/events", methods=["GET"])
def get_events():

    events_per_request = random.randint(200, 5000)
    events = []
    generated_event_ids = []

    for _ in range(events_per_request):

        category = random.choice(["MEMBER","CLAIMS","POLICY","BILLING"])

        if category == "MEMBER":
            event_type = random.choice(MEMBER_EVENTS)
        elif category == "CLAIMS":
            event_type = random.choice(CLAIM_EVENTS)
        elif category == "POLICY":
            event_type = random.choice(POLICY_EVENTS)
        else:
            event_type = random.choice(BILLING_EVENTS)

        # Unknown event injection
        if random.random() < UNKNOWN_EVENT_PCT:
            event_type = "UnknownEventType"

        event_id = f"EVT_{uuid.uuid4().hex[:16]}"

        # Duplicate injection
        if random.random() < DUPLICATE_EVENT_PCT and generated_event_ids:
            event_id = random.choice(generated_event_ids)

        generated_event_ids.append(event_id)

        member_id = generate_member_id()

        if random.random() < NULL_MEMBER_PCT:
            member_id = None

        event_timestamp = random_timestamp()

        if random.random() < LATE_EVENT_PCT:
            event_timestamp -= timedelta(days=random.randint(2,5))

        schema_version = "v2" if random.random() < 0.7 else "v1"

        # =========================
        # PAYLOAD GENERATION
        # =========================

        payload = {}

        # -------- MEMBER EVENTS --------
        if category == "MEMBER":

            first, last = random_name()
            address = random_address()

            if schema_version == "v2":
                payload = {
                    "profile": {
                        "first_name": first,
                        "last_name": last,
                        "dob": str(datetime(
                            random.randint(1960,2005),
                            random.randint(1,12),
                            random.randint(1,28)
                        ).date()),
                        "email": random_email(first, last),
                        "address": address,
                        "change_type": random.choice(
                            ["AddressUpdate","PhoneUpdate"]
                        )
                    }
                }
            else:  # v1 flat
                payload = {
                    "first_name": first,
                    "last_name": last,
                    "dob": str(datetime(
                        random.randint(1960,2005),
                        random.randint(1,12),
                        random.randint(1,28)
                    ).date()),
                    "email": random_email(first, last),
                    "city": address["city"],
                    "state": address["state"],
                    "change_type": "AddressUpdate"
                }

        # -------- CLAIM EVENTS --------
        elif category == "CLAIMS":

            diagnosis_codes = [
                f"D{random.randint(100,999)}"
                for _ in range(random.randint(1,3))
            ]

            if schema_version == "v2":
                payload = {
                    "claim": {
                        "id": f"CLM_{random.randint(1,500000)}",
                        "amount": random.randint(100,20000),
                        "diagnosis_codes": diagnosis_codes
                    },
                    "provider": {
                        "id": f"PRV_{random.randint(100,999)}",
                        "type": random.choice(
                            ["Hospital","Clinic"]
                        )
                    }
                }
            else:
                payload = {
                    "claim_id": f"CLM_{random.randint(1,500000)}",
                    "claim_amount": random.randint(100,20000),
                    "provider_id": f"PRV_{random.randint(100,999)}"
                }

        # -------- POLICY EVENTS --------
        elif category == "POLICY":

            if schema_version == "v2":
                payload = {
                    "policy": {
                        "id": f"POL_{random.randint(1000,9999)}",
                        "coverage": {
                            "start_date": str(datetime.utcnow().date()),
                            "end_date": str(
                                (datetime.utcnow() +
                                 timedelta(days=365)).date()
                            )
                        }
                    }
                }
            else:
                payload = {
                    "policy_id": f"POL_{random.randint(1000,9999)}",
                    "policy_status": "Active"
                }

        # -------- BILLING EVENTS --------
        else:

            methods = random.sample(
                ["CARD","UPI","BANK_TRANSFER"],
                random.randint(1,2)
            )

            if schema_version == "v2":
                payload = {
                    "payment": {
                        "amount": random.randint(100,5000),
                        "methods": methods
                    }
                }
            else:
                payload = {
                    "payment_amount": random.randint(100,5000),
                    "payment_status": "Success"
                }

        # Corrupt payload injection
        if random.random() < CORRUPT_PAYLOAD_PCT:
            payload = {"corrupt_field": "###INVALID###"}

        event = {
            "event_id": event_id,
            "event_type": event_type,
            "event_category": category,
            "event_timestamp": event_timestamp.isoformat(),
            "member_id": member_id,
            "source_system": random.choice(SOURCE_SYSTEMS),
            "schema_version": schema_version,
            "trace_id": f"TRACE_{uuid.uuid4().hex[:12]}",
            "payload": payload
        }

        events.append(event)

    return jsonify(events)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)