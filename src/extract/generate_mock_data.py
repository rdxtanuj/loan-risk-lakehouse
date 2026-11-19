import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker("en_IN")
random.seed(42)

LANDING_DIR = Path("data/landing")
ASSET_CLASSES = ["AUTO_ABS", "RMBS", "CLO", "CREDIT_CARD_ABS", "CMBS", "STUDENT_LOAN_ABS"]
LOAN_STATUSES = ["ACTIVE", "PAID_OFF", "DELINQUENT", "DEFAULT"]


def generate_daily_batch(run_date: str, num_loans: int = 500, inject_dirty_data: bool = True) -> None:
    """Generates raw loan applications (CSV) and repayment events (JSONL) in data/landing."""
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    loans_file = LANDING_DIR / f"loans_batch_{run_date}.csv"
    payments_file = LANDING_DIR / f"payments_stream_{run_date}.jsonl"

    loans = []
    for i in range(1, num_loans + 1):
        loan_id = f"LN-{run_date.replace('-', '')}-{i:05d}"
        customer_id = f"CUST-{random.randint(1000, 1400)}"
        loans.append({
            "loan_id": loan_id,
            "customer_id": customer_id,
            "customer_name": fake.name(),
            "city": fake.city(),
            "credit_score": random.randint(580, 850),
            "asset_class": random.choice(ASSET_CLASSES),
            "principal_amount": round(random.uniform(50000, 1500000), 2),
            "interest_rate": round(random.uniform(7.5, 18.5), 2),
            "origination_date": run_date,
            "status": random.choices(LOAN_STATUSES, weights=[0.75, 0.12, 0.08, 0.05])[0],
            "updated_at": f"{run_date}T08:00:00Z",
        })

    if inject_dirty_data:
        # 1. Exact duplicate record with a later timestamp (tests CDC deduplication)
        dup_loan = loans[0].copy()
        dup_loan["status"] = "DELINQUENT"
        dup_loan["updated_at"] = f"{run_date}T14:30:00Z"
        loans.append(dup_loan)

        # 2. Corrupt records (tests quarantine / Dead Letter Queue)
        loans.append({
            "loan_id": None,  # Null Primary Key
            "customer_id": "CUST-9999",
            "customer_name": "Corrupt User",
            "city": "Mumbai",
            "credit_score": 710,
            "asset_class": "RMBS",
            "principal_amount": 250000.00,
            "interest_rate": 9.5,
            "origination_date": run_date,
            "status": "ACTIVE",
            "updated_at": f"{run_date}T09:00:00Z",
        })
        loans.append({
            "loan_id": f"LN-{run_date.replace('-', '')}-BAD01",
            "customer_id": "CUST-8888",
            "customer_name": "Negative Loan",
            "city": "Bengaluru",
            "credit_score": 650,
            "asset_class": "AUTO_ABS",
            "principal_amount": -45000.00,  # Invalid negative principal
            "interest_rate": 11.0,
            "origination_date": "invalid-date",  # Malformed date
            "status": "UNKNOWN_CODE",  # Invalid enum
            "updated_at": f"{run_date}T10:00:00Z",
        })

    with open(loans_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(loans[0].keys()))
        writer.writeheader()
        writer.writerows(loans)

    # Generate payment transactions for ~60% of loans
    payments = []
    valid_loans = [l for l in loans if l["loan_id"] and l["principal_amount"] > 0]
    for idx, loan in enumerate(random.sample(valid_loans, k=int(len(valid_loans) * 0.6)), start=1):
        payments.append({
            "payment_id": f"PAY-{run_date.replace('-', '')}-{idx:05d}",
            "loan_id": loan["loan_id"],
            "payment_date": run_date,
            "principal_paid": round(loan["principal_amount"] * 0.02, 2),
            "interest_paid": round((loan["principal_amount"] * (loan["interest_rate"] / 100)) / 12, 2),
            "days_past_due": random.choice([0, 0, 0, 5, 15, 45]),
            "event_timestamp": f"{run_date}T{random.randint(9, 20):02d}:15:00Z",
        })

    if inject_dirty_data:
        # Duplicate payment event (at-least-once stream delivery simulation)
        payments.append(payments[0].copy())

    with open(payments_file, mode="w", encoding="utf-8") as f:
        for p in payments:
            f.write(json.dumps(p) + "\n")

    print(f"[SIMULATOR] Generated {len(loans)} loans -> {loans_file}")
    print(f"[SIMULATOR] Generated {len(payments)} payments -> {payments_file}")


if __name__ == "__main__":
    generate_daily_batch(run_date="2026-11-16", num_loans=500, inject_dirty_data=True)