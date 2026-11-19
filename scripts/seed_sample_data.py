import argparse
import random
from datetime import date, timedelta
from pathlib import Path
import polars as pl


ASSET_CLASSES = ["Home Loan", "Auto Loan", "Personal Loan", "MSME Working Capital"]
CITIES = ["Mumbai", "Bengaluru", "Delhi", "Pune", "Hyderabad", "Chennai"]
STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "DEFAULT"]


def generate_partition(
    dt_str: str,
    num_loans: int = 1000,
    seed: int = 42,
    base_dir: Path = Path("data/bronze"),
) -> None:
    rng = random.Random(seed)
    loans = []
    payments = []

    for i in range(1, num_loans + 1):
        loan_id = f"LN-{dt_str.replace('-', '')}-{i:05d}"
        asset_class = rng.choice(ASSET_CLASSES)
        city = rng.choice(CITIES)
        status = rng.choice(STATUSES)

        if asset_class == "Home Loan":
            principal = round(rng.uniform(1_500_000, 8_500_000), 2)
            rate = round(rng.uniform(8.2, 9.8), 2)
        elif asset_class == "Auto Loan":
            principal = round(rng.uniform(350_000, 1_800_000), 2)
            rate = round(rng.uniform(9.0, 11.5), 2)
        elif asset_class == "MSME Working Capital":
            principal = round(rng.uniform(500_000, 4_000_000), 2)
            rate = round(rng.uniform(11.0, 15.5), 2)
        else:
            principal = round(rng.uniform(50_000, 750_000), 2)
            rate = round(rng.uniform(12.5, 18.0), 2)

        updated_at = f"{dt_str}T08:00:00"
        loans.append(
            {
                "loan_id": loan_id,
                "customer_id": f"CUST-{rng.randint(10000, 99999)}",
                "asset_class": asset_class,
                "city": city,
                "principal_amount": principal,
                "interest_rate": rate,
                "loan_status": status,
                "updated_at": updated_at,
            }
        )

        # Generate 1-2 payment records per loan
        for p_idx in range(rng.randint(1, 2)):
            payments.append(
                {
                    "payment_id": f"PAY-{dt_str.replace('-', '')}-{i:05d}-{p_idx + 1}",
                    "loan_id": loan_id,
                    "payment_amount": round(principal * (rate / 100.0) / 12.0, 2),
                    "payment_status": "FAILED" if status == "DEFAULT" else "SUCCESS",
                    "payment_date": dt_str,
                }
            )

    # Inject ~2% intentional dirty records for Quarantine testing (keeps retention ~98% > 95% SLA)
    dirty_count = max(1, int(num_loans * 0.02))
    for idx in range(dirty_count):
        if idx % 2 == 0:
            # Missing loan_id violation
            loans[idx]["loan_id"] = None
        else:
            # Negative principal violation
            loans[idx]["principal_amount"] = -5000.0

    # Inject ~3% CDC duplicate updates with newer timestamps to test deduplication
    cdc_sample = rng.sample(loans[dirty_count:], k=max(1, int(num_loans * 0.03)))
    for record in cdc_sample:
        dup = record.copy()
        dup["updated_at"] = f"{dt_str}T14:30:00"
        loans.append(dup)

    loans_dir = base_dir / "loans" / f"dt={dt_str}"
    payments_dir = base_dir / "payments" / f"dt={dt_str}"
    loans_dir.mkdir(parents=True, exist_ok=True)
    payments_dir.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(loans).write_parquet(loans_dir / "loans.parquet")
    pl.DataFrame(payments).write_parquet(payments_dir / "payments.parquet")

    print(
        f"[{dt_str}] Seeded {len(loans)} raw loan rows "
        f"(incl. {dirty_count} dirty, {len(cdc_sample)} CDC updates) "
        f"and {len(payments)} payment rows."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic multi-day Bronze Parquet partitions for the Loan Risk Lakehouse."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="Number of consecutive date partitions to generate ending today (default: 3).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Base number of loans per day partition (default: 1000).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=date.today().isoformat(),
        help="Final partition date in YYYY-MM-DD format (default: today).",
    )
    args = parser.parse_args()

    end_dt = date.fromisoformat(args.end_date)
    for offset in range(args.days - 1, -1, -1):
        target_dt = (end_dt - timedelta(days=offset)).isoformat()
        generate_partition(dt_str=target_dt, num_loans=args.rows, seed=42 + offset)


if __name__ == "__main__":
    main()