from pathlib import Path
import polars as pl
from src.quality.validators import route_quarantine

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")


def transform_loans_to_silver(partition_dt: str) -> None:
    bronze_path = BRONZE_DIR / "loans" / f"dt={partition_dt}" / "*.parquet"
    df = pl.read_parquet(bronze_path)

    # 1. TYPE ENFORCEMENT: Match exact columns present in the Bronze Parquet files
    df = df.with_columns([
        pl.col("loan_id").str.strip_chars().cast(pl.Utf8),
        pl.col("customer_id").str.strip_chars().cast(pl.Utf8),
        pl.col("customer_name").str.strip_chars().cast(pl.Utf8),
        pl.col("city").str.strip_chars().cast(pl.Utf8),
        pl.col("credit_score").cast(pl.Int32, strict=False),
        pl.col("asset_class").str.strip_chars().cast(pl.Utf8),
        pl.col("principal_amount").cast(pl.Float64, strict=False),
        pl.col("interest_rate").cast(pl.Float64, strict=False),
        pl.col("origination_date").str.to_date("%Y-%m-%d", strict=False),
        pl.col("status").str.strip_chars().str.to_uppercase().cast(pl.Utf8),
        pl.col("updated_at").str.to_datetime(strict=False),
    ])

    # 2. QUALITY GATES & QUARANTINE ROUTING
    df = route_quarantine(
        df,
        pl.col("loan_id").is_null() | (pl.col("loan_id") == ""),
        "MISSING_LOAN_ID",
        "loans",
        partition_dt,
    )
    df = route_quarantine(
        df,
        pl.col("principal_amount").is_null() | (pl.col("principal_amount") <= 0),
        "INVALID_PRINCIPAL_AMOUNT",
        "loans",
        partition_dt,
    )

    # 3. CDC DEDUPLICATION: Keep only the latest record per loan_id based on updated_at
    before_dedup = df.height
    df = (
        df.sort("updated_at", descending=True)
        .unique(subset=["loan_id"], keep="first")
        .sort("loan_id")
    )
    deduped_count = before_dedup - df.height
    if deduped_count > 0:
        print(f"[SILVER] Deduplicated {deduped_count} older CDC record(s) for loans.")

    # 4. WRITE TO SILVER
    target_dir = SILVER_DIR / "loans" / f"dt={partition_dt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "loans_silver.parquet"
    df.write_parquet(target_file, compression="snappy")
    print(f"[SILVER] Wrote {df.height} clean rows -> {target_file}")


def transform_payments_to_silver(partition_dt: str) -> None:
    bronze_path = BRONZE_DIR / "payments" / f"dt={partition_dt}" / "*.parquet"
    df = pl.read_parquet(bronze_path)

    # 1. TYPE ENFORCEMENT: Match exact columns present in the payments Bronze files
    df = df.with_columns([
        pl.col("payment_id").str.strip_chars().cast(pl.Utf8),
        pl.col("loan_id").str.strip_chars().cast(pl.Utf8),
        pl.col("payment_date").str.to_date("%Y-%m-%d", strict=False),
        pl.col("principal_paid").cast(pl.Float64, strict=False),
        pl.col("interest_paid").cast(pl.Float64, strict=False),
        pl.col("days_past_due").cast(pl.Int32, strict=False),
        pl.col("event_timestamp").str.to_datetime(strict=False),
    ])

    # 2. QUALITY GATES & QUARANTINE ROUTING
    df = route_quarantine(
        df,
        pl.col("payment_id").is_null() | pl.col("loan_id").is_null(),
        "MISSING_PRIMARY_OR_FOREIGN_KEY",
        "payments",
        partition_dt,
    )
    df = route_quarantine(
        df,
        (pl.col("principal_paid").is_null() | (pl.col("principal_paid") < 0)) |
        (pl.col("interest_paid").is_null() | (pl.col("interest_paid") < 0)),
        "INVALID_PAYMENT_AMOUNTS",
        "payments",
        partition_dt,
    )

    # 3. DEDUPLICATION: Remove duplicate payment events based on payment_id
    before_dedup = df.height
    df = df.unique(subset=["payment_id"], keep="first").sort("payment_id")
    deduped_count = before_dedup - df.height
    if deduped_count > 0:
        print(f"[SILVER] Deduplicated {deduped_count} duplicate payment(s).")

    # 4. WRITE TO SILVER
    target_dir = SILVER_DIR / "payments" / f"dt={partition_dt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "payments_silver.parquet"
    df.write_parquet(target_file, compression="snappy")
    print(f"[SILVER] Wrote {df.height} clean rows -> {target_file}")


if __name__ == "__main__":
    transform_loans_to_silver("2026-11-16")
    transform_payments_to_silver("2026-11-16")