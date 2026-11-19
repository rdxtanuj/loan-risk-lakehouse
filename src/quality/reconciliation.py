from datetime import datetime
from pathlib import Path
import polars as pl


class DataQualityAssertionError(AssertionError):
    """Raised when a lakehouse lineage, retention, or completeness rule fails."""


def _count_cdc_deduped_loans(partition_dt: str) -> int:
    """
    Counts exact duplicate loan_id rows present in Bronze (excluding null/blank IDs)
    that collapse to one row per loan_id in Silver.
    """
    bronze_glob = list(Path("data/bronze/loans").glob(f"dt={partition_dt}/*.parquet"))
    silver_file = Path(f"data/silver/loans/dt={partition_dt}/loans_silver.parquet")
    if not bronze_glob or not silver_file.exists():
        return 0

    df_bronze = pl.read_parquet(Path(f"data/bronze/loans/dt={partition_dt}/*.parquet"))
    if "loan_id" not in df_bronze.columns:
        return 0

    valid_id_rows = df_bronze.filter(
        pl.col("loan_id").is_not_null()
        & (pl.col("loan_id").cast(pl.Utf8).str.strip_chars() != "")
    )
    unique_valid_ids = valid_id_rows.select(
        pl.col("loan_id").cast(pl.Utf8).str.strip_chars().n_unique()
    ).item()
    return max(0, valid_id_rows.height - unique_valid_ids)


def run_reconciliation_assertions(
    reconciliation_df: pl.DataFrame,
    min_retention_pct: float = 95.0,
    cdc_deduped_count: int = 0,
) -> None:
    """
    Enforces senior-grade data quality contracts on the reconciliation metrics.
    Fails loudly before downstream consumers trust the partition.
    """
    row = reconciliation_df.row(0, named=True)
    bronze_raw = row["bronze_raw_records"]
    silver_clean = row["silver_clean_records"]
    quarantined = row["quarantined_records"]
    accounted = row["accounted_records_check"]
    retention_pct = row["data_retention_pct"]
    partition_dt = row["partition_date"]

    # Gate 1: Accounted records must equal Silver + Quarantine
    if accounted != (silver_clean + quarantined):
        raise DataQualityAssertionError(
            f"[{partition_dt}] Reconciliation mismatch: accounted={accounted} != "
            f"silver({silver_clean}) + quarantined({quarantined})"
        )

    # Gate 2: Bronze raw must be explained by Silver + Quarantine + CDC deduplication
    explained_total = silver_clean + quarantined + cdc_deduped_count
    if bronze_raw > 0 and explained_total != bronze_raw:
        raise DataQualityAssertionError(
            f"[{partition_dt}] Silent lineage discrepancy: bronze_raw={bronze_raw}, "
            f"silver={silver_clean}, quarantined={quarantined}, cdc_deduped={cdc_deduped_count}, "
            f"unaccounted_diff={bronze_raw - explained_total}"
        )

    # Gate 3: Retention floor check
    if bronze_raw > 0 and retention_pct < min_retention_pct:
        raise DataQualityAssertionError(
            f"[{partition_dt}] Data retention {retention_pct}% fell below threshold "
            f"{min_retention_pct}% (silver={silver_clean}, quarantined={quarantined}, bronze={bronze_raw})"
        )

    # Gate 4: Non-empty Silver when Bronze has data
    if bronze_raw > 0 and silver_clean == 0:
        raise DataQualityAssertionError(
            f"[{partition_dt}] Zero clean Silver records produced from {bronze_raw} Bronze records."
        )


def generate_audit_report(
    partition_dt: str,
    min_retention_pct: float = 95.0,
    enforce_assertions: bool = True,
) -> pl.DataFrame:
    """
    Computes data reconciliation and lineage metrics across Bronze, Silver,
    and Quarantine layers, persists the audit Parquet artifact, and optionally
    enforces fail-fast data quality assertions.
    """
    bronze_glob = list(Path("data/bronze/loans").glob(f"dt={partition_dt}/*.parquet"))
    silver_file = Path(f"data/silver/loans/dt={partition_dt}/loans_silver.parquet")
    quarantine_dir = Path(f"data/quarantine/loans/dt={partition_dt}")

    raw_bronze_count = (
        pl.read_parquet(Path(f"data/bronze/loans/dt={partition_dt}/*.parquet")).height
        if bronze_glob
        else 0
    )
    clean_silver_count = pl.read_parquet(silver_file).height if silver_file.exists() else 0

    quarantine_count = 0
    if quarantine_dir.exists():
        for q_file in quarantine_dir.glob("*.parquet"):
            quarantine_count += pl.read_parquet(q_file).height

    cdc_deduped_count = _count_cdc_deduped_loans(partition_dt)

    reconciliation_df = pl.DataFrame(
        {
            "partition_date": [partition_dt],
            "dataset_name": ["loans_portfolio"],
            "bronze_raw_records": [raw_bronze_count],
            "silver_clean_records": [clean_silver_count],
            "quarantined_records": [quarantine_count],
            "cdc_deduped_records": [cdc_deduped_count],
            "accounted_records_check": [clean_silver_count + quarantine_count],
            "data_retention_pct": [
                round((clean_silver_count / raw_bronze_count) * 100, 2)
                if raw_bronze_count > 0
                else 0.0
            ],
            "audit_timestamp": [datetime.now()],
        }
    )

    audit_dir = Path(f"data/gold/audit/dt={partition_dt}")
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_file = audit_dir / "lineage_reconciliation_audit.parquet"
    reconciliation_df.write_parquet(audit_file, compression="snappy")

    print("\n[AUDIT & LINEAGE REPORT COMPLETED]")
    print(reconciliation_df)

    if enforce_assertions:
        run_reconciliation_assertions(
            reconciliation_df=reconciliation_df,
            min_retention_pct=min_retention_pct,
            cdc_deduped_count=cdc_deduped_count,
        )
        print(f"[QUALITY GATES PASSED] Retention >= {min_retention_pct}% and lineage reconciled.")

    return reconciliation_df


if __name__ == "__main__":
    generate_audit_report("2026-11-16", min_retention_pct=95.0, enforce_assertions=True)