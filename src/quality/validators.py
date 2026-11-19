from datetime import datetime, timezone
from pathlib import Path
import polars as pl

QUARANTINE_DIR = Path("data/quarantine")


def route_quarantine(
    df: pl.DataFrame,
    invalid_condition: pl.Expr,
    reason: str,
    entity_name: str,
    partition_dt: str,
) -> pl.DataFrame:
    """Splits invalid rows into data/quarantine/ with a _failure_reason column and returns clean rows."""
    bad_rows = df.filter(invalid_condition)
    clean_rows = df.filter(~invalid_condition)

    if bad_rows.height > 0:
        quarantined_at = datetime.now(timezone.utc).isoformat()
        bad_rows = bad_rows.with_columns([
            pl.lit(reason).alias("_failure_reason"),
            pl.lit(quarantined_at).alias("_quarantined_at"),
        ])

        target_dir = QUARANTINE_DIR / entity_name / f"dt={partition_dt}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / f"quarantine_{reason.lower().replace(' ', '_')}.parquet"
        bad_rows.write_parquet(target_file, compression="snappy")
        print(f"[QUARANTINE] Routed {bad_rows.height} {entity_name} row(s) -> {target_file} ({reason})")

    return clean_rows