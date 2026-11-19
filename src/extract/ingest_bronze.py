import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
import polars as pl

# --- CONFIGURATION ---
LANDING_DIR = Path("data/landing")
ARCHIVE_DIR = Path("data/landing/archive")
BRONZE_DIR = Path("data/bronze")


def ingest_file_to_bronze(file_path: Path, entity_name: str, partition_dt: str, batch_id: str) -> int:
    """Reads a raw CSV or JSONL file as all-strings, appends audit metadata, and writes Parquet to Bronze."""
    ingested_at = datetime.now(timezone.utc).isoformat()

    # 1. READ: Pull raw data as unparsed strings (schema-on-read protection)
    if file_path.suffix == ".csv":
        df = pl.read_csv(file_path, infer_schema_length=0)
    elif file_path.suffix == ".jsonl":
        with open(file_path, mode="r", encoding="utf-8") as f:
            records = [
                {k: (None if v is None else str(v)) for k, v in json.loads(line).items()}
                for line in f
                if line.strip()
            ]
        df = pl.DataFrame(records)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")

    # 2. STAMP: Force Utf8 strings and attach lineage/audit columns
    df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in df.columns]).with_columns([
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(file_path.name).alias("_source_file"),
        pl.lit(batch_id).alias("_batch_id"),
    ])

    # 3. WRITE: Save as Snappy-compressed Parquet in Hive-partitioned folders
    target_dir = BRONZE_DIR / entity_name / f"dt={partition_dt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{file_path.stem}_{batch_id[:8]}.parquet"
    df.write_parquet(target_file, compression="snappy")

    # 4. CLEANUP: Move processed landing file to archive for idempotency
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(file_path), str(ARCHIVE_DIR / file_path.name))

    return df.height


# --- BATCH DISCOVERY & EXECUTION ---
def run_bronze_ingestion() -> None:
    batch_id = str(uuid.uuid4())
    csv_files = sorted(LANDING_DIR.glob("loans_batch_*.csv"))
    jsonl_files = sorted(LANDING_DIR.glob("payments_stream_*.jsonl"))

    if not csv_files and not jsonl_files:
        print("[BRONZE] No new files found in data/landing/.")
        return

    for file_path in csv_files:
        partition_dt = file_path.stem.replace("loans_batch_", "")
        rows = ingest_file_to_bronze(file_path, "loans", partition_dt, batch_id)
        print(f"[BRONZE] Ingested {rows} rows -> data/bronze/loans/dt={partition_dt}/")

    for file_path in jsonl_files:
        partition_dt = file_path.stem.replace("payments_stream_", "")
        rows = ingest_file_to_bronze(file_path, "payments", partition_dt, batch_id)
        print(f"[BRONZE] Ingested {rows} rows -> data/bronze/payments/dt={partition_dt}/")


if __name__ == "__main__":
    run_bronze_ingestion()