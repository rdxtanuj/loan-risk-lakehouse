from datetime import datetime
from pathlib import Path
import duckdb
import polars as pl
import pytest

from src.quality import reconciliation
from src.quality.reconciliation import (
    DataQualityAssertionError,
    _count_cdc_deduped_loans,
    run_reconciliation_assertions,
)


def test_cdc_deduped_excludes_quarantined_and_null_ids(tmp_path: Path, monkeypatch):
    """
    503 raw rows: 1 missing loan_id, 1 quarantined invalid principal (with valid ID),
    1 duplicate loan_id pair -> 1 deduplicated CDC record.
    """
    dt = "2026-11-16"
    bronze_dir = tmp_path / "data" / "bronze" / "loans" / f"dt={dt}"
    silver_dir = tmp_path / "data" / "silver" / "loans" / f"dt={dt}"
    bronze_dir.mkdir(parents=True)
    silver_dir.mkdir(parents=True)

    # 500 unique valid loans + 1 CDC duplicate of L001 + 1 quarantined valid-ID loan (L999) + 1 null ID = 503
    ids = [f"L{i:04d}" for i in range(1, 501)] + ["L0001", "L0999", None]
    pl.DataFrame({"loan_id": ids}).write_parquet(bronze_dir / "part-000.parquet")
    pl.DataFrame({"loan_id": [f"L{i:04d}" for i in range(1, 501)]}).write_parquet(
        silver_dir / "loans_silver.parquet"
    )

    monkeypatch.chdir(tmp_path)
    assert _count_cdc_deduped_loans(dt) == 1


def test_reconciliation_assertions_pass_and_fail():
    good_df = pl.DataFrame(
        {
            "partition_date": ["2026-11-16"],
            "dataset_name": ["loans_portfolio"],
            "bronze_raw_records": [503],
            "silver_clean_records": [500],
            "quarantined_records": [2],
            "cdc_deduped_records": [1],
            "accounted_records_check": [502],
            "data_retention_pct": [99.4],
            "audit_timestamp": [datetime.now()],
        }
    )
    run_reconciliation_assertions(good_df, min_retention_pct=95.0, cdc_deduped_count=1)

    # Low retention must fail fast
    low_retention_df = good_df.with_columns(pl.lit(80.0).alias("data_retention_pct"))
    with pytest.raises(DataQualityAssertionError, match="below threshold"):
        run_reconciliation_assertions(low_retention_df, min_retention_pct=95.0, cdc_deduped_count=1)

    # Unaccounted row discrepancy must fail fast
    with pytest.raises(DataQualityAssertionError, match="Silent lineage discrepancy"):
        run_reconciliation_assertions(good_df, min_retention_pct=95.0, cdc_deduped_count=2)


def test_gold_credit_risk_formulas_in_duckdb():
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TEMP TABLE loans AS
            SELECT * FROM (VALUES
                ('CLO', 'Purnia', 100000.0, 12.0, 'DEFAULTED'),
                ('CLO', 'Purnia', 100000.0, 12.0, 'CURRENT')
            ) AS t(asset_class, city, principal_amount, interest_rate, status);
            """
        )
        row = con.sql(
            """
            WITH agg AS (
                SELECT
                    asset_class,
                    city,
                    SUM(principal_amount) AS exposure,
                    AVG(interest_rate) AS rate,
                    SUM(CASE WHEN status = 'DEFAULTED' THEN principal_amount ELSE 0 END) * 100.0
                        / NULLIF(SUM(principal_amount), 0) AS pd_pct
                FROM loans
                GROUP BY asset_class, city
            )
            SELECT
                ROUND(exposure * (pd_pct / 100.0) * 0.60, 2) AS expected_loss,
                ROUND((exposure * (rate / 100.0)) - (exposure * (pd_pct / 100.0) * 0.60), 2) AS net_rev
            FROM agg
            """
        ).pl().row(0)
        assert row[0] == 60000.0
        assert row[1] == -36000.0
    finally:
        con.close()