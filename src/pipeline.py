from datetime import datetime
from prefect import flow, task

from src.quality.reconciliation import generate_audit_report
from src.transform.gold_aggregations import generate_gold_aggregations
from src.transform.silver_transform import (
    transform_loans_to_silver,
    transform_payments_to_silver,
)


@task(name="Transform Loans to Silver", retries=2, retry_delay_seconds=5)
def run_loans_silver(partition_dt: str) -> None:
    print(f"[PREFECT TASK] Starting Silver transformation for loans (dt={partition_dt})")
    transform_loans_to_silver(partition_dt)


@task(name="Transform Payments to Silver", retries=2, retry_delay_seconds=5)
def run_payments_silver(partition_dt: str) -> None:
    print(f"[PREFECT TASK] Starting Silver transformation for payments (dt={partition_dt})")
    transform_payments_to_silver(partition_dt)


@task(name="Generate Gold Risk Aggregations", retries=1, retry_delay_seconds=3)
def run_gold_layer(partition_dt: str) -> None:
    print(f"[PREFECT TASK] Starting Gold layer aggregations (dt={partition_dt})")
    generate_gold_aggregations(partition_dt)


@task(name="Generate Reconciliation Audit", retries=1, retry_delay_seconds=2)
def run_audit_report(partition_dt: str, min_retention_pct: float = 95.0) -> None:
    print(
        f"[PREFECT TASK] Running Lineage & Reconciliation Audit "
        f"(dt={partition_dt}, min_retention_pct={min_retention_pct}%)"
    )
    generate_audit_report(
        partition_dt=partition_dt,
        min_retention_pct=min_retention_pct,
        enforce_assertions=True,
    )


@flow(name="Loan Risk Lakehouse Pipeline", log_prints=True)
def loan_risk_pipeline(
    partition_dt: str | None = None,
    min_retention_pct: float = 95.0,
) -> None:
    """
    Production-grade DAG orchestration for the FinTech Loan Risk Lakehouse.
    Executes Silver transformations, Gold credit risk aggregations, and
    fail-fast lineage/reconciliation quality gates for a target date partition.
    """
    if not partition_dt:
        partition_dt = datetime.today().strftime("%Y-%m-%d")

    print(f"=== Starting Lakehouse Pipeline Execution for Partition: {partition_dt} ===")

    # 1. Transform Bronze to Silver (with Quarantine & CDC Deduplication)
    run_loans_silver(partition_dt)
    run_payments_silver(partition_dt)

    # 2. Generate Senior Risk Aggregations in Gold
    run_gold_layer(partition_dt)

    # 3. Enforce Regulatory Lineage & Data Quality Assertions
    run_audit_report(partition_dt, min_retention_pct=min_retention_pct)

    print(f"=== Lakehouse Pipeline Execution Completed Successfully for {partition_dt}! ===")


if __name__ == "__main__":
    loan_risk_pipeline(partition_dt="2026-11-16", min_retention_pct=95.0)