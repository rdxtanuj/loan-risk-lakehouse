# FinTech Credit Risk & Regulatory Lineage Lakehouse

An end-to-end, local-first Medallion Lakehouse engineered for **credit risk analytics, regulatory data lineage (BCBS 239 alignment), and fail-fast data quality governance** within an 8 GB memory footprint.

---

## Architecture Overview

```text
[Raw Loan & Payment Feeds]
          │
          ▼
   ┌─────────────┐
   │   BRONZE    │  Immutable, date-partitioned raw Parquet snapshots (dt=YYYY-MM-DD)
   └──────┬──────┘
          │
          ├───► [QUARANTINE]  Schema/domain rule failures isolated by error code
          │                   (e.g., MISSING_LOAN_ID, INVALID_PRINCIPAL_AMOUNT)
          ▼
   ┌─────────────┐
   │   SILVER    │  Strict typing, CDC primary-key deduplication, Snappy Parquet
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    GOLD     │  DuckDB credit risk marts (EAD, PD, 60% LGD Expected Loss,
   │             │  Net Risk-Adjusted Revenue) + Reconciliation Audit tables
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │  BI PORTAL  │  Interactive Streamlit risk & compliance portal (`app.py`)
   └─────────────┘



loan-risk-lakehouse/
├── src/
│   ├── pipeline.py                  # Prefect v3 DAG orchestration & SLA gating
│   ├── quality/
│   │   ├── validators.py            # Schema & business-rule quarantine routing
│   │   └── reconciliation.py        # Lineage accounting & fail-fast assertions
│   └── transform/
│       ├── silver_transform.py      # Type enforcement & CDC deduplication
│       └── gold_aggregations.py     # DuckDB credit risk & yield aggregations
├── tests/
│   └── test_lakehouse.py            # Pytest suite for CDC counts, assertions, & risk SQL
├── app.py                           # Streamlit executive risk & compliance portal
└── pyproject.toml                   # Pytest path & project configuration


Quickstart(Bash)


# 1. Activate virtual environment
.venv\Scripts\activate

# 2. Generate synthetic multi-day Bronze partitions (includes CDC duplicates & dirty rows)
python -m scripts.seed_sample_data --days 3 --rows 1000

# 3. Execute end-to-end Prefect pipeline (Silver -> Gold -> Audit Gates)
python -m src.pipeline

# 4. Run automated unit & quality-gate tests
pytest -v

# 5. Launch Executive Risk & Lineage Portal
streamlit run app.py