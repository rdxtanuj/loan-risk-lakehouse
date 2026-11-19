from pathlib import Path
import duckdb

GOLD_DIR = Path("data/gold")

def generate_gold_aggregations(partition_dt: str) -> None:
    """
    Uses DuckDB to join Silver loans and payments tables, computing senior-level 
    credit risk metrics (PD, EAD, LGD-based Expected Loss, and Risk-Adjusted Revenue).
    """
    # Initialize an in-memory DuckDB analytical session
    con = duckdb.connect()

    # Register Silver Parquet files directly as virtual SQL tables
    con.execute(f"""
        CREATE OR REPLACE VIEW silver_loans AS 
        SELECT * FROM read_parquet('data/silver/loans/dt={partition_dt}/*.parquet');
        
        CREATE OR REPLACE VIEW silver_payments AS 
        SELECT * FROM read_parquet('data/silver/payments/dt={partition_dt}/*.parquet');
    """)

    # 1. SENIOR CREDIT RISK SUMMARY: EAD, PD, LGD Expected Loss & Risk-Adjusted Margin
    risk_summary_query = """
        WITH base_aggregation AS (
            SELECT 
                l.asset_class,
                l.city,
                COUNT(l.loan_id) AS total_loans,
                ROUND(SUM(l.principal_amount), 2) AS total_portfolio_exposure,
                ROUND(AVG(l.credit_score), 1) AS avg_credit_score,
                ROUND(AVG(l.interest_rate), 2) AS avg_interest_rate,
                SUM(CASE WHEN l.status = 'DEFAULTED' THEN l.principal_amount ELSE 0 END) AS defaulted_exposure,
                ROUND(
                    SUM(CASE WHEN l.status = 'DEFAULTED' THEN l.principal_amount ELSE 0 END) * 100.0 / 
                    NULLIF(SUM(l.principal_amount), 0), 2
                ) AS default_rate_pct
            FROM silver_loans l
            GROUP BY l.asset_class, l.city
        )
        SELECT 
            asset_class,
            city,
            total_loans,
            total_portfolio_exposure,
            avg_credit_score,
            avg_interest_rate,
            defaulted_exposure,
            default_rate_pct,
            -- Senior Metric: Expected Loss assuming 60% Loss Given Default (LGD)
            ROUND(total_portfolio_exposure * (default_rate_pct / 100.0) * 0.60, 2) AS estimated_expected_loss,
            -- Senior Metric: Risk-Adjusted Revenue (Interest Earned vs Expected Loss)
            ROUND(
                (total_portfolio_exposure * (avg_interest_rate / 100.0)) - 
                (total_portfolio_exposure * (default_rate_pct / 100.0) * 0.60), 2
            ) AS net_risk_adjusted_revenue
        FROM base_aggregation
        ORDER BY net_risk_adjusted_revenue ASC;
    """

    # Execute SQL query via DuckDB relation and convert directly to a Polars DataFrame
    df_summary = con.sql(risk_summary_query).pl()

    # 2. WRITE TO GOLD LAYER
    target_dir = GOLD_DIR / f"dt={partition_dt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "portfolio_risk_summary.parquet"
    
    df_summary.write_parquet(target_file, compression="snappy")
    print(f"[GOLD] Successfully generated senior risk summary -> {target_file}")
    
    # Print a clean preview table of the metrics in the terminal
    print("\n--- SENIOR PORTFOLIO RISK SUMMARY (TOP ROWS) ---")
    print(df_summary)

    con.close()

if __name__ == "__main__":
    generate_gold_aggregations("2026-11-16")