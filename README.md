# Credit Risk Reporting Platform

A real-world banking data engineering project that simulates a credit risk reporting pipeline using Python, PySpark, Parquet, validation rules, risk-band assignment, and Gold-level portfolio reporting.

## Business Problem

Banks and financial institutions need accurate credit risk reporting across loan portfolios. Raw loan data can contain missing customer identifiers, invalid loan amounts, out-of-range credit scores, duplicate loan IDs, and delinquency values that must be validated before reporting.

This project demonstrates how a data engineer can build a reliable credit risk pipeline that transforms raw loan records into validated and analytics-ready risk reports.

## Project Objective

Build a PySpark credit risk reporting platform that:

- Generates raw loan portfolio data
- Stores raw loan data in a Bronze layer
- Validates loan quality in a Silver layer
- Removes duplicate loan IDs
- Assigns LOW, MEDIUM, and HIGH risk bands
- Creates Gold-level credit risk reporting metrics

## Architecture

```text
Raw Loan Portfolio Data
        ↓
Bronze Layer
Raw loans stored as Parquet
        ↓
Silver Layer
Validated and risk-scored loans
        ↓
Gold Layer
Credit risk report by loan type and risk band
```

## Tech Stack

- Python 3.11
- PySpark
- Spark SQL DataFrame API
- Pandas
- Parquet
- Credit risk rules
- Data quality validation
- Bronze/Silver/Gold architecture
- Git/GitHub

## Dataset

The generated loan dataset includes:

- loan_id
- customer_id
- loan_amount
- credit_score
- days_past_due
- loan_type

## Pipeline Layers

### Bronze Layer

Stores raw loan portfolio records with a load timestamp.

### Silver Layer

Applies validation and risk scoring:

- loan_id must not be null
- customer_id must not be null
- loan_amount must be greater than zero
- credit_score must be between 300 and 850
- days_past_due must be non-negative
- duplicate loan_id records are removed
- risk_band is assigned based on delinquency and credit score

### Gold Layer

Creates reporting metrics:

- loan count
- total exposure
- average credit score
- average days past due
- grouped by loan_type and risk_band

## How to Run

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install pyspark pandas

python src/run_pipeline.py
python src/pyspark_credit_risk_pipeline.py
python src/project_summary.py
```

## Expected Outputs

```text
data/output/bronze_loans_parquet
data/output/silver_loans_risk_scored_parquet
data/output/gold_credit_risk_report_parquet
```

## Project Summary Script

`src/project_summary.py` validates the pipeline output by showing:

- raw record count
- Bronze count
- Silver count
- Gold count
- duplicate loan_id proof before and after deduplication
- invalid record proof before and after validation
- risk band distribution
- sample Gold output

## Key Data Engineering Concepts Demonstrated

- PySpark batch processing
- Explicit schema enforcement
- Banking credit risk data modeling
- Data quality validation
- Duplicate loan handling
- Risk-band assignment
- Portfolio exposure aggregation
- Gold reporting layer creation
- Parquet analytical outputs

## Resume Bullet

Built a PySpark credit risk reporting platform that validates raw loan portfolio data, removes duplicate loan IDs, assigns risk bands using credit score and delinquency rules, and publishes Bronze, Silver, and Gold Parquet layers for banking risk analytics.
