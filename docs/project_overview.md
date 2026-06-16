# Credit Risk Reporting Platform

## Business Problem
Banking teams need reliable credit risk reporting across loan portfolios, including exposure, delinquency, credit score distribution, and risk-band segmentation.

## Objective
Build a credit risk reporting pipeline that scores loans, assigns risk bands, and creates portfolio-level reporting outputs.

## Architecture
```text
Raw Loan Data
  -> Risk Scoring Transform
  -> Loan Risk Dataset
  -> Gold Credit Risk Report
```

## Key Data Engineering Concepts
- Banking risk data modeling
- Risk-band assignment
- Portfolio exposure aggregation
- Gold reporting mart creation

## How to Run
```bash
python src/run_pipeline.py
```

## Resume Bullet
Built a banking credit risk reporting platform that transforms raw loan data, assigns portfolio risk bands using credit score and delinquency features, and produces Gold-level exposure metrics by loan type and risk band.
