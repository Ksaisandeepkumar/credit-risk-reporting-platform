import random
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
INPUT = BASE / "data" / "input"
OUTPUT = BASE / "data" / "output"
INPUT.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)


def main():
    loans = pd.DataFrame([
        {
            "loan_id": f"LN{i:07d}",
            "customer_id": f"CUST{random.randint(10000,99999)}",
            "loan_amount": round(random.uniform(5000, 500000), 2),
            "credit_score": random.randint(520, 820),
            "days_past_due": random.choice([0, 0, 0, 15, 30, 60, 90]),
            "loan_type": random.choice(["AUTO", "MORTGAGE", "PERSONAL", "CREDIT_CARD"]),
        }
        for i in range(1, 2001)
    ])
    loans.to_csv(INPUT / "loans_raw.csv", index=False)

    def risk_band(row):
        if row["days_past_due"] >= 60 or row["credit_score"] < 600:
            return "HIGH"
        if row["days_past_due"] >= 30 or row["credit_score"] < 680:
            return "MEDIUM"
        return "LOW"

    loans["risk_band"] = loans.apply(risk_band, axis=1)
    loans.to_csv(OUTPUT / "silver_loans_risk_scored.csv", index=False)

    report = loans.groupby(["loan_type", "risk_band"], as_index=False).agg(
        loan_count=("loan_id", "count"),
        total_exposure=("loan_amount", "sum"),
        avg_credit_score=("credit_score", "mean"),
        avg_days_past_due=("days_past_due", "mean"),
    )
    report.to_csv(OUTPUT / "gold_credit_risk_report.csv", index=False)

    print("Credit risk reporting platform completed")
    print(f"Report: {OUTPUT / 'gold_credit_risk_report.csv'}")


if __name__ == "__main__":
    main()
