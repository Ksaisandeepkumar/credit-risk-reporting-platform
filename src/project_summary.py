from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "loans_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_loans_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_loans_risk_scored_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_credit_risk_report_parquet")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CreditRiskProjectSummary")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_safe(spark, path, label, file_type="parquet"):
    try:
        if file_type == "csv":
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        else:
            df = spark.read.parquet(path)
        print(f"\n{label} loaded successfully")
        return df
    except Exception as exc:
        print(f"\n{label} not available yet: {exc}")
        return None


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_safe(spark, INPUT_PATH, "Raw loans CSV", "csv")
    bronze_df = read_safe(spark, BRONZE_PATH, "Bronze loans")
    silver_df = read_safe(spark, SILVER_PATH, "Silver risk-scored loans")
    gold_df = read_safe(spark, GOLD_PATH, "Gold credit risk report")

    if raw_df is not None:
        print("\n===== RAW LOAN SUMMARY =====")
        print("Raw record count:", raw_df.count())

        print("\nDuplicate loan_id proof in raw data:")
        raw_df.groupBy("loan_id").agg(count("*").alias("record_count")) \
            .filter(col("loan_id").isNotNull()) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid raw loan records:")
        raw_df.filter(
            col("loan_id").isNull()
            | col("customer_id").isNull()
            | (col("loan_amount") <= 0)
            | (col("credit_score") < 300)
            | (col("credit_score") > 850)
            | (col("days_past_due") < 0)
        ).show(truncate=False)

    if bronze_df is not None:
        print("\n===== BRONZE SUMMARY =====")
        print("Bronze count:", bronze_df.count())

    if silver_df is not None:
        print("\n===== SILVER SUMMARY =====")
        print("Silver count:", silver_df.count())
        print("Unique loan_id count:", silver_df.select("loan_id").distinct().count())

        print("\nDuplicate proof after Silver deduplication:")
        silver_df.groupBy("loan_id").agg(count("*").alias("record_count")) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid record proof after Silver validation:")
        silver_df.filter(
            col("loan_id").isNull()
            | col("customer_id").isNull()
            | (col("loan_amount") <= 0)
            | (col("credit_score") < 300)
            | (col("credit_score") > 850)
            | (col("days_past_due") < 0)
        ).show(truncate=False)

        print("\nRisk band distribution:")
        silver_df.groupBy("risk_band").agg(count("*").alias("loan_count")).show(truncate=False)

    if gold_df is not None:
        print("\n===== GOLD SUMMARY =====")
        print("Gold count:", gold_df.count())
        print("\nSample Gold output:")
        gold_df.show(20, truncate=False)


if __name__ == "__main__":
    main()
