from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "loans_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_loans_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_loans_risk_scored_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_credit_risk_report_parquet")

schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("loan_type", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CreditRiskReportingPlatform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(INPUT_PATH)
        .withColumn("bronze_loaded_at", current_timestamp())
    )
    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .filter(col("loan_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("loan_amount") > 0)
        .filter(col("credit_score").between(300, 850))
        .filter(col("days_past_due") >= 0)
        .dropDuplicates(["loan_id"])
        .withColumn(
            "risk_band",
            when((col("days_past_due") >= 60) | (col("credit_score") < 600), "HIGH")
            .when((col("days_past_due") >= 30) | (col("credit_score") < 680), "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("silver_loaded_at", current_timestamp())
    )
    silver_df.write.mode("overwrite").parquet(SILVER_PATH)

    gold_df = (
        silver_df
        .groupBy("loan_type", "risk_band")
        .agg(
            count("loan_id").alias("loan_count"),
            spark_sum("loan_amount").alias("total_exposure"),
            avg("credit_score").alias("avg_credit_score"),
            avg("days_past_due").alias("avg_days_past_due"),
        )
    )
    gold_df.write.mode("overwrite").parquet(GOLD_PATH)

    print("PySpark credit risk reporting pipeline completed")
    print(f"Bronze records: {bronze_df.count()}")
    print(f"Silver records: {silver_df.count()}")
    print(f"Gold records: {gold_df.count()}")
    gold_df.show(truncate=False)


if __name__ == "__main__":
    main()
