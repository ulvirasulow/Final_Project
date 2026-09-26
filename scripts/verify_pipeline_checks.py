import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark-jobs")

from common.spark_utils import (
    bronze_path,
    create_spark_session,
    gold_dimension_path,
    gold_fact_path,
    quarantine_path,
    silver_path,
)


def main():
    spark = create_spark_session("verify_pipeline_checks")
    results = {}

    bronze_txn = spark.read.parquet(bronze_path("transactions"))
    results["BRONZE_TXN_COUNT"] = bronze_txn.count()

    silver_txn = spark.read.parquet(silver_path("transactions"))
    quarantine_txn = spark.read.parquet(quarantine_path("transactions"))
    results["SILVER_VALID_COUNT"] = silver_txn.count()
    results["QUARANTINE_COUNT"] = quarantine_txn.count()
    results["SILVER_TXN4_COUNT"] = silver_txn.filter(F.col("transaction_id") == 4).count()
    for bad_id in (6, 7, 8):
        results[f"SILVER_TXN{bad_id}_COUNT"] = silver_txn.filter(F.col("transaction_id") == bad_id).count()

    dim_customer = spark.read.parquet(gold_dimension_path("dim_customer"))
    dim_account = spark.read.parquet(gold_dimension_path("dim_account"))
    dim_branch = spark.read.parquet(gold_dimension_path("dim_branch"))
    dim_date = spark.read.parquet(gold_dimension_path("dim_date"))

    results["DIM_CUSTOMER_COUNT"] = dim_customer.count()
    results["DIM_ACCOUNT_COUNT"] = dim_account.count()
    results["DIM_BRANCH_COUNT"] = dim_branch.count()
    results["DIM_DATE_COUNT"] = dim_date.count()

    current_counts = dim_customer.groupBy("customer_id").agg(
        F.sum(F.col("is_current").cast("int")).alias("current_count")
    )
    results["DIM_CUSTOMER_BAD_CURRENT_COUNT"] = current_counts.filter(F.col("current_count") != 1).count()

    fact = spark.read.parquet(gold_fact_path("fact_transaction"))
    fact_count = fact.count()
    results["FACT_COUNT"] = fact_count
    results["FACT_DISTINCT_TXN_ID"] = fact.select("transaction_id").distinct().count()
    results["FACT_ACCOUNT_SK_NULLS"] = fact.filter(F.col("account_sk").isNull()).count()
    results["FACT_DATE_SK_NULLS"] = fact.filter(F.col("date_sk").isNull()).count()
    results["FACT_BRANCH_SK_NULLS"] = fact.filter(F.col("branch_sk").isNull()).count()
    results["FACT_CUSTOMER_SK_NULLS"] = fact.filter(F.col("customer_sk").isNull()).count()

    account_customer = dim_account.select("account_sk", "customer_id")
    date_lookup = dim_date.select("date_sk", "full_date")
    customer_periods = dim_customer.select(
        "customer_id", "effective_from", "effective_to",
        F.col("customer_sk").alias("expected_customer_sk"),
    )

    candidates = (
        fact.select("transaction_id", "account_sk", "date_sk", "customer_sk")
        .join(account_customer, "account_sk", "left")
        .join(date_lookup, "date_sk", "left")
        .join(customer_periods, "customer_id", "left")
        .filter(
            (F.col("full_date") >= F.col("effective_from"))
            & (F.col("effective_to").isNull() | (F.col("full_date") <= F.col("effective_to")))
        )
    )

    match_counts = candidates.groupBy("transaction_id").count()
    results["SCD2_UNRESOLVED_COUNT"] = fact_count - match_counts.count()
    results["SCD2_DUPLICATE_MATCH_COUNT"] = match_counts.filter(F.col("count") > 1).count()
    results["SCD2_SK_MISMATCH_COUNT"] = candidates.filter(
        F.col("customer_sk") != F.col("expected_customer_sk")
    ).count()

    spark.stop()

    print("===VERIFY_CHECKS_START===")
    for key, value in results.items():
        print(f"{key}={value}")
    print("===VERIFY_CHECKS_END===")


if __name__ == "__main__":
    main()
