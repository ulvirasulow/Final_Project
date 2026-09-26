import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark-jobs")

from common.spark_utils import (
    create_spark_session,
    gold_dimension_path,
    gold_fact_path,
    silver_path,
)


def fail(message: str):
    print(f"VALIDATE_GOLD ugursuz oldu: {message}", file=sys.stderr)
    sys.exit(1)


def main():
    spark = create_spark_session("validate_gold")

    try:
        fact = spark.read.parquet(gold_fact_path("fact_transaction"))
        silver_txn = spark.read.parquet(silver_path("transactions"))
        dim_account = spark.read.parquet(gold_dimension_path("dim_account"))
        dim_date = spark.read.parquet(gold_dimension_path("dim_date"))
        dim_customer = spark.read.parquet(gold_dimension_path("dim_customer"))
    except Exception as exc:
        fail(f"teleb olunan Gold dimension ve ya fact_transaction oxuna bilmedi: {exc}")

    fact_count = fact.count()
    distinct_txn = fact.select("transaction_id").distinct().count()
    if distinct_txn != fact_count:
        fail(f"transaction_id unique deyil: {fact_count} rows, {distinct_txn} distinct")

    silver_count = silver_txn.count()
    if fact_count != silver_count:
        fail(f"fact_transaction sayi ({fact_count}) Silver valid transaction sayina ({silver_count}) beraber deyil")

    for sk_col in ("account_sk", "date_sk", "branch_sk", "customer_sk"):
        null_count = fact.filter(F.col(sk_col).isNull()).count()
        if null_count != 0:
            fail(f"{null_count} setirde {sk_col} sutunu NULL-dir")

    fact_base = fact.select(
        F.col("transaction_id"),
        F.col("account_sk"),
        F.col("customer_sk").alias("fact_customer_sk"),
        F.col("date_sk"),
    )
    with_customer = (
        fact_base
        .join(dim_account.select("account_sk", "customer_id"), "account_sk")
        .join(dim_date.select("date_sk", "full_date"), "date_sk")
    )
    candidates = with_customer.join(
        dim_customer.select("customer_id", "customer_sk", "effective_from", "effective_to"),
        "customer_id",
    )
    matched = candidates.filter(
        (F.col("full_date") >= F.col("effective_from"))
        & (F.col("effective_to").isNull() | (F.col("full_date") <= F.col("effective_to")))
    )

    matched_distinct_txn = matched.select("transaction_id").distinct().count()
    unresolved = fact_count - matched_distinct_txn
    if unresolved != 0:
        fail(f"{unresolved} fact setri hec bir SCD Type 2 musteri versiyasi ile uygunlasmadi")

    dup_matches = matched.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
    if dup_matches != 0:
        fail(f"{dup_matches} fact setri birden cox musteri versiyasi ile uygunlasdi")

    sk_mismatch = matched.filter(F.col("customer_sk") != F.col("fact_customer_sk")).count()
    if sk_mismatch != 0:
        fail(f"{sk_mismatch} fact sətri yeniden elde edilmis customer_sk-dan ferqli bir customer_sk ile uygunlasdi")

    print(f"validate_gold ugurla kecdi: {fact_count} fact_transaction setri, "
          f"unique transaction_id, NULL surrogate key yoxdu, "
          f"her bir setir deqiq bir SCD Type 2 musteri versiyasi ile uygunlasir")
    spark.stop()


if __name__ == "__main__":
    main()
