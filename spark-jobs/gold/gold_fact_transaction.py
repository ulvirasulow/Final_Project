import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.spark_utils import (
    create_spark_session,
    gold_dimension_path,
    gold_fact_path,
    silver_path,
)


ALLOWED_CURRENCIES = ["AZN", "USD", "EUR"]

REQUIRED_TXN_COLUMNS = ["transaction_id", "transaction_ref", "account_id", "date_key",
                         "transaction_timestamp", "amount", "currency", "transaction_type", "channel"]
REQUIRED_ACCOUNT_COLUMNS = ["account_sk", "account_id", "customer_id", "branch_id"]
REQUIRED_DATE_COLUMNS = ["date_sk", "date_key", "full_date"]
REQUIRED_BRANCH_COLUMNS = ["branch_sk", "branch_id"]
REQUIRED_CUSTOMER_COLUMNS = ["customer_sk", "customer_id", "effective_from", "effective_to"]


def read_silver_transactions(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(silver_path("transactions"))
    validate_required_columns(df, "silver/transactions", REQUIRED_TXN_COLUMNS)
    return df


def read_gold_dimension(spark: SparkSession, dimension_name: str, required_columns: list) -> DataFrame:
    df = spark.read.parquet(gold_dimension_path(dimension_name))
    validate_required_columns(df, dimension_name, required_columns)
    return df


def validate_required_columns(df: DataFrame, dataset_name: str, required_columns: list) -> None:
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise AssertionError(f"[{dataset_name}] teleb olunan sutunlar catismir: {missing}")


def lookup_account_dimension(txn_df: DataFrame, dim_account_df: DataFrame, expected_count: int) -> DataFrame:
    account_sel = dim_account_df.select("account_sk", "account_id", "customer_id", "branch_id")
    joined = txn_df.join(account_sel, on="account_id", how="left")

    unresolved = joined.filter(F.col("account_sk").isNull()).count()
    if unresolved != 0:
        raise AssertionError(f"[account lookup] {unresolved} emeliyyat account_sk-ni teyin ede bilmedi")

    row_count = joined.count()
    if row_count != expected_count:
        raise AssertionError(
            f"[account lookup] setir sayi gozlenilmeden deyisdi: {expected_count} gozlenilirdi, {row_count} tapildi"
        )
    print(f"  [OK] butun {row_count} emeliyyat ucun account lookup ugurla teyin edildi")
    return joined


def lookup_date_dimension(joined_df: DataFrame, dim_date_df: DataFrame, expected_count: int) -> DataFrame:
    date_sel = dim_date_df.select("date_sk", "date_key", "full_date")
    joined = joined_df.join(date_sel, on="date_key", how="left")

    unresolved = joined.filter(F.col("date_sk").isNull()).count()
    if unresolved != 0:
        raise AssertionError(f"[date lookup] {unresolved} emeliyyat date_sk-ni teyin ede bilmedi")

    row_count = joined.count()
    if row_count != expected_count:
        raise AssertionError(
            f"[date lookup] setir sayi gozlenilmeden deyisdi: {expected_count} gozlenilirdi, {row_count} tapildi"
        )
    print(f"  [OK] butun {row_count} emeliyyat ucun date lookup ugurla teyin edildi")
    return joined


def lookup_branch_dimension(joined_df: DataFrame, dim_branch_df: DataFrame, expected_count: int) -> DataFrame:
    branch_sel = dim_branch_df.select("branch_sk", "branch_id")
    joined = joined_df.join(branch_sel, on="branch_id", how="left")

    unresolved = joined.filter(F.col("branch_sk").isNull()).count()
    if unresolved != 0:
        raise AssertionError(f"[branch lookup] {unresolved} emeliyyat branch_sk-ni teyin ede bilmedi")

    row_count = joined.count()
    if row_count != expected_count:
        raise AssertionError(
            f"[branch lookup] setir sayi gozlenilmeden deyisdi: {expected_count} gozlenilirdi, {row_count} tapildi"
        )
    print(f"  [OK] butun {row_count} emeliyyat ucun branch lookup ugurla teyin edildi")
    return joined


def lookup_customer_scd2(joined_df: DataFrame, dim_customer_df: DataFrame, expected_count: int) -> DataFrame:
    customer_sel = dim_customer_df.select("customer_sk", "customer_id", "effective_from", "effective_to")

    candidates = joined_df.join(customer_sel, on="customer_id", how="left")

    matched = candidates.filter(
        (F.col("full_date") >= F.col("effective_from"))
        & (F.col("effective_to").isNull() | (F.col("full_date") <= F.col("effective_to")))
    )

    matched_count = matched.count()
    if matched_count != expected_count:
        raise AssertionError(
            f"[customer SCD2 lookup] her emeliyyat ucun deqiq bir musteri versiyasi gozlenilirdi ({expected_count} emeliyyat), lakin {matched_count} uygun sətir tapildi — ya bezi emeliyyatlar ucun hec bir uygunluq tapilmayib, ya da birden cox tapilib"
        )

    distinct_txn_count = matched.select("transaction_id").distinct().count()
    if distinct_txn_count != expected_count:
        raise AssertionError(
            f"[customer SCD2 lookup] {expected_count - distinct_txn_count} transaction(s) "
            f"emeliyyat hec bir musteri versiyasi ile uygunlasmadi"
        )

    print(f"  [OK] butun {matched_count} emeliyyat ucun customer SCD Type 2 lookup ile ugurla uygunlasdi")
    return matched


def build_fact_transaction(matched_df: DataFrame) -> DataFrame:
    return (
        matched_df.select(
            "transaction_id", "transaction_ref",
            "account_sk", "customer_sk", "branch_sk", "date_sk",
            "transaction_timestamp", "amount", "currency", "transaction_type", "channel",
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )


def write_fact_transaction(df: DataFrame) -> None:
    df.write.mode("overwrite").parquet(gold_fact_path("fact_transaction"))


def validate_fact_transaction(spark: SparkSession, expected_count: int) -> None:
    fact_df = spark.read.parquet(gold_fact_path("fact_transaction"))
    dim_account = spark.read.parquet(gold_dimension_path("dim_account"))
    dim_branch = spark.read.parquet(gold_dimension_path("dim_branch"))
    dim_date = spark.read.parquet(gold_dimension_path("dim_date"))
    dim_customer = spark.read.parquet(gold_dimension_path("dim_customer"))

    row_count = fact_df.count()

    if row_count != expected_count:
        raise AssertionError(f"[fact_transaction] setir sayi uygunsuzlugu: {expected_count} gozlenilirdi, {row_count} tapildi")

    if fact_df.select("transaction_id").distinct().count() != row_count:
        raise AssertionError("[fact_transaction] transaction_id unique deyil")

    for sk_col in ("account_sk", "date_sk", "branch_sk", "customer_sk"):
        null_count = fact_df.filter(F.col(sk_col).isNull()).count()
        if null_count != 0:
            raise AssertionError(f"[fact_transaction] {null_count} setrin {sk_col} deyeri NULL-dur")

    if fact_df.join(dim_account.select("account_sk"), "account_sk", "left_anti").count() != 0:
        raise AssertionError("[fact_transaction] dim_account terefinde movcud olmayan account_sk tapildi")
    if fact_df.join(dim_branch.select("branch_sk"), "branch_sk", "left_anti").count() != 0:
        raise AssertionError("[fact_transaction] dim_branch terefinde movcud olmayan branch_sk tapildi")
    if fact_df.join(dim_date.select("date_sk"), "date_sk", "left_anti").count() != 0:
        raise AssertionError("[fact_transaction] dim_date terefinde movcud olmayan date_sk tapildi")
    if fact_df.join(dim_customer.select("customer_sk"), "customer_sk", "left_anti").count() != 0:
        raise AssertionError("[fact_transaction] dim_date terefinde movcud olmayan date_sk tapildi")

    scd_check = (
        fact_df
        .join(dim_date.select("date_sk", "full_date"), "date_sk", "inner")
        .join(dim_customer.select("customer_sk", "effective_from", "effective_to"), "customer_sk", "inner")
        .filter(
            ~(
                (F.col("full_date") >= F.col("effective_from"))
                & (F.col("effective_to").isNull() | (F.col("full_date") <= F.col("effective_to")))
            )
        )
        .count()
    )
    if scd_check != 0:
        raise AssertionError(
            f"[fact_transaction] {scd_check} setir emeliyyat tarixinde etibarli olmayan bir customer_sk-ya esaslanir"
        )

    if fact_df.filter(F.col("amount").isNull()).count() != 0:
        raise AssertionError("[fact_transaction] NULL amount tapildi")
    if fact_df.filter(F.col("amount") < 0).count() != 0:
        raise AssertionError("[fact_transaction] menfi amount tapildi")
    if fact_df.filter(~F.col("currency").isin(ALLOWED_CURRENCIES)).count() != 0:
        raise AssertionError("[fact_transaction] icaze verilenlerden kenar valyuta tapildi")

    if fact_df.filter(F.col("gold_processed_at").isNull()).count() != 0:
        raise AssertionError("[fact_transaction] bezi setirler ucun gold_processed_at deyeri catismir")

    print(f"  [OK] fact_transaction tesdiqlendi: {row_count} setir, unique transaction_id, "
          f"butun surrogate key-ler uygunlasdi ve etibarlidir, SCD Type 2 tesdiqlendi, biznes melumatlari temizdir movcuddur")

def main():
    spark = create_spark_session("gold_fact_transaction")

    try:
        print("Silver transaction ve Gold dimension-lar oxunur...")
        txn_df = read_silver_transactions(spark)
        silver_count = txn_df.count()
        print(f"  Silver transactionlarin setir sayi: {silver_count}")

        dim_account_df = read_gold_dimension(spark, "dim_account", REQUIRED_ACCOUNT_COLUMNS)
        dim_date_df = read_gold_dimension(spark, "dim_date", REQUIRED_DATE_COLUMNS)
        dim_branch_df = read_gold_dimension(spark, "dim_branch", REQUIRED_BRANCH_COLUMNS)
        dim_customer_df = read_gold_dimension(spark, "dim_customer", REQUIRED_CUSTOMER_COLUMNS)
        print("  butun 4 Gold dimensions ugurla oxundu")
    except Exception as exc:
        raise RuntimeError(f"Silver emeliyyatlarini ve ya Gold olculerini oxumaq mumkun olmadi: {exc}") from exc

    print("\ndimension surrogate keyleri uygunlasdirilir...")
    try:
        joined = lookup_account_dimension(txn_df, dim_account_df, silver_count)
        joined = lookup_date_dimension(joined, dim_date_df, silver_count)
        joined = lookup_branch_dimension(joined, dim_branch_df, silver_count)
        matched = lookup_customer_scd2(joined, dim_customer_df, silver_count)
    except Exception as exc:
        raise RuntimeError(f"dimension lookup ugursuz oldu: {exc}") from exc

    try:
        fact_df = build_fact_transaction(matched)
        write_fact_transaction(fact_df)
        print(f"\nfact_transaction {gold_fact_path('fact_transaction')} unvanina yazildi")
    except Exception as exc:
        raise RuntimeError(f"fact_transactio-i yazmaq mumkun olmadi: {exc}") from exc

    print("\nfact_transaction tesdiqlenir...")
    try:
        validate_fact_transaction(spark, silver_count)
    except Exception as exc:
        raise RuntimeError(f"fact_transaction validation ugursuz oldu: {exc}") from exc

    print("\nGold fact_transaction qurulmasi Ugurla Kecdi.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nGOLD FACT_TRANSACTION Ugursuz oldu: {exc}", file=sys.stderr)
        sys.exit(1)
