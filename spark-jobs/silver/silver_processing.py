from common.spark_utils import (
    bronze_path,
    create_spark_session,
    quarantine_path,
    silver_path,
)
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


STANDARD_DATASETS = [
    "branches",
    "dim_date",
    "customers",
    "customer_history",
    "accounts",
    "account_customer_bridge",
    "account_balance_snapshot",
    "loan_lifecycle",
]

REQUIRED_COLUMNS = {
    "branches": ["branch_id", "branch_name", "branch_code", "city", "region", "country", "opening_date"],
    "dim_date": ["date_key", "full_date", "day", "month", "month_name", "quarter", "year",
                 "day_of_week", "day_name", "is_weekend"],
    "customers": ["customer_id", "first_name", "last_name", "date_of_birth", "email", "phone_number",
                  "address_country", "customer_segment", "credit_rating_current",
                  "credit_rating_previous", "change_hash"],
    "customer_history": ["customer_id", "effective_from", "effective_to", "is_current", "first_name",
                         "last_name", "email", "phone_number", "customer_segment", "credit_rating",
                         "address_country"],
    "accounts": ["account_id", "customer_id", "branch_id", "account_type", "account_status",
                 "currency", "open_date"],
    "account_customer_bridge": ["account_id", "customer_id", "allocation_weight", "is_primary_owner"],
    "account_balance_snapshot": ["account_id", "snapshot_date_key", "closing_balance", "currency"],
    "loan_lifecycle": ["loan_id", "account_id", "loan_amount", "loan_status", "application_date",
                       "approval_date", "disbursement_date", "closure_date"],
    "transactions": ["transaction_id", "transaction_ref", "account_id", "date_key",
                     "transaction_timestamp", "amount", "currency", "transaction_type", "channel"],
}

BRONZE_TECHNICAL_COLUMNS = ["load_run_id", "source_system", "processed_at",
                            "bronze_ingested_at", "bronze_source_table"]

ALLOWED_CURRENCIES = ["AZN", "USD", "EUR"]
TRANSACTIONS_PARTITION_COLUMN = "transaction_year"

REASON_DUPLICATE = "DUPLICATE_TRANSACTION_ID"
REASON_NULL_AMOUNT = "NULL_AMOUNT"
REASON_ORPHAN_ACCOUNT = "ORPHAN_ACCOUNT"
REASON_NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"
REASON_INVALID_CURRENCY = "INVALID_CURRENCY"
REASON_SEPARATOR = "|"


def read_bronze_dataset(spark: SparkSession, dataset_name: str) -> DataFrame:
    return spark.read.parquet(bronze_path(dataset_name))


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("silver_processed_at", F.current_timestamp())


def validate_required_columns(df: DataFrame, dataset_name: str, required_columns: list) -> None:
    expected = set(required_columns) | set(BRONZE_TECHNICAL_COLUMNS)
    missing = expected - set(df.columns)
    if missing:
        raise AssertionError(
            f"[{dataset_name}] Bronze terefinde teleb olunan sutunlar catismir: {missing}")


def process_standard_dataset(spark: SparkSession, dataset_name: str) -> DataFrame:

    print(f"\n'{dataset_name}' emal edilir...")

    try:
        bronze_df = read_bronze_dataset(spark, dataset_name)
        bronze_count = bronze_df.count()
        validate_required_columns(
            bronze_df, dataset_name, REQUIRED_COLUMNS[dataset_name])
        print(f"  Bronze setir sayi: {bronze_count}")
    except Exception as exc:
        raise RuntimeError(
            f"[{dataset_name}] Bronze-u oxumaq ve ya yoxlamaq mumkun olmadi: {exc}") from exc

    try:
        silver_df = add_silver_metadata(bronze_df)
        silver_df.write.mode("overwrite").parquet(silver_path(dataset_name))
        print(f" {silver_path(dataset_name)} Silvere yazildi")
    except Exception as exc:
        raise RuntimeError(
            f"[{dataset_name}] Silver-i yazmaq mumkun olmadi: {exc}") from exc

    try:
        written_df = spark.read.parquet(silver_path(dataset_name))
        silver_count = written_df.count()
        if silver_count != bronze_count:
            raise AssertionError(
                f"setir sayi uygun deyil: bronze={bronze_count}, silver={silver_count}"
            )
        missing_cols = set(bronze_df.columns) - set(written_df.columns)
        if missing_cols:
            raise AssertionError(
                f"Silver terefde Bronze sutunlari catismir: {missing_cols}")
        print(
            f"  [OK] Silver setir ve sutun saylari duzgundur ({silver_count} setir)")
    except Exception as exc:
        raise RuntimeError(
            f"[{dataset_name}] Silver validation ugursuz oldu: {exc}") from exc

    return written_df


def mark_duplicates(df: DataFrame) -> DataFrame:

    window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").asc(), F.col("processed_at").asc()
    )
    return df.withColumn("_dup_rank", F.row_number().over(window))


def build_data_quality_reason(df: DataFrame, valid_account_ids: list) -> DataFrame:
    df = (
        df
        .withColumn("_reason_duplicate",
                    F.when(F.col("_dup_rank") > 1, F.lit(REASON_DUPLICATE)))
        .withColumn("_reason_null_amount",
                    F.when(F.col("amount").isNull(), F.lit(REASON_NULL_AMOUNT)))
        .withColumn("_reason_orphan_account",
                    F.when(~F.col("account_id").isin(valid_account_ids), F.lit(REASON_ORPHAN_ACCOUNT)))
        .withColumn("_reason_negative_amount",
                    F.when(F.col("amount") < 0, F.lit(REASON_NEGATIVE_AMOUNT)))
        .withColumn("_reason_invalid_currency",
                    F.when(~F.col("currency").isin(ALLOWED_CURRENCIES), F.lit(REASON_INVALID_CURRENCY)))
    )

    reason_columns = [
        "_reason_duplicate", "_reason_null_amount", "_reason_orphan_account",
        "_reason_negative_amount", "_reason_invalid_currency",
    ]
    non_null_reasons = F.filter(
        F.array(*reason_columns), lambda x: x.isNotNull())
    df = df.withColumn("data_quality_reason", F.concat_ws(
        REASON_SEPARATOR, non_null_reasons))

    return df.drop(*reason_columns)


def process_transactions(spark: SparkSession, valid_account_ids: list) -> None:
    print("\n'transactions' emal edilir...")

    try:
        bronze_df = read_bronze_dataset(spark, "transactions")
        bronze_count = bronze_df.count()
        validate_required_columns(
            bronze_df, "transactions", REQUIRED_COLUMNS["transactions"])
        print(f"  Bronze setir sayi: {bronze_count}")
    except Exception as exc:
        raise RuntimeError(
            f"[transactions] Bronze-u oxumaq ve ya yoxlamaq mumkun olmadi: {exc}") from exc

    try:
        evaluated_df = mark_duplicates(bronze_df)
        evaluated_df = build_data_quality_reason(
            evaluated_df, valid_account_ids)
        evaluated_df = evaluated_df.drop("_dup_rank").cache()

        valid_df = (
            evaluated_df
            .filter(F.col("data_quality_reason") == "")
            .drop("data_quality_reason")
            .transform(add_silver_metadata)
        )
        quarantined_df = (
            evaluated_df
            .filter(F.col("data_quality_reason") != "")
            .withColumn("quarantined_at", F.current_timestamp())
        )

        valid_count = valid_df.count()
        quarantine_count = quarantined_df.count()
        print(
            f"  etibarli setirler: {valid_count}, karantine alinmis setirler: {quarantine_count}")

        if valid_count + quarantine_count != bronze_count:
            raise AssertionError(
                f"yazilmadan evvel setir balansi (sayi) ugursuz oldu: bronze={bronze_count}, "
                f"valid={valid_count}, quarantine={quarantine_count}"
            )
    except Exception as exc:
        raise RuntimeError(
            f"[transactions] setirlerin yoxlanmasi zamani ugursuz oldu: {exc}") from exc

    try:
        valid_df.write.mode("overwrite").partitionBy(TRANSACTIONS_PARTITION_COLUMN).parquet(
            silver_path("transactions")
        )
        print(f"Silver emeliyyatlari {silver_path('transactions')} unvanina yazildi")

        quarantined_df.write.mode("overwrite").parquet(
            quarantine_path("transactions"))
        print(
            f"karantine alinmis emeliyyatlar {quarantine_path('transactions')} unvanina yazildi")
    except Exception as exc:
        raise RuntimeError(
            f"[transactions] Silver - karantin cixisini yazmaq mumkun olmadi: {exc}") from exc

    validate_transactions_output(spark, bronze_count, valid_account_ids)


def validate_transactions_output(spark: SparkSession, bronze_count: int, valid_account_ids: list) -> None:
    silver_df = spark.read.parquet(silver_path("transactions"))
    quarantine_df = spark.read.parquet(quarantine_path("transactions"))

    silver_count = silver_df.count()
    quarantine_count = quarantine_df.count()

    if bronze_count != silver_count + quarantine_count:
        raise AssertionError(
            f"[transactions] yazilmadan sonra setir balansi (sayi) ugursuz oldu: "
            f"bronze={bronze_count}, silver={silver_count}, quarantine={quarantine_count}"
        )
    print(
        f"  [OK] setir balansi (sayi): bronze({bronze_count}) = silver({silver_count}) + quarantine({quarantine_count})")

    distinct_ids = silver_df.select("transaction_id").distinct().count()
    if distinct_ids != silver_count:
        raise AssertionError(
            f"[transactions] Silver terefinde duplicate transaction_id deyerleri var")

    id4_count = silver_df.filter(F.col("transaction_id") == 4).count()
    if id4_count != 1:
        raise AssertionError(
            f"[transactions] transaction_id=4 ucun Silver terefinde deqiq bir defe gorunməsi gözlenilirdi, {id4_count} tapildi")

    for bad_id in (6, 7, 8):
        if silver_df.filter(F.col("transaction_id") == bad_id).count() != 0:
            raise AssertionError(
                f"[transactions] transaction_id={bad_id} Silver terefinde gorunmemelidir")

    if silver_df.filter(F.col("amount").isNull()).count() != 0:
        raise AssertionError("[transactions] Silver terefinde NULL amount var")
    if silver_df.filter(F.col("amount") < 0).count() != 0:
        raise AssertionError(
            "[transactions] Silver terefinde menfi amount var")
    if silver_df.filter(~F.col("currency").isin(ALLOWED_CURRENCIES)).count() != 0:
        raise AssertionError(
            "[transactions] Silver terefinde icaze verilenden kenar valyuta var")
    if silver_df.filter(~F.col("account_id").isin(valid_account_ids)).count() != 0:
        raise AssertionError(
            "[transactions] Silver terefinde Silver accounts daxilinde movcud olmayan account_id var")
    print(f"  [OK] Silver emeliyyatlari temizdir ({silver_count} setir)")

    reasons_by_id = {
        row["transaction_id"]: row["reasons"]
        for row in (
            quarantine_df.groupBy("transaction_id")
            .agg(F.concat_ws(",", F.collect_list("data_quality_reason")).alias("reasons"))
            .collect()
        )
    }

    dup_copies = quarantine_df.filter(F.col("transaction_id") == 4).count()
    if dup_copies != 1:
        raise AssertionError(
            f"[transactions] expected exactly 1 quarantined copy of transaction_id=4, found {dup_copies}")
    if REASON_DUPLICATE not in reasons_by_id.get(4, ""):
        raise AssertionError(
            "[transactions] transaction_id=4 karantin sebebi DUPLICATE_TRANSACTION_ID ifadesini cemlesdirib")

    if REASON_NULL_AMOUNT not in reasons_by_id.get(6, ""):
        raise AssertionError(
            "[transactions] transaction_id=6 karantin sebebi NULL_AMOUNT ifadesini cemlesdirib")

    if REASON_ORPHAN_ACCOUNT not in reasons_by_id.get(7, ""):
        raise AssertionError(
            "[transactions] transaction_id=7 karantin sebebi ORPHAN_ACCOUNT ifadesini cemlesdirib")

    reasons_8 = reasons_by_id.get(8, "")
    if REASON_NEGATIVE_AMOUNT not in reasons_8 or REASON_INVALID_CURRENCY not in reasons_8:
        raise AssertionError(
            f"[transactions] transaction_id=8 hem NEGATIVE_AMOUNT, hem de INVALID_CURRENCY ifadelerini cemlesdirmelidir, {reasons_8} tapildi"
        )
    print("  [OK] karantinde gozlenilen qeydler movcuddur "
          "(id=4 duplicate, id=6 NULL_AMOUNT, id=7 ORPHAN_ACCOUNT, id=8 her iki sebeb)")


def main():
    spark = create_spark_session("silver_processing")

    silver_accounts_df = None
    for dataset_name in STANDARD_DATASETS:
        written_df = process_standard_dataset(spark, dataset_name)
        if dataset_name == "accounts":
            silver_accounts_df = written_df

    if silver_accounts_df is None:
        raise RuntimeError(
            "Silver accounts datasetleri hazirlanmadi — emeliyyatlari dogrulamaq mumkun deyil")

    valid_account_ids = [
        row["account_id"] for row in silver_accounts_df.select("account_id").distinct().collect()
    ]
    print(f"\n{len(valid_account_ids)} emeliyyatlarin dogrulanmasi ucun duzgun acount_id-lar movcuddur.")

    process_transactions(spark, valid_account_ids)

    print("\nSilver emali ugurla kecdi — doqquz Silver dataseti yazildi, kecersiz emeliyyatlar karantine alindi.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nSILVER emali ugursuz oldu: {exc}", file=sys.stderr)
        sys.exit(1)
