import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.spark_utils import bronze_path, create_spark_session, read_source_table


DATASETS = [
    "branches",
    "dim_date",
    "customers",
    "customer_history",
    "accounts",
    "account_customer_bridge",
    "transactions",
    "account_balance_snapshot",
    "loan_lifecycle",
]

TRANSACTIONS_PARTITION_COLUMN = "transaction_year"


def add_bronze_metadata(df: DataFrame, table_name: str) -> DataFrame:
    return (
        df.withColumn("bronze_ingested_at", F.current_timestamp())
          .withColumn("bronze_source_table", F.lit(table_name))
    )


def add_transaction_partition_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        TRANSACTIONS_PARTITION_COLUMN,
        (F.col("date_key") / F.lit(10000)).cast("int"),
    )


def write_bronze(df: DataFrame, dataset_name: str, partition_col: str = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(bronze_path(dataset_name))


def validate_row_count(spark: SparkSession, dataset_name: str, expected_count: int) -> int:
    bronze_df = spark.read.parquet(bronze_path(dataset_name))
    actual_count = bronze_df.count()
    if actual_count != expected_count:
        raise AssertionError(
            f"[{dataset_name}] Bronze qeydiyyatindan sonra setir sayi uygunsuzlugu: gozlenilen: {expected_count}, tapilan: {actual_count}"
        )
    return actual_count


def validate_columns_present(dataset_name: str, source_df: DataFrame, bronze_df: DataFrame) -> None:
    missing = set(source_df.columns) - set(bronze_df.columns)
    if missing:
        raise AssertionError(f"[{dataset_name}] Bronze terefinde catismayan source sutunlari: {missing}")


def validate_transaction_defects(spark: SparkSession) -> None:
    df = spark.read.parquet(bronze_path("transactions"))

    dup_count = df.filter(F.col("transaction_id") == 4).count()
    if dup_count != 2:
        raise AssertionError(f"[transactions] gozlenilen transaction_id=4 iki defe Bronze-da gorunmeli idi, tapilan: {dup_count}")

    null_amount = df.filter((F.col("transaction_id") == 6) & F.col("amount").isNull()).count()
    if null_amount != 1:
        raise AssertionError("[transactions] transaction_id=6 ucun Bronze terefinde amount NULL olmasi gozlenilirdi")

    orphan = df.filter((F.col("transaction_id") == 7) & (F.col("account_id") == 999)).count()
    if orphan != 1:
        raise AssertionError("[transactions] transaction_id=7 ucun Bronze terefinde account_id=999 olmasi gozlenilirdi")

    bad_row = df.filter(
        (F.col("transaction_id") == 8) & (F.col("amount") == -50) & (F.col("currency") == "XXX")
    ).count()
    if bad_row != 1:
        raise AssertionError("[transactions] transaction_id=8 ucun Bronze terefinde amount=-50 ve currency=XXX olmasi gozlenilirdi")

    print("  [OK] butun 5 qesden edilmis defect deyismeden Bronze-a yuklendi")


def ingest_dataset(spark: SparkSession, table_name: str) -> None:
    print(f"\n'{table_name}' yuklenilir...")

    try:
        source_df = read_source_table(spark, table_name)
        source_count = source_df.count()
        print(f" PostgreSQL-de menbe setir sayi: {source_count}")
    except Exception as exc:
        raise RuntimeError(f"[{table_name}] PostgreSQL-den oxumaq mumkun olmadi: {exc}") from exc

    try:
        bronze_df = add_bronze_metadata(source_df, table_name)
        partition_col = None
        if table_name == "transactions":
            bronze_df = add_transaction_partition_column(bronze_df)
            partition_col = TRANSACTIONS_PARTITION_COLUMN

        bronze_count_before_write = bronze_df.count()
        if bronze_count_before_write != source_count:
            raise AssertionError(
                f"setir sayi Bronze metadata elave olunarken deyisdi: "
                f"source={source_count}, bronze={bronze_count_before_write}"
            )
    except Exception as exc:
        raise RuntimeError(f"[{table_name}] Bronze DataFrame hazirlanarken xeta bas verdi: {exc}") from exc

    try:
        write_bronze(bronze_df, table_name, partition_col=partition_col)
        print(f" Parquet {bronze_path(table_name)} unvanina yazildi")
    except Exception as exc:
        raise RuntimeError(f"[{table_name}] Bronze Parquet faylini MinIO-a yazmaq mumkun olmadi: {exc}") from exc

    try:
        validate_row_count(spark, table_name, source_count)
        validate_columns_present(table_name, source_df, bronze_df)
        print(f"  [OK] setir sayi ve sutunlar tesdiqlendi ({source_count} setir)")

        if table_name == "transactions":
            validate_transaction_defects(spark)
    except Exception as exc:
        raise RuntimeError(f"[{table_name}] Bronze validasiyasi ugursuz oldu: {exc}") from exc

def main():
    spark = create_spark_session("bronze_ingestion")
    print(f"{len(DATASETS)} datasenti Bronze yuklenmesine basladilir: {DATASETS}")

    for table_name in DATASETS:
        ingest_dataset(spark, table_name)

    print("\nBronze-a yuklenme ugurlu oldu — bütün doqquz dataset MinIO-ya Parquet formatinda yuklendi.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nBronze-a yuklenme ugursuz oldu: {exc}", file=sys.stderr)
        sys.exit(1)
