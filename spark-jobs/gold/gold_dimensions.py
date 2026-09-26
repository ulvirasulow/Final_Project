from common.spark_utils import create_spark_session, gold_dimension_path, silver_path
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


REQUIRED_COLUMNS = {
    "customer_history": ["customer_id", "effective_from", "effective_to", "is_current",
                         "first_name", "last_name", "email", "phone_number",
                         "customer_segment", "credit_rating", "address_country"],
    "accounts": ["account_id", "customer_id", "branch_id", "account_type",
                 "account_status", "currency", "open_date"],
    "branches": ["branch_id", "branch_name", "branch_code", "city", "region", "country", "opening_date"],
    "dim_date": ["date_key", "full_date", "day", "month", "month_name", "quarter", "year",
                 "day_of_week", "day_name", "is_weekend"],
}


def read_silver_dataset(spark: SparkSession, dataset_name: str) -> DataFrame:
    return spark.read.parquet(silver_path(dataset_name))


def validate_required_columns(df: DataFrame, dataset_name: str, required_columns: list) -> None:
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise AssertionError(
            f"[{dataset_name}] Silver terefinde teleb olunan sutunlar catismir: {missing}")


def add_gold_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("gold_processed_at", F.current_timestamp())


def generate_surrogate_key(df: DataFrame, order_columns: list, sk_column_name: str) -> DataFrame:
    window = Window.orderBy(*order_columns)
    return df.withColumn(sk_column_name, F.row_number().over(window).cast("long"))


def write_gold_dimension(df: DataFrame, dimension_name: str) -> None:
    df.write.mode("overwrite").parquet(gold_dimension_path(dimension_name))


def build_dim_customer(spark: SparkSession) -> tuple:
    print("\n'dim_customer' (SCD Type 2) yaradilir...")

    history_df = read_silver_dataset(spark, "customer_history")
    source_count = history_df.count()
    validate_required_columns(
        history_df, "customer_history", REQUIRED_COLUMNS["customer_history"])
    print(f"  Silver customer_history setir sayi: {source_count}")

    customer_window = Window.partitionBy(
        "customer_id").orderBy("effective_from")

    df = history_df.withColumn("_next_effective_from", F.lead(
        "effective_from").over(customer_window))
    df = df.withColumn(
        "effective_to",
        F.when(F.col("_next_effective_from").isNotNull(),
               F.date_sub(F.col("_next_effective_from"), 1))
        .otherwise(F.lit(None).cast("date")),
    )
    df = df.withColumn("is_current", F.col("_next_effective_from").isNull())
    df = df.drop("_next_effective_from")

    df = generate_surrogate_key(
        df, ["customer_id", "effective_from"], "customer_sk")

    df = add_gold_metadata(df)

    dim_customer_df = df.select(
        "customer_sk", "customer_id", "effective_from", "effective_to", "is_current",
        "first_name", "last_name", "email", "phone_number",
        "customer_segment", "credit_rating", "address_country",
        "gold_processed_at",
    )
    return dim_customer_df, source_count


def validate_dim_customer(spark: SparkSession, source_count: int) -> None:
    df = spark.read.parquet(gold_dimension_path("dim_customer"))
    row_count = df.count()

    if row_count != source_count:
        raise AssertionError(
            f"[dim_customer] setir sayi uygunsuzlugu: customer_history={source_count}, dim_customer={row_count}"
        )

    if df.select("customer_sk").distinct().count() != row_count:
        raise AssertionError("[dim_customer] customer_sk unikal deyil")

    dup_keys = df.groupBy("customer_id", "effective_from").count().filter(
        F.col("count") > 1).count()
    if dup_keys != 0:
        raise AssertionError(
            f"[dim_customer] {dup_keys} tekrarlanan (customer_id, effective_from) cutluyy tapildi")

    current_counts = df.groupBy("customer_id").agg(
        F.sum(F.col("is_current").cast("int")).alias("current_count"))
    bad_customers = current_counts.filter(F.col("current_count") != 1).count()
    if bad_customers != 0:
        raise AssertionError(
            f"[dim_customer] {bad_customers} musterinin deqiq bir cari versiyasi yoxdur")

    if df.filter(F.col("is_current") & F.col("effective_to").isNotNull()).count() != 0:
        raise AssertionError(
            "[dim_customer] cari versiyanin effective_to deyeri NULL deyil")

    bad_history = df.filter(
        (~F.col("is_current")) &
        (F.col("effective_to").isNull() |
         (F.col("effective_to") < F.col("effective_from")))
    ).count()
    if bad_history != 0:
        raise AssertionError(
            f"[dim_customer] {bad_history} tarixce versiyasinin tarix araligi kecersizdir")

    window = Window.partitionBy("customer_id").orderBy("effective_from")
    overlap_check = (
        df.withColumn("_prev_effective_to", F.lag("effective_to").over(window))
          .filter(F.col("_prev_effective_to").isNotNull() & (F.col("effective_from") <= F.col("_prev_effective_to")))
          .count()
    )
    if overlap_check != 0:
        raise AssertionError(
            f"[dim_customer] {overlap_check} ust-uste dusen tarixce tapildi")

    print(f"  [OK] dim_customer tesdiqlendi: {row_count} rows, unique customer_sk, "
          f"her musteri ucun deqiq bir cari versiya, ust-uste dusen dovrler yoxdur")


def build_dim_account(spark: SparkSession) -> tuple:
    print("\n'dim_account' yaradilir...")
    accounts_df = read_silver_dataset(spark, "accounts")
    source_count = accounts_df.count()
    validate_required_columns(accounts_df, "accounts",
                              REQUIRED_COLUMNS["accounts"])
    print(f"  Silver accounts setir sayi: {source_count}")

    df = generate_surrogate_key(accounts_df, ["account_id"], "account_sk")
    df = add_gold_metadata(df)

    dim_account_df = df.select(
        "account_sk", "account_id", "customer_id", "branch_id",
        "account_type", "account_status", "currency", "open_date",
        "gold_processed_at",
    )
    return dim_account_df, source_count


def build_dim_branch(spark: SparkSession) -> tuple:
    print("\n'dim_branch' yaradilir...")
    branches_df = read_silver_dataset(spark, "branches")
    source_count = branches_df.count()
    validate_required_columns(branches_df, "branches",
                              REQUIRED_COLUMNS["branches"])
    print(f"  Silver branches setir sayi: {source_count}")

    df = generate_surrogate_key(branches_df, ["branch_id"], "branch_sk")
    df = add_gold_metadata(df)

    dim_branch_df = df.select(
        "branch_sk", "branch_id", "branch_name", "branch_code",
        "city", "region", "country", "opening_date",
        "gold_processed_at",
    )
    return dim_branch_df, source_count


def build_dim_date(spark: SparkSession) -> tuple:
    print("\n'dim_date' yaradilir...")
    date_df = read_silver_dataset(spark, "dim_date")
    source_count = date_df.count()
    validate_required_columns(
        date_df, "dim_date", REQUIRED_COLUMNS["dim_date"])
    print(f"  Silver dim_date setir sayi: {source_count}")

    df = generate_surrogate_key(date_df, ["date_key"], "date_sk")
    df = add_gold_metadata(df)

    dim_date_df = df.select(
        "date_sk", "date_key", "full_date", "day", "month", "month_name",
        "quarter", "year", "day_of_week", "day_name", "is_weekend",
        "gold_processed_at",
    )
    return dim_date_df, source_count


def validate_standard_dimension(spark: SparkSession, dimension_name: str,
                                natural_key_col: str, sk_col: str, source_count: int) -> None:
    df = spark.read.parquet(gold_dimension_path(dimension_name))
    row_count = df.count()

    if row_count != source_count:
        raise AssertionError(
            f"[{dimension_name}] setir sayi uygun deyil: Silver={source_count}, Gold={row_count}"
        )
    if df.filter(F.col(natural_key_col).isNull()).count() != 0:
        raise AssertionError(
            f"[{dimension_name}] {natural_key_col} NULL deyerleri cemlesdirir")
    if df.select(natural_key_col).distinct().count() != row_count:
        raise AssertionError(
            f"[{dimension_name}] {natural_key_col} unikal deyil")
    if df.filter(F.col(sk_col).isNull()).count() != 0:
        raise AssertionError(
            f"[{dimension_name}] {sk_col} NULL deyerleri cemlesdirir")
    if df.select(sk_col).distinct().count() != row_count:
        raise AssertionError(f"[{dimension_name}] {sk_col} unikal deyil")
    if "gold_processed_at" not in df.columns:
        raise AssertionError(f"[{dimension_name}] gold_processed_at catismir")

    print(f"  [OK] {dimension_name} tesdiqlendi: {row_count} setir, "
          f"unikal {natural_key_col} ve {sk_col}")


def check_dimension_relationships(spark: SparkSession) -> None:
    dim_account = spark.read.parquet(gold_dimension_path("dim_account"))
    dim_branch = spark.read.parquet(gold_dimension_path("dim_branch"))
    dim_customer = spark.read.parquet(gold_dimension_path("dim_customer"))

    orphan_branches = dim_account.join(
        dim_branch, "branch_id", "left_anti").count()
    if orphan_branches != 0:
        raise AssertionError(
            f"[dim_account] {orphan_branches} account(s) reference a branch_id not in dim_branch")

    known_customer_ids = dim_customer.select("customer_id").distinct()
    orphan_customers = dim_account.join(
        known_customer_ids, "customer_id", "left_anti").count()
    if orphan_customers != 0:
        raise AssertionError(
            f"[dim_account] {orphan_customers} hesab dim_customer-de olmayan customer_id-ye istinad edir")

    print("  [OK] dim_account.branch_id -> dim_branch ve dim_account.customer_id -> dim_customer kesikleri istinadlari etibarlidir")


def main():
    spark = create_spark_session("gold_dimensions")

    try:
        dim_customer_df, customer_history_count = build_dim_customer(spark)
        write_gold_dimension(dim_customer_df, "dim_customer")
        print(f" {gold_dimension_path('dim_customer')} Gold-a yazildi")

        dim_account_df, accounts_count = build_dim_account(spark)
        write_gold_dimension(dim_account_df, "dim_account")
        print(f" {gold_dimension_path('dim_account')} Gold-a yazildi")

        dim_branch_df, branches_count = build_dim_branch(spark)
        write_gold_dimension(dim_branch_df, "dim_branch")
        print(f" {gold_dimension_path('dim_branch')} Gold-a yazildi")

        dim_date_df, dim_date_count = build_dim_date(spark)
        write_gold_dimension(dim_date_df, "dim_date")
        print(f" {gold_dimension_path('dim_date')} Gold-a yazildi")
    except Exception as exc:
        raise RuntimeError(
            f"Gold dimensions qurularken ve ya yazilarken xeta bas verdi: {exc}") from exc

    print("\nGold dimensions tesdiqlenir...")
    try:
        validate_dim_customer(spark, customer_history_count)
        validate_standard_dimension(
            spark, "dim_account", "account_id", "account_sk", accounts_count)
        validate_standard_dimension(
            spark, "dim_branch", "branch_id", "branch_sk", branches_count)
        validate_standard_dimension(
            spark, "dim_date", "date_key", "date_sk", dim_date_count)
        check_dimension_relationships(spark)
    except Exception as exc:
        raise RuntimeError(f"Gold dimension-larin validation ugursuz oldu: {exc}") from exc

    print("\nGold dimensionlarin qurulmasi Ugurla Kecdi — dim_customer, dim_account, dim_branch və dim_date MinIO-da hazirdir.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nGold dimensionlar ugursuz oldu: {exc}", file=sys.stderr)
        sys.exit(1)
