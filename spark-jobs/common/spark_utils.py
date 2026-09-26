import os

from pyspark.sql import SparkSession


POSTGRES_INTERNAL_HOST = "postgres"
POSTGRES_INTERNAL_PORT = 5432

MINIO_BUCKET = "data-lake"


def create_spark_session(app_name: str) -> SparkSession:
    minio_user = _require_env("MINIO_ROOT_USER")
    minio_password = _require_env("MINIO_ROOT_PASSWORD")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_postgres_jdbc_url() -> str:
    db_name = _require_env("POSTGRES_DB")
    return f"jdbc:postgresql://{POSTGRES_INTERNAL_HOST}:{POSTGRES_INTERNAL_PORT}/{db_name}"


def get_postgres_properties() -> dict:
    return {
        "user": _require_env("POSTGRES_USER"),
        "password": _require_env("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }


def read_source_table(spark: SparkSession, table_name: str):
    return spark.read.jdbc(
        url=get_postgres_jdbc_url(),
        table=table_name,
        properties=get_postgres_properties(),
    )


def bronze_path(dataset_name: str) -> str:
    return f"s3a://{MINIO_BUCKET}/bronze/{dataset_name}/"


def silver_path(dataset_name: str) -> str:
    return f"s3a://{MINIO_BUCKET}/silver/{dataset_name}/"


def quarantine_path(dataset_name: str) -> str:
    return f"s3a://{MINIO_BUCKET}/quarantine/{dataset_name}/"


def gold_dimension_path(dimension_name: str) -> str:
    return f"s3a://{MINIO_BUCKET}/gold/dimensions/{dimension_name}/"


def gold_fact_path(fact_name: str) -> str:
    return f"s3a://{MINIO_BUCKET}/gold/facts/{fact_name}/"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Teleb olunan '{name}' environment variable teyin edilmeyib")
    return value
