import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK_CONTAINER = "project1-spark"
SPARK_SUBMIT_PREFIX = (
    f"docker exec {SPARK_CONTAINER} spark-submit --master local[*] "
    f"--properties-file /opt/spark-config/spark-defaults.conf"
)

REQUIRED_SOURCE_TABLES = [
    "branches", "dim_date", "customers", "customer_history", "accounts",
    "account_customer_bridge", "transactions", "account_balance_snapshot",
    "loan_lifecycle",
]


def check_source_ready():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            for table in REQUIRED_SOURCE_TABLES:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s)",
                    (table,),
                )
                if not cur.fetchone()[0]:
                    raise RuntimeError(f"teleb olunan '{table}' cedveli movcud deyil.")

                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()[0]
                if row_count == 0:
                    raise RuntimeError(f"teleb olunan '{table}' cedveli bosdur.")
    finally:
        conn.close()

    print(f"check_source_ready ugurla kecdi: butun {len(REQUIRED_SOURCE_TABLES)} cedveller movcuddur ve bos deyil.")


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="banking_batch_pipeline",
    description="Movcud Bronze -> Silver -> Gold pipeline orkestrasiya edir.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["project1-batch-pipeline"],
) as dag:

    generate_fake_data = BashOperator(
        task_id="generate_fake_data",
        bash_command="python3 /opt/airflow/data-generation/generate_data.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="POSTGRES_HOST=postgres python3 /opt/airflow/data-generation/load_to_postgres.py",
    )

    check_source_ready_task = PythonOperator(
        task_id="check_source_ready",
        python_callable=check_source_ready,
    )

    run_bronze = BashOperator(
        task_id="run_bronze",
        bash_command=f"{SPARK_SUBMIT_PREFIX} /opt/spark-jobs/bronze/bronze_ingestion.py",
    )

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command=f"{SPARK_SUBMIT_PREFIX} /opt/spark-jobs/silver/silver_processing.py",
    )

    run_gold_dimensions = BashOperator(
        task_id="run_gold_dimensions",
        bash_command=f"{SPARK_SUBMIT_PREFIX} /opt/spark-jobs/gold/gold_dimensions.py",
    )

    run_gold_fact_transaction = BashOperator(
        task_id="run_gold_fact_transaction",
        bash_command=f"{SPARK_SUBMIT_PREFIX} /opt/spark-jobs/gold/gold_fact_transaction.py",
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=f"{SPARK_SUBMIT_PREFIX} /opt/scripts/validate_gold.py",
    )

    (
        generate_fake_data
        >> load_to_postgres
        >> check_source_ready_task
        >> run_bronze
        >> run_silver
        >> run_gold_dimensions
        >> run_gold_fact_transaction
        >> validate_gold
    )
