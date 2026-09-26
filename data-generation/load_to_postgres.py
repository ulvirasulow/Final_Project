import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

import config

PROJECT_ROOT = config.BASE_DIR.parent
DDL_PATH = PROJECT_ROOT / "postgres-init" / \
    "02-create-banking-source-tables.sql"

TABLES = [
    ("branches", "branches.csv"),
    ("dim_date", "dim_date.csv"),
    ("customers", "customers.csv"),
    ("customer_history", "customer_history.csv"),
    ("accounts", "accounts.csv"),
    ("account_customer_bridge", "account_customer_bridge.csv"),
    ("transactions", "transactions.csv"),
    ("account_balance_snapshot", "account_balance_snapshot.csv"),
    ("loan_lifecycle", "loan_lifecycle.csv"),
]


def get_connection():
    load_dotenv(PROJECT_ROOT / ".env")
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    conn.autocommit = False
    return conn


def run_ddl(conn):
    sql = DDL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(
        f"  [OK] cedvel strukturlari teyin edildi: {DDL_PATH.relative_to(PROJECT_ROOT)}")


def truncate_all_tables(conn):
    table_names = ", ".join(name for name, _ in TABLES)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_names} CASCADE;")
    conn.commit()
    print(
        "  [OK] butun 9 cedvel temizlendi (yeniden yuklenmeden evvel idempotent olaraq)")


def load_csv_into_table(conn, table_name: str, csv_filename: str):
    csv_path = config.OUTPUT_DIR / csv_filename
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} tapilmadi. evvelce data-generation/generate_data.py ise salin."
        )
    with conn.cursor() as cur, open(csv_path, "r", encoding=config.CSV_ENCODING) as f:
        cur.copy_expert(
            f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')",
            f,
        )
    conn.commit()
    row_count = count_csv_rows(csv_path)
    print(f"  [OK] yuklendi: {table_name:<28} {row_count} setir")


def count_csv_rows(csv_path: Path) -> int:
    with open(csv_path, "r", encoding=config.CSV_ENCODING) as f:
        return sum(1 for _ in f) - 1


def _fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()[0]


def _assert(condition: bool, message: str):
    if not condition:
        raise AssertionError(f"[YOXLAMA UGURSUZ OLDU] {message}")


def validate_table_existence(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        existing = {row[0] for row in cur.fetchall()}
    for table_name, _ in TABLES:
        _assert(table_name in existing, f"'{table_name}' cedveli movcud deyil")
    print("  [OK] butun 9 cedvel movcuddur)")


def validate_row_counts(conn):
    for table_name, csv_filename in TABLES:
        csv_count = count_csv_rows(config.OUTPUT_DIR / csv_filename)
        db_count = _fetch_one(conn, f"SELECT COUNT(*) FROM {table_name}")
        _assert(db_count == csv_count,
                f"{table_name}: gozlenilen setir sayi: {csv_count}, PostgreSQL-de tapilan: {db_count}")
    print("  [OK] Butun cedveller ucun db setir sayi csv fayllari ile eynidir")


def validate_audit_columns(conn):
    for table_name, _ in TABLES:
        missing = _fetch_one(conn, f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE load_run_id IS NULL OR source_system IS NULL OR processed_at IS NULL
        """)
        _assert(
            missing == 0, f"{table_name}: {missing} setirde audit sutunlarin deyeri NULL-dir")
    print("  [OK] butun cedvellerde audit sutunlari dolduruldu")


def validate_relationships(conn):
    checks = [
        ("accounts -> customers", """
            SELECT COUNT(*) FROM accounts a
            LEFT JOIN customers c ON a.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """),
        ("accounts -> branches", """
            SELECT COUNT(*) FROM accounts a
            LEFT JOIN branches b ON a.branch_id = b.branch_id
            WHERE b.branch_id IS NULL
        """),
        ("customer_history -> customers", """
            SELECT COUNT(*) FROM customer_history h
            LEFT JOIN customers c ON h.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """),
        ("account_customer_bridge -> accounts", """
            SELECT COUNT(*) FROM account_customer_bridge br
            LEFT JOIN accounts a ON br.account_id = a.account_id
            WHERE a.account_id IS NULL
        """),
        ("account_customer_bridge -> customers", """
            SELECT COUNT(*) FROM account_customer_bridge br
            LEFT JOIN customers c ON br.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """),
        (f"transactions -> accounts (excluding transaction_id={config.ORPHAN_TRANSACTION_ID})", f"""
            SELECT COUNT(*) FROM transactions t
            LEFT JOIN accounts a ON t.account_id = a.account_id
            WHERE a.account_id IS NULL
              AND t.transaction_id != {config.ORPHAN_TRANSACTION_ID}
        """),
        ("transactions -> dim_date", """
            SELECT COUNT(*) FROM transactions t
            LEFT JOIN dim_date d ON t.date_key = d.date_key
            WHERE d.date_key IS NULL
        """),
        ("account_balance_snapshot -> accounts", """
            SELECT COUNT(*) FROM account_balance_snapshot s
            LEFT JOIN accounts a ON s.account_id = a.account_id
            WHERE a.account_id IS NULL
        """),
        ("account_balance_snapshot -> dim_date", """
            SELECT COUNT(*) FROM account_balance_snapshot s
            LEFT JOIN dim_date d ON s.snapshot_date_key = d.date_key
            WHERE d.date_key IS NULL
        """),
        ("loan_lifecycle -> accounts", """
            SELECT COUNT(*) FROM loan_lifecycle l
            LEFT JOIN accounts a ON l.account_id = a.account_id
            WHERE a.account_id IS NULL
        """),
    ]
    for label, sql in checks:
        bad = _fetch_one(conn, sql)
        _assert(bad == 0, f"'{label}' elaqesinde {bad} orphan setir var")
    print(
        "  [OK] butun elaqeler duzgundur (qesden saxlanilmis orphan setirler olmadan)")


def validate_transaction_defects(conn):
    dup_count = _fetch_one(
        conn, "SELECT COUNT(*) FROM transactions WHERE transaction_id = %s",
        (config.DUPLICATE_TRANSACTION_ID,),
    )
    _assert(dup_count == 2,
            f"transaction_id={config.DUPLICATE_TRANSACTION_ID} deqiq 2 defe cixmasi gozlenilirdi, amma {dup_count} defe tapildi")

    null_amount = _fetch_one(
        conn, "SELECT COUNT(*) FROM transactions WHERE transaction_id = %s AND amount IS NULL",
        (config.NULL_AMOUNT_TRANSACTION_ID,),
    )
    _assert(null_amount == 1,
            f"transaction_id={config.NULL_AMOUNT_TRANSACTION_ID} amount deyeri null olmasi gozlenilirdi")

    orphan = _fetch_one(
        conn, "SELECT COUNT(*) FROM transactions WHERE transaction_id = %s AND account_id = %s",
        (config.ORPHAN_TRANSACTION_ID, config.ORPHAN_ACCOUNT_ID),
    )
    _assert(orphan == 1,
            f"transaction_id={config.ORPHAN_TRANSACTION_ID} ucun account_id={config.ORPHAN_ACCOUNT_ID} olmasi gozlenilirdi")

    orphan_exists = _fetch_one(
        conn, "SELECT COUNT(*) FROM accounts WHERE account_id = %s", (config.ORPHAN_ACCOUNT_ID,)
    )
    _assert(orphan_exists == 0,
            f"account_id={config.ORPHAN_ACCOUNT_ID} deyeri accounts cedvelinde movcud olmamalidir")

    bad_row = _fetch_one(
        conn,
        "SELECT COUNT(*) FROM transactions WHERE transaction_id = %s AND amount = %s AND currency = %s",
        (config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID,
         config.INVALID_AMOUNT_VALUE, config.INVALID_CURRENCY_VALUE),
    )
    _assert(bad_row == 1,
            f"transaction_id={config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID} ucun "
            f" amount={config.INVALID_AMOUNT_VALUE} ve currency={config.INVALID_CURRENCY_VALUE} olmasi gozlenilirdi")

    print("  [OK] Qesden daxil edilmis 5 xetali hisse PostgreSQL-de teleb olundugu kimi movcuddur")


def validate_all(conn):
    validate_table_existence(conn)
    validate_row_counts(conn)
    validate_audit_columns(conn)
    validate_relationships(conn)
    validate_transaction_defects(conn)


def main():
    print("banking_source bazasina qosulur...")
    conn = get_connection()
    try:
        print("\nCedvel strukturlari tetbiq edilir...")
        run_ddl(conn)

        print("\nCedveller sifirlanir (idempotent yenilenme)...")
        truncate_all_tables(conn)

        print("\nCsv fayllari yuklenir...")
        for table_name, csv_filename in TABLES:
            load_csv_into_table(conn, table_name, csv_filename)

        print("\nYuklenen datalar yoxlanilir...")
        validate_all(conn)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
