import config


AUDIT_COLUMNS = ["load_run_id", "source_system", "processed_at"]


def _assert(condition: bool, message: str):
    if not condition:
        raise AssertionError(f"[Validation ugursuz oldu] {message}")


def check_audit_columns(datasets: dict):
    for name, df in datasets.items():
        for col in AUDIT_COLUMNS:
            _assert(col in df.columns,
                    f"{name}: catismayan audit sutunu '{col}'")
            _assert(df[col].notna().all(),
                    f"{name}: '{col}' sutununda NULL deyerler var")
    print("  [OK] audit sutunlari butun datasetlerde movcuddur ve doldurulub")


def check_referential_integrity(datasets: dict):
    customers = set(datasets["customers"]["customer_id"])
    branches = set(datasets["branches"]["branch_id"])
    accounts = set(datasets["accounts"]["account_id"])
    date_keys = set(datasets["dim_date"]["date_key"])

    accounts_df = datasets["accounts"]
    _assert(set(accounts_df["customer_id"]).issubset(customers),
            "accounts.customer_id movcud olmayan bir customer_id-ye istinad edir")
    _assert(set(accounts_df["branch_id"]).issubset(branches),
            "accounts.branch_id movcud olmayan bir branch_id-ye istinad edir")

    history_df = datasets["customer_history"]
    _assert(set(history_df["customer_id"]).issubset(customers),
            "customer_history.customer_id movcud olmayan bir customer_id-ye istinad edir")

    bridge_df = datasets["account_customer_bridge"]
    _assert(set(bridge_df["account_id"]).issubset(accounts),
            "account_customer_bridge.account_id movcud olmayan bir account_id-ye istinad edir")
    _assert(set(bridge_df["customer_id"]).issubset(customers),
            "account_customer_bridge.customer_id movcud olmayan bir customer_id-ye istinad edir")

    txns = datasets["transactions"]
    non_orphan_txns = txns[txns["transaction_id"]
                           != config.ORPHAN_TRANSACTION_ID]
    bad_accounts = set(non_orphan_txns["account_id"]) - accounts
    _assert(not bad_accounts,
            f"eliyyatlar gozlenilmeyen yanlis account_id-ye (ve ya account_id-lere) istinad edir: {bad_accounts}")
    _assert(set(txns["date_key"]).issubset(date_keys),
            "transactions.date_key movcud olmayan bir date_key-ye istinad edir")

    snapshots = datasets["account_balance_snapshot"]
    _assert(set(snapshots["account_id"]).issubset(accounts),
            "account_balance_snapshot.account_id movcud olmayan bir account_id-ye istinad edir")
    _assert(set(snapshots["snapshot_date_key"]).issubset(date_keys),
            "account_balance_snapshot.snapshot_date_key movcud olmayan bir date_key-ye istinad edir")

    loans = datasets["loan_lifecycle"]
    _assert(set(loans["account_id"]).issubset(accounts),
            "loan_lifecycle.account_id movcud olmayan bir account_id-ye istinad edir")

    print("  [OK] qesden saxlanilmis orphan emeliyyat istisna olmaqla butun datasetlerde referensial integritet qorunur")


def check_customer_history(datasets: dict):
    history_df = datasets["customer_history"].copy()
    history_df["is_current_bool"] = history_df["is_current"] == "true"

    for customer_id, group in history_df.groupby("customer_id"):
        current_rows = group[group["is_current_bool"]]
        _assert(len(current_rows) == 1,
                f"customer_id={customer_id}: 1 setir gozlenilirdi, {len(current_rows)} eded tapildi")

        periods = group.sort_values("effective_from")
        prev_effective_to = None
        for _, row in periods.iterrows():
            eff_from = row["effective_from"]
            eff_to = row["effective_to"]
            if prev_effective_to is not None:
                _assert(eff_from > prev_effective_to,
                        f"customer_id={customer_id}: ust-uste dusen tarixce dovrleri var")
            prev_effective_to = eff_to if eff_to != config.CSV_NULL_REPRESENTATION else "9999-12-31"

    print("  [OK] customer_history dovrleri ust-uste dusmur ve her birinin deqiq bir cari setri var")


def check_bridge_allocations(datasets: dict):
    bridge_df = datasets["account_customer_bridge"]
    for account_id, group in bridge_df.groupby("account_id"):
        total = round(group["allocation_weight"].sum(), 2)
        _assert(abs(total - 1.0) < 0.01,
                f"account_id={account_id}: allocation_weight cemi expected 1.0 gozlenilirdi, amma {total} edir")
    print("  [OK] her bir hesab ucun allocation_weight cemi 1.0-a beraberdir")


def check_transaction_defects(datasets: dict):
    txns = datasets["transactions"]
    accounts = set(datasets["accounts"]["account_id"])

    dup_rows = txns[txns["transaction_id"] == config.DUPLICATE_TRANSACTION_ID]
    _assert(len(dup_rows) == 2,
            f"transaction_id={config.DUPLICATE_TRANSACTION_ID} 2 defe gorunmeliydi, "
            f" {len(dup_rows)} defe tapildi")

    null_amount_row = txns[txns["transaction_id"]
                           == config.NULL_AMOUNT_TRANSACTION_ID]
    _assert(null_amount_row["amount"].isna().all(),
            f"transaction_id={config.NULL_AMOUNT_TRANSACTION_ID}  NULL amount olmali idi")

    orphan_row = txns[txns["transaction_id"] == config.ORPHAN_TRANSACTION_ID]
    _assert((orphan_row["account_id"] == config.ORPHAN_ACCOUNT_ID).all(),
            f"transaction_id={config.ORPHAN_TRANSACTION_ID} ucun "
            f"account_id={config.ORPHAN_ACCOUNT_ID} olmali idi")
    _assert(config.ORPHAN_ACCOUNT_ID not in accounts,
            f"account_id={config.ORPHAN_ACCOUNT_ID} accounts.csv faylinda movcud olmamalidir")

    bad_row = txns[txns["transaction_id"] ==
                   config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID]
    _assert((bad_row["amount"] == config.INVALID_AMOUNT_VALUE).all(),
            f"transaction_id={config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID} ucun "
            f"amount={config.INVALID_AMOUNT_VALUE} olmali idi")
    _assert((bad_row["currency"] == config.INVALID_CURRENCY_VALUE).all(),
            f"transaction_id={config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID} ucun "
            f"currency={config.INVALID_CURRENCY_VALUE} olmali idi")

    print(
        "  [OK] butun 5 qesden edilmis defect deqiq teleb olundugu kimi movcuddur")


def check_no_unintended_defects(datasets: dict):
    txns = datasets["transactions"]
    accounts = set(datasets["accounts"]["account_id"])
    defect_ids = {
        config.DUPLICATE_TRANSACTION_ID,
        config.NULL_AMOUNT_TRANSACTION_ID,
        config.ORPHAN_TRANSACTION_ID,
        config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID,
    }

    clean = txns[~txns["transaction_id"].isin(defect_ids)]

    _assert(clean["transaction_id"].is_unique,
            "qesden edilmis xetalardan kenar dublikat transaction_id tapildi")

    _assert((clean["amount"] > 0).all(),
            "qesden edilmis xetalardan kenar musbet olmayan ve ya NULL mebleg tapildi")
    _assert(clean["account_id"].isin(accounts).all(),
            "qesden edilmis xetalardan kenar invalid account_id tapildi")
    _assert(clean["currency"].isin(config.ALLOWED_CURRENCIES).all(),
            "qesden edilmis xetalardan kenar invalid currency tapildi")

    print("  [OK] teleb olunan 4 transaction_id-den basqa hec bir defect yoxdur")


def check_transaction_temporal_consistency(datasets: dict):
    txns = datasets["transactions"]
    accounts = datasets["accounts"]
    history = datasets["customer_history"]

    defect_ids = {
        config.DUPLICATE_TRANSACTION_ID,
        config.NULL_AMOUNT_TRANSACTION_ID,
        config.ORPHAN_TRANSACTION_ID,
        config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID,
    }
    clean = txns[~txns["transaction_id"].isin(defect_ids)].copy()

    account_open_date = accounts.set_index("account_id")["open_date"].to_dict()
    account_customer_id = accounts.set_index(
        "account_id")["customer_id"].to_dict()
    customer_since = history.groupby("customer_id")[
        "effective_from"].min().to_dict()

    clean["txn_date"] = clean["transaction_timestamp"].str.slice(0, 10)
    clean["account_open_date"] = clean["account_id"].map(account_open_date)
    clean["customer_since"] = clean["account_id"].map(
        account_customer_id).map(customer_since)

    bad_open = clean[clean["txn_date"] < clean["account_open_date"]]
    _assert(len(bad_open) == 0,
            f"{len(bad_open)} emeliyyat aid oldugu hesabin open_date tarixinden evvel bas verib")

    bad_history = clean[clean["txn_date"] < clean["customer_since"]]
    _assert(len(bad_history) == 0,
            f"{len(bad_history)} emeliyyat musterinin en erken tarixce dovrunden evvel bas verib")
    print("  [OK] butun normal emeliyyatlar hesabin acilis tarixinde veya sonrasinda ve musterinin en erken tarixce dovrunde veya sonrasinda bas verir")


def validate_all(datasets: dict):
    required = [
        "branches", "dim_date", "customers", "customer_history", "accounts",
        "account_customer_bridge", "transactions", "account_balance_snapshot",
        "loan_lifecycle",
    ]
    for name in required:
        _assert(name in datasets, f"gozlenilen dataset catmir: {name}")
    print("  [OK] butun 9 dataset movcuddur")

    check_audit_columns(datasets)
    check_referential_integrity(datasets)
    check_customer_history(datasets)
    check_bridge_allocations(datasets)
    check_transaction_defects(datasets)
    check_no_unintended_defects(datasets)
    check_transaction_temporal_consistency(datasets)
