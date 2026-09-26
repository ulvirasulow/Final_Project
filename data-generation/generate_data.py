import hashlib
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

import config
import validators


def make_rng_and_faker(seed: int):
    random.seed(seed)
    fake = Faker(config.FAKER_LOCALE)
    Faker.seed(seed)
    return random, fake


def daterange_list(start: str, end: str):
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    days = (end_date - start_date).days
    return [start_date + timedelta(days=i) for i in range(days + 1)]


def random_date(rng, start: str, end: str):
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    delta_days = (end_date - start_date).days
    return start_date + timedelta(days=rng.randint(0, delta_days))


def add_audit_columns(df: pd.DataFrame, load_run_id: str, processed_at: str) -> pd.DataFrame:
    df = df.copy()
    df["load_run_id"] = load_run_id
    df["source_system"] = config.SOURCE_SYSTEM
    df["processed_at"] = processed_at
    return df


def format_bool_column(series: pd.Series) -> pd.Series:
    return series.map(lambda v: "true" if bool(v) else "false")


def to_date_str(d) -> str:
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return config.CSV_NULL_REPRESENTATION
    return d.strftime(config.DATE_FORMAT)


def generate_branches(rng, fake) -> pd.DataFrame:
    rows = []
    for branch_id in range(1, config.NUM_BRANCHES + 1):
        city = fake.city()
        rows.append({
            "branch_id": branch_id,
            "branch_name": f"{city} Branch",
            "branch_code": f"BR{branch_id:03d}",
            "city": city,
            "region": fake.state(),
            "country": "Azerbaijan" if rng.random() < 0.7 else fake.country(),
            "opening_date": to_date_str(random_date(rng, config.DATE_RANGE_START, "2023-06-30")),
        })
    return pd.DataFrame(rows)


def generate_dim_date() -> pd.DataFrame:
    rows = []
    for d in daterange_list(config.DATE_RANGE_START, config.DATE_RANGE_END):
        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d.strftime(config.DATE_FORMAT),
            "day": d.day,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "day_of_week": d.isoweekday(),
            "day_name": d.strftime("%A"),
            "is_weekend": d.isoweekday() in (6, 7),
        })
    df = pd.DataFrame(rows)
    df["is_weekend"] = format_bool_column(df["is_weekend"])
    return df


def compute_change_hash(segment: str, email: str, phone: str, credit_rating: int) -> str:
    raw = f"{segment}|{email}|{phone}|{credit_rating}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def generate_customers(rng, fake) -> pd.DataFrame:
    rows = []
    for customer_id in range(1, config.NUM_CUSTOMERS + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        segment = rng.choices(config.CUSTOMER_SEGMENTS,
                              weights=config.CUSTOMER_SEGMENT_WEIGHTS)[0]
        credit_rating_current = rng.randint(300, 850)
        credit_rating_previous = max(
            300, credit_rating_current - rng.randint(-40, 60))
        email = fake.unique.email()
        phone = fake.phone_number()

        rows.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "date_of_birth": to_date_str(dob),
            "email": email,
            "phone_number": phone,
            "address_country": "Azerbaijan" if rng.random() < 0.6 else fake.country(),
            "customer_segment": segment,
            "credit_rating_current": credit_rating_current,
            "credit_rating_previous": credit_rating_previous,
            "change_hash": compute_change_hash(segment, email, phone, credit_rating_current),
        })
    return pd.DataFrame(rows)


def generate_customer_history(rng, customers_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    end_date = datetime.strptime(config.DATE_RANGE_END, "%Y-%m-%d").date()

    for _, cust in customers_df.iterrows():
        customer_id = cust["customer_id"]

        acquisition = random_date(rng, config.DATE_RANGE_START, "2025-06-30")

        num_periods = rng.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]

        span_days = max((end_date - acquisition).days, 1)
        cut_points = sorted(rng.sample(range(1, span_days),
                            num_periods - 1)) if num_periods > 1 else []
        boundaries = [acquisition] + [acquisition +
                                      timedelta(days=c) for c in cut_points] + [end_date]

        for i in range(num_periods):
            effective_from = boundaries[i]
            is_last = (i == num_periods - 1)
            effective_to = None if is_last else boundaries[i + 1] - timedelta(
                days=1)

            if is_last:
                segment = cust["customer_segment"]
                credit_rating = cust["credit_rating_current"]
                email = cust["email"]
                phone = cust["phone_number"]
            else:
                segment = rng.choices(
                    config.CUSTOMER_SEGMENTS, weights=config.CUSTOMER_SEGMENT_WEIGHTS)[0]
                credit_rating = rng.randint(300, 850)
                email = cust["email"]
                phone = cust["phone_number"]

            rows.append({
                "customer_id": customer_id,
                "effective_from": to_date_str(effective_from),
                "effective_to": to_date_str(effective_to),
                "is_current": is_last,
                "first_name": cust["first_name"],
                "last_name": cust["last_name"],
                "email": email,
                "phone_number": phone,
                "customer_segment": segment,
                "credit_rating": credit_rating,
                "address_country": cust["address_country"],
            })

    df = pd.DataFrame(rows)
    df["is_current"] = format_bool_column(df["is_current"])
    return df


def generate_accounts(rng, customers_df: pd.DataFrame, branches_df: pd.DataFrame,
                      customer_history_df: pd.DataFrame) -> pd.DataFrame:
    customer_since = (
        customer_history_df.groupby("customer_id")["effective_from"]
        .min()
        .to_dict()
    )

    rows = []
    customer_ids = customers_df["customer_id"].tolist()
    branch_ids = branches_df["branch_id"].tolist()

    for account_id in range(1, config.NUM_ACCOUNTS + 1):
        customer_id = rng.choice(customer_ids)
        branch_id = rng.choice(branch_ids)
        since = datetime.strptime(
            customer_since[customer_id], "%Y-%m-%d").date()
        open_date = random_date(
            rng, since.strftime("%Y-%m-%d"), config.DATE_RANGE_END
        )

        rows.append({
            "account_id": account_id,
            "customer_id": customer_id,
            "branch_id": branch_id,
            "account_type": rng.choices(config.ACCOUNT_TYPES, weights=config.ACCOUNT_TYPE_WEIGHTS)[0],
            "account_status": rng.choices(config.ACCOUNT_STATUSES, weights=config.ACCOUNT_STATUS_WEIGHTS)[0],
            "currency": rng.choice(config.ALLOWED_CURRENCIES),
            "open_date": to_date_str(open_date),
        })
    return pd.DataFrame(rows)


def generate_bridge(rng, accounts_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    customer_ids = customers_df["customer_id"].tolist()
    num_joint = max(1, int(len(accounts_df) * config.JOINT_ACCOUNT_RATIO))
    joint_accounts = rng.sample(accounts_df["account_id"].tolist(), num_joint)

    rows = []
    for account_id in joint_accounts:
        primary_customer_id = accounts_df.loc[
            accounts_df["account_id"] == account_id, "customer_id"
        ].iloc[0]

        co_owner = rng.choice(customer_ids)
        while co_owner == primary_customer_id:
            co_owner = rng.choice(customer_ids)

        primary_weight = round(rng.uniform(0.5, 0.7), 2)
        co_owner_weight = round(1.0 - primary_weight, 2)

        rows.append({
            "account_id": account_id,
            "customer_id": primary_customer_id,
            "allocation_weight": primary_weight,
            "is_primary_owner": True,
        })
        rows.append({
            "account_id": account_id,
            "customer_id": co_owner,
            "allocation_weight": co_owner_weight,
            "is_primary_owner": False,
        })

    df = pd.DataFrame(rows)
    df["is_primary_owner"] = format_bool_column(df["is_primary_owner"])
    return df


def generate_transactions(rng, accounts_df: pd.DataFrame, customer_history_df: pd.DataFrame,
                          dim_date_df: pd.DataFrame) -> pd.DataFrame:

    account_ids = accounts_df["account_id"].tolist()
    account_open_date = accounts_df.set_index(
        "account_id")["open_date"].to_dict()
    account_customer_id = accounts_df.set_index(
        "account_id")["customer_id"].to_dict()
    customer_since = (
        customer_history_df.groupby("customer_id")[
            "effective_from"].min().to_dict()
    )

    window_start = datetime.strptime(
        config.TRANSACTION_DATE_START, "%Y-%m-%d").date()
    window_end = datetime.strptime(
        config.TRANSACTION_DATE_END, "%Y-%m-%d").date()

    valid_date_keys = set(dim_date_df["date_key"].tolist())

    rows = []
    for transaction_id in range(1, config.NUM_VALID_TRANSACTIONS + 1):
        account_id = rng.choice(account_ids)
        customer_id = account_customer_id[account_id]

        open_date = datetime.strptime(
            account_open_date[account_id], "%Y-%m-%d").date()
        customer_start = datetime.strptime(
            customer_since[customer_id], "%Y-%m-%d").date()

        minimum_transaction_date = max(window_start, open_date, customer_start)

        if minimum_transaction_date > window_end:
            raise ValueError(
                f"account_id={account_id} (customer_id={customer_id}) ucun duzgun tranzaksiya yoxdur: "
                f"minimum icaze verilen tranzaksiya tarixi ({minimum_transaction_date}), konfiqurasiya olunmus tranzaksiya "
                f"({window_end}) sonradir. config.py-de DATE_RANGE_*/TRANSACTION_DATE_* "
                f"hissesini yoxla."
            )

        txn_date = random_date(
            rng, minimum_transaction_date.strftime(
                "%Y-%m-%d"), window_end.strftime("%Y-%m-%d")
        )
        date_key = int(txn_date.strftime("%Y%m%d"))
        if date_key not in valid_date_keys:
            raise ValueError(
                f"yaradilmis emeliyyatin date_key={date_key} deyeri dim_date cedvelinde movcud deyil "
                f"(DATE_RANGE_START/END hissesinin TRANSACTION_DATE_START/END ehate etdiyini yoxla)."
            )

        txn_timestamp = datetime.combine(txn_date, datetime.min.time()) + timedelta(
            hours=rng.randint(0, 23), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
        )

        rows.append({
            "transaction_id": transaction_id,
            "transaction_ref": f"TXN-{transaction_id:06d}",
            "account_id": account_id,
            "date_key": date_key,
            "transaction_timestamp": txn_timestamp.strftime(config.TIMESTAMP_FORMAT),
            "amount": round(rng.uniform(1.0, 5000.0), 2),
            "currency": rng.choice(config.ALLOWED_CURRENCIES),
            "transaction_type": rng.choice(config.TRANSACTION_TYPES),
            "channel": rng.choice(config.TRANSACTION_CHANNELS),
        })

    df = pd.DataFrame(rows)
    df = inject_transaction_defects(df)
    return df


def inject_transaction_defects(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("transaction_id", drop=False)

    df.loc[config.NULL_AMOUNT_TRANSACTION_ID, "amount"] = None

    df.loc[config.ORPHAN_TRANSACTION_ID,
           "account_id"] = config.ORPHAN_ACCOUNT_ID

    df.loc[config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID,
           "amount"] = config.INVALID_AMOUNT_VALUE
    df.loc[config.INVALID_AMOUNT_CURRENCY_TRANSACTION_ID,
           "currency"] = config.INVALID_CURRENCY_VALUE

    df = df.reset_index(drop=True)

    original = df[df["transaction_id"] ==
                  config.DUPLICATE_TRANSACTION_ID].iloc[0].copy()
    duplicate = original.copy()
    dup_ts = datetime.strptime(
        duplicate["transaction_timestamp"], config.TIMESTAMP_FORMAT) + timedelta(seconds=5)
    duplicate["transaction_timestamp"] = dup_ts.strftime(
        config.TIMESTAMP_FORMAT)

    df = pd.concat([df, pd.DataFrame([duplicate])], ignore_index=True)
    return df


def generate_balance_snapshots(rng, accounts_df: pd.DataFrame, dim_date_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    all_dates = dim_date_df[["date_key", "full_date"]].sort_values("date_key")

    for _, acc in accounts_df.iterrows():
        account_id = acc["account_id"]
        open_date = acc["open_date"]

        eligible = all_dates[all_dates["full_date"] >= open_date]
        if len(eligible) < config.BALANCE_SNAPSHOTS_PER_ACCOUNT:
            continue
        idxs = [
            int(i * (len(eligible) - 1) /
                (config.BALANCE_SNAPSHOTS_PER_ACCOUNT - 1))
            for i in range(config.BALANCE_SNAPSHOTS_PER_ACCOUNT)
        ] if config.BALANCE_SNAPSHOTS_PER_ACCOUNT > 1 else [0]

        for idx in idxs:
            snap = eligible.iloc[idx]
            rows.append({
                "account_id": account_id,
                "snapshot_date_key": int(snap["date_key"]),
                "closing_balance": round(rng.uniform(0.0, 50000.0), 2),
                "currency": acc["currency"],
            })

    return pd.DataFrame(rows)


def generate_loans(rng, accounts_df: pd.DataFrame) -> pd.DataFrame:
    loan_accounts = accounts_df[accounts_df["account_type"]
                                == "LOAN"]["account_id"].tolist()
    if not loan_accounts:
        loan_accounts = accounts_df["account_id"].tolist()

    rows = []
    for loan_id in range(1, config.NUM_LOANS + 1):
        account_id = rng.choice(loan_accounts)
        status = rng.choices(config.LOAN_STATUSES,
                             weights=config.LOAN_STATUS_WEIGHTS)[0]

        application_date = random_date(
            rng, config.DATE_RANGE_START, "2025-09-30")
        approval_date = None
        disbursement_date = None
        closure_date = None

        if status in ("APPROVED", "DISBURSED", "CLOSED"):
            approval_date = application_date + \
                timedelta(days=rng.randint(1, 14))
        if status in ("DISBURSED", "CLOSED"):
            disbursement_date = approval_date + \
                timedelta(days=rng.randint(1, 10))
        if status == "CLOSED":
            closure_date = disbursement_date + \
                timedelta(days=rng.randint(30, 700))

        rows.append({
            "loan_id": loan_id,
            "account_id": account_id,
            "loan_amount": round(rng.uniform(1000.0, 100000.0), 2),
            "loan_status": status,
            "application_date": to_date_str(application_date),
            "approval_date": to_date_str(approval_date),
            "disbursement_date": to_date_str(disbursement_date),
            "closure_date": to_date_str(closure_date),
        })
    return pd.DataFrame(rows)


def main():
    rng, fake = make_rng_and_faker(config.SEED)

    load_run_id = f"run-{config.SEED}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    processed_at = datetime.now(timezone.utc).strftime(config.TIMESTAMP_FORMAT)

    print(
        f"Generating fake banking data (seed={config.SEED}, load_run_id={load_run_id})")
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    branches_df = generate_branches(rng, fake)
    dim_date_df = generate_dim_date()
    customers_df = generate_customers(rng, fake)
    customer_history_df = generate_customer_history(rng, customers_df)
    accounts_df = generate_accounts(
        rng, customers_df, branches_df, customer_history_df)
    bridge_df = generate_bridge(rng, accounts_df, customers_df)
    transactions_df = generate_transactions(
        rng, accounts_df, customer_history_df, dim_date_df)
    snapshots_df = generate_balance_snapshots(rng, accounts_df, dim_date_df)
    loans_df = generate_loans(rng, accounts_df)

    datasets = {
        "branches": branches_df,
        "dim_date": dim_date_df,
        "customers": customers_df,
        "customer_history": customer_history_df,
        "accounts": accounts_df,
        "account_customer_bridge": bridge_df,
        "transactions": transactions_df,
        "account_balance_snapshot": snapshots_df,
        "loan_lifecycle": loans_df,
    }

    for name, df in datasets.items():
        df_with_audit = add_audit_columns(df, load_run_id, processed_at)
        datasets[name] = df_with_audit
        out_path = config.OUTPUT_DIR / f"{name}.csv"
        df_with_audit.to_csv(
            out_path,
            index=False,
            encoding=config.CSV_ENCODING,
            na_rep=config.CSV_NULL_REPRESENTATION,
        )
        print(
            f"  wrote {out_path.relative_to(config.BASE_DIR.parent)}  ({len(df_with_audit)} rows)")

    print("\nyoxlanilir...")
    validators.validate_all(datasets)
    print("yoxlanma ugurla bitdi — butun 9 dataset uygundur.")


if __name__ == "__main__":
    main()
