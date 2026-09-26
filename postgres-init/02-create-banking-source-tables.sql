CREATE TABLE IF NOT EXISTS branches (
    branch_id       INTEGER      NOT NULL PRIMARY KEY,
    branch_name     VARCHAR(150) NOT NULL,
    branch_code     VARCHAR(20)  NOT NULL,
    city            VARCHAR(100) NOT NULL,
    region          VARCHAR(100) NOT NULL,
    country         VARCHAR(100) NOT NULL,
    opening_date    DATE         NOT NULL,
    load_run_id     VARCHAR(100) NOT NULL,
    source_system   VARCHAR(50)  NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER      NOT NULL PRIMARY KEY,
    full_date       DATE         NOT NULL,
    day             SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,
    quarter         SMALLINT     NOT NULL,
    year            SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,
    day_name        VARCHAR(20)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    load_run_id     VARCHAR(100) NOT NULL,
    source_system   VARCHAR(50)  NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id             INTEGER      NOT NULL PRIMARY KEY,
    first_name              VARCHAR(100) NOT NULL,
    last_name               VARCHAR(100) NOT NULL,
    date_of_birth            DATE         NOT NULL,
    email                    VARCHAR(255) NOT NULL,
    phone_number             VARCHAR(50)  NOT NULL,
    address_country          VARCHAR(100) NOT NULL,
    customer_segment         VARCHAR(20)  NOT NULL,
    credit_rating_current    SMALLINT     NOT NULL,
    credit_rating_previous   SMALLINT     NOT NULL,
    change_hash              VARCHAR(32)  NOT NULL,
    load_run_id              VARCHAR(100) NOT NULL,
    source_system            VARCHAR(50)  NOT NULL,
    processed_at              TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_history (
    customer_id       INTEGER      NOT NULL REFERENCES customers (customer_id),
    effective_from    DATE         NOT NULL,
    effective_to      DATE         NULL,
    is_current        BOOLEAN      NOT NULL,
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    email             VARCHAR(255) NOT NULL,
    phone_number      VARCHAR(50)  NOT NULL,
    customer_segment  VARCHAR(20)  NOT NULL,
    credit_rating     SMALLINT     NOT NULL,
    address_country   VARCHAR(100) NOT NULL,
    load_run_id       VARCHAR(100) NOT NULL,
    source_system     VARCHAR(50)  NOT NULL,
    processed_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (customer_id, effective_from)
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id       INTEGER      NOT NULL PRIMARY KEY,
    customer_id      INTEGER      NOT NULL REFERENCES customers (customer_id),
    branch_id        INTEGER      NOT NULL REFERENCES branches (branch_id),
    account_type     VARCHAR(20)  NOT NULL,
    account_status   VARCHAR(20)  NOT NULL,
    currency         VARCHAR(10)  NOT NULL,
    open_date        DATE         NOT NULL,
    load_run_id      VARCHAR(100) NOT NULL,
    source_system    VARCHAR(50)  NOT NULL,
    processed_at     TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS account_customer_bridge (
    account_id         INTEGER       NOT NULL REFERENCES accounts (account_id),
    customer_id        INTEGER       NOT NULL REFERENCES customers (customer_id),
    allocation_weight  NUMERIC(4,2)  NOT NULL,
    is_primary_owner   BOOLEAN       NOT NULL,
    load_run_id        VARCHAR(100)  NOT NULL,
    source_system      VARCHAR(50)   NOT NULL,
    processed_at       TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (account_id, customer_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id         INTEGER       NOT NULL,
    transaction_ref        VARCHAR(20)   NOT NULL,
    account_id             INTEGER       NOT NULL,
    date_key                INTEGER       NOT NULL REFERENCES dim_date (date_key),
    transaction_timestamp  TIMESTAMPTZ   NOT NULL,
    amount                  NUMERIC(12,2) NULL,
    currency                VARCHAR(10)   NOT NULL,
    transaction_type        VARCHAR(20)   NOT NULL,
    channel                  VARCHAR(20)   NOT NULL,
    load_run_id              VARCHAR(100)  NOT NULL,
    source_system            VARCHAR(50)   NOT NULL,
    processed_at              TIMESTAMPTZ   NOT NULL
);

CREATE TABLE IF NOT EXISTS account_balance_snapshot (
    account_id          INTEGER       NOT NULL REFERENCES accounts (account_id),
    snapshot_date_key   INTEGER       NOT NULL REFERENCES dim_date (date_key),
    closing_balance     NUMERIC(14,2) NOT NULL,
    currency            VARCHAR(10)   NOT NULL,
    load_run_id         VARCHAR(100)  NOT NULL,
    source_system       VARCHAR(50)   NOT NULL,
    processed_at        TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (account_id, snapshot_date_key)
);

CREATE TABLE IF NOT EXISTS loan_lifecycle (
    loan_id             INTEGER       NOT NULL PRIMARY KEY,
    account_id          INTEGER       NOT NULL REFERENCES accounts (account_id),
    loan_amount         NUMERIC(14,2) NOT NULL,
    loan_status         VARCHAR(20)   NOT NULL,
    application_date    DATE          NOT NULL,
    approval_date       DATE          NULL,
    disbursement_date   DATE          NULL,
    closure_date        DATE          NULL,
    load_run_id         VARCHAR(100)  NOT NULL,
    source_system       VARCHAR(50)   NOT NULL,
    processed_at         TIMESTAMPTZ   NOT NULL
);
