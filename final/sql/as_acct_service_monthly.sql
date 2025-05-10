CREATE TABLE as_acct_service_monthly (
    account_id TEXT NOT NULL,
    month DATE NOT NULL,
    spend REAL NOT NULL,
    service_id INTEGER NOT NULL,
    UNIQUE(account_id, month,service_id)
);