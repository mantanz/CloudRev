CREATE TABLE as_acct_monthly(
    account_id TEXT NOT NULL,
    month DATE NOT NULL,
    spend REAL NOT NULL,
    UNIQUE(account_id, month)
);