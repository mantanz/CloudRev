CREATE TABLE aop_budget_monthly (
    account_id TEXT,
    month DATE NOT NULL,
    aop_amount REAL NOT NULL,
    UNIQUE(account_id, month)
);