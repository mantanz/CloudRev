CREATE TABLE as_acct_service_daily(
    account_id TEXT NOT NULL,
    day DATE NOT NULL,
    spend REAL NOT NULL,
    service_id INTEGER MOT NULL ,
    UNIQUE(account_id,day,service_id)
);