CREATE TABLE account_details (
    account_id TEXT PRIMARY KEY,
    account_name TEXT,
    hod_id TEXT,
    entity TEXT,
    cloud_id INTEGER,
    business_id INTEGER,
    percentage REAL CHECK (percentage >= 0 AND percentage <= 100),
    prod_flg TEXT CHECK (prod_flg IN ('Y', 'N')),
    account_creation_date DATE,
    cls_flg TEXT CHECK (prod_flg IN ('Y', 'N')),
    cls_date DATE
);