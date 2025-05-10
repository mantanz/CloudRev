CREATE TABLE people_spend (
    hod_id TEXT NOT NULL,
    month DATE NOT NULL,
    spend REAL NOT NULL,
    head_count INTEGER NOT NULL,
    resource_type TEXT NOT NULL
    UNIQUE(hod_id, month)
);