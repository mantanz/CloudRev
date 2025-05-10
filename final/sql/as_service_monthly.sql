CREATE TABLE as_service_monthly (
    service_id INTEGER NOT NULL,
    month DATE NOT NULL,
    spend REAL NOT NULL,
    UNIQUE(service_id,month)
);