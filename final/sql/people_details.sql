CREATE TABLE people_details (
emp_id TEXT PRIMARY KEY,
emp_name TEXT,
email_id TEXT,
L1_mgr_id TEXT,
L1_mgr_name TEXT,
hod_id TEXT,
tech_product TEXT,
entity TEXT,
business_id TEXT,
percentage REAL CHECK (percentage >= 0 AND percentage <= 100) 
);