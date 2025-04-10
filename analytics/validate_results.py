import pandas as pd
import numpy as np

# Read original files
print("Reading original files...")
df_a = pd.read_csv('../data_files/actual_spend_d.csv')
df_b = pd.read_csv('../data_files/transformed_aws_accountwise.csv')

print("\nOriginal file statistics:")
print(f"Records in actual_spend_d.csv: {len(df_a)}")
print(f"Records in transformed_aws_accountwise.csv: {len(df_b)}")
print(f"Unique accounts in actual_spend_d.csv: {df_a['account_id'].nunique()}")
print(f"Unique accounts in transformed_aws_accountwise.csv: {df_b['account_id'].nunique()}")

# Read and validate analysis files
print("\nValidating analysis files...")

# 1. Records in A not in B
a_not_b = pd.read_csv('../aws_p_errors/records_in_a_not_in_b.csv')
print(f"\n1. Records only in actual_spend_d.csv:")
print(f"Total records: {len(a_not_b)}")
print(f"Unique accounts: {a_not_b['account_id'].nunique()}")
print(f"Months covered: {a_not_b['month'].nunique()}")

# 2. Records in B not in A
b_not_a = pd.read_csv('../aws_p_errors/records_in_b_not_in_a.csv')
print(f"\n2. Records only in transformed_aws_accountwise.csv:")
print(f"Total records: {len(b_not_a)}")
print(f"Unique accounts: {b_not_a['account_id'].nunique()}")
print(f"Months covered: {b_not_a['month'].nunique()}")

# 3. Different spend records
diff_spend = pd.read_csv('../aws_p_errors/matching_records_different_spend.csv')
print(f"\n3. Records with different spend values:")
print(f"Total records: {len(diff_spend)}")
print(f"Unique accounts: {diff_spend['account_id'].nunique()}")
print(f"Months covered: {diff_spend['month'].nunique()}")
print("Spend difference statistics:")
diff_spend['spend_diff'] = diff_spend['spend_actual'] - diff_spend['spend_transformed']
print(diff_spend['spend_diff'].describe())

# 4. Different total spend
total_diff = pd.read_csv('../aws_p_errors/accounts_different_total_spend_summary.csv')
monthly_diff = pd.read_csv('../aws_p_errors/accounts_different_total_spend_monthly.csv')
print(f"\n4. Accounts with different total spend:")
print(f"Total accounts: {len(total_diff)}")
print(f"Total monthly records: {len(monthly_diff)}")
print("Total spend difference statistics:")
total_diff['total_spend_diff'] = total_diff['total_spend_actual'] - total_diff['total_spend_transformed']
print(total_diff['total_spend_diff'].describe())

# Validation checks
print("\nValidation Checks:")
# Check if sum of records equals total records
total_records_a = len(a_not_b) + len(diff_spend) + len(df_a[df_a['account_id'].isin(df_b['account_id']) & df_a['month'].isin(df_b['month'])])
print(f"Sum of analyzed records from A: {total_records_a}")
print(f"Total records in A: {len(df_a)}")

total_records_b = len(b_not_a) + len(diff_spend) + len(df_b[df_b['account_id'].isin(df_a['account_id']) & df_b['month'].isin(df_a['month'])])
print(f"Sum of analyzed records from B: {total_records_b}")
print(f"Total records in B: {len(df_b)}")
