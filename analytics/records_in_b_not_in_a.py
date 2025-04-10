import pandas as pd

# Read the CSV files
df_a = pd.read_csv('../data_files/actual_spend_d.csv')
df_b = pd.read_csv('../data_files/transformed_aws_accountwise.csv')

# Select relevant columns and merge
df_a = df_a[['account_id', 'month', 'spend']]
df_b = df_b[['account_id', 'month', 'spend']]

# Find records in B that are not in A
records_only_in_b = df_b.merge(
    df_a, 
    on=['account_id', 'month'], 
    how='left', 
    indicator=True
).query('_merge == "left_only"').drop('_merge', axis=1)

# Save the results
records_only_in_b.to_csv('../aws_p_errors/records_in_b_not_in_a.csv', index=False)
print(f"Found {len(records_only_in_b)} records in B that are not in A")
