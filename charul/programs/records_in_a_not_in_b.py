import pandas as pd

# Read the CSV files
df_a = pd.read_csv('../data_files/actual_spend_d.csv')
df_b = pd.read_csv('../data_files/transformed_aws_accountwise.csv')

# Select relevant columns and merge
df_a = df_a[['account_id', 'month', 'spend']]
df_b = df_b[['account_id', 'month', 'spend']]

# Find records in A that are not in B
records_only_in_a = df_a.merge(
    df_b, 
    on=['account_id', 'month'], 
    how='left', 
    indicator=True
).query('_merge == "left_only"').drop('_merge', axis=1)

# Save the results
records_only_in_a.to_csv('../aws_p_errors/records_in_a_not_in_b.csv', index=False)
print(f"Found {len(records_only_in_a)} records in A that are not in B")
