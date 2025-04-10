import pandas as pd

# Read the CSV files
df_a = pd.read_csv('../data_files/actual_spend_d.csv')
df_b = pd.read_csv('../data_files/transformed_aws_accountwise.csv')

# Select relevant columns
df_a = df_a[['account_id', 'month', 'spend']]
df_b = df_b[['account_id', 'month', 'spend']]

# Rename spend columns to distinguish between sources
df_a = df_a.rename(columns={'spend': 'spend_actual'})
df_b = df_b.rename(columns={'spend': 'spend_transformed'})

# Merge the dataframes
merged_df = pd.merge(df_a, df_b, on=['account_id', 'month'])

# Find records where spend values differ
different_spend = merged_df[merged_df['spend_actual'] != merged_df['spend_transformed']]

# Save the results
different_spend.to_csv('../aws_p_errors/matching_records_different_spend.csv', index=False)
print(f"Found {len(different_spend)} records with different spend values")
