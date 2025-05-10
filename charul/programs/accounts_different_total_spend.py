import pandas as pd

# Read the CSV files
df_a = pd.read_csv('../data_files/actual_spend_d.csv')
df_b = pd.read_csv('../data_files/transformed_aws_accountwise.csv')

# Calculate total spend per account in each dataset
total_spend_a = df_a.groupby('account_id')['spend'].sum().reset_index()
total_spend_b = df_b.groupby('account_id')['spend'].sum().reset_index()

# Rename columns to distinguish between sources
total_spend_a = total_spend_a.rename(columns={'spend': 'total_spend_actual'})
total_spend_b = total_spend_b.rename(columns={'spend': 'total_spend_transformed'})

# Merge the total spends
merged_totals = pd.merge(total_spend_a, total_spend_b, on='account_id')

# Find accounts where total spend differs
different_totals = merged_totals[
    merged_totals['total_spend_actual'] != merged_totals['total_spend_transformed']
]

# Get monthly details for these accounts
df_a = df_a[['account_id', 'month', 'spend']].rename(columns={'spend': 'spend_actual'})
df_b = df_b[['account_id', 'month', 'spend']].rename(columns={'spend': 'spend_transformed'})

# Get all monthly data for accounts with different totals
monthly_details = pd.merge(
    df_a[df_a['account_id'].isin(different_totals['account_id'])],
    df_b[df_b['account_id'].isin(different_totals['account_id'])],
    on=['account_id', 'month'],
    how='outer'
)

# Save both total differences and monthly details
different_totals.to_csv('../aws_p_errors/accounts_different_total_spend_summary.csv', index=False)
monthly_details.to_csv('../aws_p_errors/accounts_different_total_spend_monthly.csv', index=False)
print(f"Found {len(different_totals)} accounts with different total spend values")
