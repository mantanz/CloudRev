import pandas as pd

# Read both CSV files
transformed_df = pd.read_csv('/Users/charulmauni/Desktop/CloudRev/sqlite/transformed_aws_accountwise.csv')
deepika_df = pd.read_csv('/Users/charulmauni/Desktop/CloudRev/sqlite/actual_spend_deepika.csv')

# Ensure date formats match
transformed_df['month'] = pd.to_datetime(transformed_df['month']).dt.strftime('%Y-%m-%d')
deepika_df['month'] = pd.to_datetime(deepika_df['month']).dt.strftime('%Y-%m-%d')

# Merge the dataframes on account_id and month
merged_df = pd.merge(
    transformed_df[['account_id', 'month', 'spend']],
    deepika_df[['account_id', 'month', 'spend']],
    on=['account_id', 'month'],
    how='inner',
    suffixes=('_transformed', '_deepika')
)

# Rename columns for clarity
final_df = merged_df.rename(columns={
    'spend_transformed': 'transformedactualspend',
    'spend_deepika': 'deepikaactualspend'
})

# Save to a new CSV file
output_file = '/Users/charulmauni/Desktop/CloudRev/sqlite/compared_spend.csv'
final_df.to_csv(output_file, index=False)

print(f"Comparison file has been created at: {output_file}")
print(f"Total matched records: {len(final_df)}")