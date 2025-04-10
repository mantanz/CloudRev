import pandas as pd

# Read the CSV files
aws_df = pd.read_csv('sqlite/transformed_aws_accountwise.csv')
deepika_df = pd.read_csv('sqlite/actual_spend_deepika.csv')

# Convert account_id to string and month to datetime for proper comparison
aws_df['account_id'] = aws_df['account_id'].astype(str)
deepika_df['account_id'] = deepika_df['account_id'].astype(str)

aws_df['month'] = pd.to_datetime(aws_df['month'])
deepika_df['month'] = pd.to_datetime(deepika_df['month'])

# Create a merged dataframe to find differences
merged_df = pd.merge(aws_df, deepika_df, on=['account_id', 'month'], how='left', indicator=True)

# Filter for rows that are only in AWS data
result = merged_df[merged_df['_merge'] == 'left_only']

# Select only the relevant columns
result = result[['account_id', 'account_name', 'month', 'spend_x']]  

# Rename columns to make them more readable
result = result.rename(columns={'spend_x': 'spend'})

# Sort by account_id and month
result = result.sort_values(['account_id', 'month'])

# Print the result
print("\nAccounts present in AWS data but not in actualspenddeepika:")
print(result)

# Save the result to a new CSV file
result.to_csv('accounts_not_in_deepika.csv', index=False)
print(f"\nResults saved to 'accounts_not_in_deepika.csv'")