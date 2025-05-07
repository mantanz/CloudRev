import pandas as pd

# Read the CSV files
df_aws = pd.read_csv('sqlite/transformed_aws_accountwise.csv')
df_actual = pd.read_csv('sqlite/actual_spend_deepika.csv')

# Convert account_id to string and month to datetime for proper comparison
df_aws['account_id'] = df_aws['account_id'].astype(str)
df_actual['account_id'] = df_actual['account_id'].astype(str)

df_aws['month'] = pd.to_datetime(df_aws['month'])
df_actual['month'] = pd.to_datetime(df_actual['month'])

# Create a merged dataframe to find differences
merged_df = pd.merge(df_actual, df_aws, on=['account_id', 'month'], how='left', indicator=True)

# Filter for rows that are only in actual_spend_deepika
not_in_aws = merged_df[merged_df['_merge'] == 'left_only']

# Select only the relevant columns
result = not_in_aws[['account_id', 'account_name', 'month', 'spend_x']]  

# Rename columns to make them more readable
result = result.rename(columns={'spend_x': 'spend'})
# Sort by account_id and month
result = result.sort_values(['account_id', 'month'])

# Print the result
print("Accounts present in actualspenddeepika but not in aws accountwise:")
print(result)

# Save the result to a CSV file
result.to_csv('not_in_aws.csv', index=False)