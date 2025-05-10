import pandas as pd

# Read the processed CSV file
df = pd.read_csv('../data_files/transformed_aws_accountwise_apr_processed.csv')

# Drop the account_name column
df = df.drop('account_name', axis=1)

# Save the modified data to a new CSV file
output_filename = '../data_files/monthly_spend_apr_final_no_account_name.csv'
df.to_csv(output_filename, index=False)

print(f"Column 'account_name' has been dropped and saved as {output_filename}")
print(f"Total number of rows: {len(df)}") 