import pandas as pd

# Read the CSV file
df = pd.read_csv('not_in_aws.csv')

# Group by account_id and calculate total spend per account
account_summary = df.groupby('account_id').agg({
    'spend': ['sum', 'count'],  # Total spend and number of months with data
    'month': ['min', 'max']     # First and last month of data
}).reset_index()

# Rename the columns for better readability
account_summary.columns = ['account_id', 'total_spend', 'months_with_data', 'first_month', 'last_month']

# Sort by total spend in descending order
account_summary = account_summary.sort_values('total_spend', ascending=False)

# Print the results
print("\nAccount-wise Spending Summary:")
print("-" * 80)
print(account_summary.to_string(index=False))

# Save the results to a new CSV file
account_summary.to_csv('account_wise_summary.csv', index=False)
print("\nResults have been saved to 'account_wise_summary.csv'")