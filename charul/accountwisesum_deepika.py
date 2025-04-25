import pandas as pd

# Read the actual_spend_deepika data
df = pd.read_csv('/Users/charulmauni/Desktop/CloudRev/sqlite/actual_spend_deepika.csv')

# Group by account_id and calculate sum of spend
account_spend_sum = df.groupby('account_id')['spend'].sum()

# Print the results
print("\nSpend by Account ID:")
print(account_spend_sum)

# Save the results to a new CSV file
account_spend_sum.to_csv('account_spend_sum.csv')