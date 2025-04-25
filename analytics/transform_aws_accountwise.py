import pandas as pd

# Read the CSV file
df = pd.read_csv('aws_account_wise_p.csv', header=None)

# Extract account IDs, names, and total spends
account_ids = df.iloc[0, :-1].tolist()  # Exclude the last column
account_names = df.iloc[1, :-1].tolist()
total_spends = df.iloc[2, :-1].tolist()

# Create a list to store the transposed data
transposed_data = []

# Iterate through the rows starting from the 4th row for monthly data
for i in range(3, len(df)):
    month = df.iloc[i, 0]  # Get the month from the first column
    for j in range(2, len(df.columns)):  # Iterate through account columns
        account_id = account_ids[j - 1]
        account_name = account_names[j - 1]
        spend = df.iloc[i, j-1]
        transposed_data.append([account_id, account_name, month, spend])

# Create a Pandas DataFrame from the transposed data
transposed_df = pd.DataFrame(transposed_data, columns=['account_id', 'account_name', 'month', 'spend'])

# Remove " ($)" from account_name column
transposed_df['account_name'] = transposed_df['account_name'].str.replace(' ($)', '', regex=False)

# Replace blank and NaN values in 'spend' column with 0
transposed_df['spend'] = transposed_df['spend'].fillna(0) # Replace NaN with 0
transposed_df['spend'] = transposed_df['spend'].replace('', 0) # Replace blank with 0

# Validation: Check if the total spend matches the sum of monthly spends for each account
for account_id, total_spend in zip(account_ids[1:], total_spends[1:]):
    # Convert total_spend to numeric type
    total_spend = pd.to_numeric(total_spend, errors='coerce')

    # Calculate monthly spend sum, converting to numeric
    monthly_spend_sum = pd.to_numeric(transposed_df[transposed_df['account_id'] == account_id]['spend'], errors='coerce').sum()

    # Check for NaN values
    if pd.isna(total_spend) or pd.isna(monthly_spend_sum):
        print(f"Validation Warning: Total spend or monthly spend for account {account_id} is not a valid number.")
    elif abs(total_spend - monthly_spend_sum) > 0.01:  # Allow a small tolerance for rounding errors # Changed 'elsif' to 'elif' and combined the conditions 
        print(f"Validation Error: Total spend for account {account_id} ({total_spend}) does not match the sum of monthly spends ({monthly_spend_sum}).") 

print(transposed_df)

# Save the updated DataFrame to a CSV file
transposed_df.to_csv('transformed_aws_accountwise.csv', index=False) 
