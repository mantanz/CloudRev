import pandas as pd

# Read both CSV files
df_deepika = pd.read_csv('account_spend_sum_deepika.csv')
df_parakh = pd.read_csv('account_spend_sum_parakh.csv')

# Rename columns to avoid conflicts
df_deepika = df_deepika.rename(columns={'spend': 'deepikasum'})
df_parakh = df_parakh.rename(columns={'spend': 'parakhspend'})

# Merge the dataframes on account_id, keeping only matching accounts
merged_df = pd.merge(df_deepika, df_parakh, on='account_id', how='inner')

# Filter for rows where the spend amounts don't match
mismatched_df = merged_df[merged_df['deepikasum'] != merged_df['parakhspend']]

# Save the result to a new CSV file
mismatched_df.to_csv('spend_mismatch.csv', index=False)

print(f"Number of mismatched accounts: {len(mismatched_df)}")
print("CSV file 'spend_mismatch.csv' has been created with the following columns:")
print("- account_id")
print("- deepikasum (from account_spend_sum_deepika.csv)")
print("- parakhspend (from account_spend_sum_parakh.csv)")