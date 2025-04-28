import pandas as pd
import numpy as np
import sys

# Paths
INPUT_FILE = '../data_files/Copy of DailyServiceWiseCost - DailyServiceWiseCost.csv'
# OUTPUT_FILE = '../data_files/OP-OCL_Apr24_Mar25/transposed_ocl_accountwise_new.csv'
OUTPUT_FILE = '../data_files/final_transposed_daily_spend_19-25.csv'
# Read the mapping file
mapping_df = pd.read_csv('../data_files/service_name_mapping.csv')
service_details_df = pd.read_csv('../data_files/service_details_new.csv')

def main():
    # Read CSV, force Account ID as string
    df = pd.read_csv(INPUT_FILE, dtype={'account_id': str})

    # Clean column names
    df.columns = [col.strip() for col in df.columns]

    # Drop total_cost column if it exists
    if 'total_cost' in df.columns:
        df = df.drop(columns=['total_cost'])

    # Filter out rows where Account ID is blank or NaN
    id_col = 'account_id'
    df_filtered = df[df[id_col].notna() & (df[id_col].astype(str).str.strip() != '')].copy()

    # Identify value columns (days) - exclude account_id and service_name
    value_vars = [col for col in df_filtered.columns if col not in [id_col, 'service_name']]

    # Remove commas and convert to numeric for all month columns
    for col in value_vars:
        df_filtered[col] = df_filtered[col].astype(str).str.replace(',', '', regex=False)
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').fillna(0)

    # Melt to long format: account_id, service_name, month, spend
    df_long = df_filtered.melt(id_vars=[id_col, 'service_name'], value_vars=value_vars,
                              var_name='day', value_name='spend')
    df_long = df_long.rename(columns={id_col: 'account_id'})

    # # Normalize day format from 'Apr-24' to 'Apr-2024'
    # def fix_day_format(d):
    #     try:
    #         if '-' in d:
    #             day, month, year = d.split('-')
    #             if len(year) == 2:
    #                 year = '20' + year
    #             return f"{day}-{month}-{year}"
    #         else:
    #             return d
    #     except Exception:
    #         return d
    # df_long['day'] = df_long['day'].apply(fix_day_format)

    # Normalize account_id to string and remove trailing .0 if present
    df_long['account_id'] = df_long['account_id'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_filtered[id_col] = df_filtered[id_col].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    # Create a dictionary for mapping
    service_name_mapping = dict(zip(mapping_df['service_name'], mapping_df['new_service_name']))
    service_id_mapping = dict(zip(service_details_df['service_name'], service_details_df['service_id']))
      # Check if all service names in cost data have a mapping
    unmapped_services = set(df_long['service_name'].unique()) - set(mapping_df['service_name'].unique())
    if unmapped_services:
        print(f"Error: The following services in cost data have no mapping: {unmapped_services}")
        sys.exit(1)
    # Update service names using the mapping
    df_long['service_name'] = df_long['service_name'].map(service_name_mapping).fillna(df_long['service_name'])
    df_long['service_id'] = df_long['service_name'].map(service_id_mapping).fillna(df_long['service_name'])

    #drop the service_name column
    df_long = df_long.drop(columns=['service_name'])

    # Validation: totals by month and account
    print("\nValidating totals...")
    
    # Calculate totals in original data
    orig_totals_month = df_filtered[value_vars].sum()
    orig_totals_account = df_filtered.set_index(id_col)[value_vars].sum(axis=1)
    
    # Calculate totals in transposed data
    trans_totals_month = df_long.groupby('day')['spend'].sum()
    trans_totals_account = df_long.groupby('account_id')['spend'].sum()

    # Compare totals by month
    print("\nTotals by day (original vs transposed):")
    for d in value_vars:
        orig = orig_totals_month[d]
        trans = trans_totals_month.get(d, 0)
        is_match = abs(orig - trans) < 0.01  # Using a small tolerance
        print(f"{d}: original={orig:.2f}, transposed={trans:.2f}, match={is_match}")

    # Compare totals by account (show mismatches only)
    print("\nTotals by account (showing mismatches):")
    mismatch = False
    for acc in orig_totals_account.index:
        # Convert Series to float by taking the sum
        orig = orig_totals_account.loc[acc].sum()
        trans = trans_totals_account.get(str(acc), 0)
        if abs(orig - trans) > 0.01:  # Using a small tolerance
            print(f"Account {acc}: original={orig:.2f}, transposed={trans:.2f}")
            mismatch = True
    if not mismatch:
        print("All account totals match.")

     # Find the maximum length of numerical account_ids
    max_length = df_long['account_id'].astype(str).str.len().max()
    print(f"\nMaximum numerical account_id length: {max_length}")

    # Pad account_ids with leading zeros
    df_long['account_id'] = df_long['account_id'].astype(str).str.zfill(max_length)

    # Show processing summary
    print("\nProcessing Summary:")
    print("\nSample of processed account_ids (showing first 20):")
    print(df_long[['account_id']].head(20).to_string(index=False))
    # Save transposed file
    df_long.to_csv(OUTPUT_FILE, index=False)
    print(f"Transposed file saved to {OUTPUT_FILE}")

if __name__ == '__main__':
    main()
