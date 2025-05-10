import pandas as pd
import os
import re
from pathlib import Path


def transform_aws_accountwise(file_path):
    df = pd.read_csv(file_path, header=None)

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
    transposed_df['spend']=transposed_df['spend'].astype(float).round(4)

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
    transposed_df=transposed_df[['account_id','month','spend']]

    # Save the updated DataFrame to a CSV file
    transposed_df.to_csv('transformed_aws_accountwise_apr.csv', index=False) 

def transform_aws_account_service_wise(file_path):


        # Assume script is run from analytics folder at project root
        SCRIPT_DIR = Path(__file__).parent.resolve()
        INPUT_DIR = SCRIPT_DIR.parent / "data_files/APR2024_transposed_data"
        OUTPUT_DIR = SCRIPT_DIR.parent / "data_files/OP_APR2024_transposed_data"

        # Ensure output directory exists
        OUTPUT_DIR.mkdir(exist_ok=True)

        # Step 1: Build account name to id mapping
        def load_account_mapping(account_details_path):
            df = pd.read_csv(account_details_path)
            # Normalize names for matching
            df['Account Name'] = df['Account Name'].str.strip().str.lower()
            mapping = dict(zip(df['Account Name'], df['Account ID']))
            return mapping

        # Step 2: Prepare mapping dataframe for files and account ids
        def map_files_to_account_ids(input_dir, mapping):
            mapping_rows = []
            for file in input_dir.glob("*.csv"):
                if file.name == "Account Details.csv":
                    continue
                account_name = file.stem.strip().lower()
                account_id = mapping.get(account_name)
                if account_id:
                    mapping_rows.append({'filename': file.name, 'account_id': account_id})
            return pd.DataFrame(mapping_rows)

        # Step 3: Month formatting helper
        def format_month(month_str):
            match = re.match(r"([A-Za-z]+)-(\d{2})", month_str)
            if match:
                mon, yr = match.groups()
                yr_full = '20' + yr if int(yr) < 50 else '19' + yr
                return f"{mon}-{yr_full}"
            return month_str

        # Step 4: Transpose and validate a single file
        def transpose_and_validate(file_path, account_id):
            df = pd.read_csv(file_path)
            service_col = df.columns[0]
            # Exclude rows where Service == "Total costs ($)"
            df = df[df[service_col].astype(str).str.strip() != "Total costs($)"]
            month_cols = [col for col in df.columns if col != service_col]
            for col in month_cols:
                df[col] = df[col].astype(str).str.replace(",", "", regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df_long = df.melt(id_vars=[service_col], value_vars=month_cols,
                            var_name='month', value_name='spend')
            df_long['month'] = df_long['month'].apply(format_month)
            orig_month_totals = df[month_cols].sum().round(2)
            transposed_month_totals = df_long.groupby('month')['spend'].sum().round(2)
            month_map = {format_month(m): m for m in month_cols}
            for m, orig_col in month_map.items():
                if abs(orig_month_totals[orig_col] - transposed_month_totals.get(m, 0)) > 0.01:
                    raise ValueError(f"Month total mismatch for {m} in {file_path.name}")
            orig_service_totals = df.set_index(service_col)[month_cols].sum(axis=1).round(2)
            transposed_service_totals = df_long.groupby(service_col)['spend'].sum().round(2)
            for svc in orig_service_totals.index:
                if abs(orig_service_totals[svc] - transposed_service_totals.get(svc, 0)) > 1:
                    raise ValueError(f"Service total mismatch for {svc} in {file_path.name}")
            df_long.insert(0, 'account_id', account_id)
            df_long['spend'] = df_long['spend'].round(4)
            return df_long[['account_id', service_col, 'month', 'spend']]


      
        mapping = load_account_mapping(INPUT_DIR / "Account Details.csv")
        file_map_df = map_files_to_account_ids(INPUT_DIR, mapping)
        file_map_df.to_csv(OUTPUT_DIR / "file_accountid_mapping.csv", index=False)
        merged = []
        for _, row in file_map_df.iterrows():
            file = INPUT_DIR / row['filename']
            account_id = row['account_id']
            try:
                transposed = transpose_and_validate(file, account_id)
            except Exception as e:
                print(f"Skipping {file.name}: {e}")
                continue
            out_name = f"transposed_ocl_{file.stem}.csv"
            transposed.to_csv(OUTPUT_DIR / out_name, index=False)
            merged.append(transposed)
        if merged:
            df_merged = pd.concat(merged, ignore_index=True)
            df_merged.to_csv(OUTPUT_DIR / "transposed_merged_ocl.csv", index=False)
        print("Processing complete.")



def transform_aws_account_service_wise_daily(input_file, output_file):
    # Paths
    INPUT_FILE = input_file
    OUTPUT_FILE = output_file

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

    # Save transposed file
    df_long.to_csv(OUTPUT_FILE, index=False)
    print(f"Transposed file saved to {OUTPUT_FILE}")

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




file_type=input("Enter the file type: ")
if file_type == "accountwise monthly":
    file_path=input("Enter the input file path: ")
    transform_aws_accountwise(file_path)
elif file_type == "account service wise monthly":
    file_path=input("Enter the input file path: ")
    transform_aws_accountwise(file_path)
elif file_type == "account service wise monthly":
    file_path=input("Enter the input file path: ")
    transform_aws_accountwise(file_path)
elif file_type == "account service wise daily":
    INPUT_FILE = '../data_files/Daily AWS Service Wise Cost - April 1-22 - Daily AWS Service Wise Cost - April 1-22.csv'
    OUTPUT_FILE = '../data_files/Demo_Daily AWS Service Wise Cost - April 1-22.csv'
    transform_aws_account_service_wise_daily(INPUT_FILE, OUTPUT_FILE)
else:
    print("Invalid file type")



