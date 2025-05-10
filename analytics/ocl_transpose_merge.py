import os
import pandas as pd
import re
from pathlib import Path

# Assume script is run from analytics folder at project root
SCRIPT_DIR = Path(__file__).parent.resolve()
INPUT_DIR = SCRIPT_DIR.parent / "data_files/APR2025_transposed_data"
OUTPUT_DIR = SCRIPT_DIR.parent / "data_files/OP_APR2025_transposed_data"

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


def main():
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

if __name__ == "__main__":
    main()
