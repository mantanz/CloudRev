import pandas as pd
import numpy as np

# Paths
INPUT_FILE = '../data_files/Service-Wise.csv'
OUTPUT_FILE = '../data_files/servicewise_data_transposed_new.csv'

def main():
    # Read CSV, force service_id as string
    df = pd.read_csv(INPUT_FILE, dtype={'service_id': str})

    # Clean column names
    df.columns = [col.strip() for col in df.columns]

    # Filter out rows where service_id is blank or NaN and drop 'service_type' column if present
    id_col = 'service_id'
    df_filtered = df[df[id_col].notna() & (df[id_col].astype(str).str.strip() != '')].copy()
    if 'service_type' in df_filtered.columns:
        df_filtered = df_filtered.drop(columns=['service_type'])

    # Identify value columns (months)
    value_vars = [col for col in df_filtered.columns if col != id_col]

    # Remove commas and convert to numeric for all month columns
    for col in value_vars:
        df_filtered[col] = df_filtered[col].astype(str).str.replace(',', '', regex=False)
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').fillna(0)

    # Melt to long format: account_id, month, spend
    df_long = df_filtered.melt(id_vars=id_col, value_vars=value_vars,
                              var_name='month', value_name='spend')
    df_long = df_long.rename(columns={id_col: 'account_id'})

    # Normalize month format from 'Apr-24' to 'Apr-2024'
    def fix_month_format(m):
        try:
            if '-' in m:
                month, year = m.split('-')
                if len(year) == 2:
                    year = '20' + year
                return f"{month}-{year}"
            else:
                return m
        except Exception:
            return m
    df_long['month'] = df_long['month'].apply(fix_month_format)

    # Normalize account_id to string and remove trailing .0 if present
    df_long['account_id'] = df_long['account_id'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_filtered[id_col] = df_filtered[id_col].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    # Save transposed file
    df_long.to_csv(OUTPUT_FILE, index=False)
    print(f"Transposed file saved to {OUTPUT_FILE}")

    # Validation: totals by month and account
    print("\nValidating totals...")
    # Totals in original (after filtering)
    orig_totals_month = df_filtered[value_vars].sum()
    orig_totals_account = df_filtered.set_index(id_col)[value_vars].sum(axis=1)
    # Totals in transposed
    trans_totals_month = df_long.groupby('month')['spend'].sum()
    trans_totals_account = df_long.groupby('account_id')['spend'].sum()

    # Compare totals by month
    print("\nTotals by month (original vs transposed):")
    for m in value_vars:
        orig = float(orig_totals_month[m])
        trans = float(trans_totals_month.get(m, 0))
        print(f"{m}: original={orig}, transposed={trans}, match={np.isclose(orig, trans)}")

    # Compare totals by account (show mismatches only)
    print("\nTotals by account (showing mismatches):")
    mismatch = False
    for acc in orig_totals_account.index:
        orig = float(orig_totals_account[acc])
        trans = float(trans_totals_account.get(str(acc), 0))
        if not np.isclose(orig, trans):
            print(f"Account {acc}: original={orig}, transposed={trans}")
            mismatch = True
    if not mismatch:
        print("All account totals match.")

if __name__ == '__main__':
    main()
