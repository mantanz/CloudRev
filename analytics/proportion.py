import pandas as pd

def calculate_proportions():
    """
    Calculate proportions for each account based on AOP and minimum commitment.
    
    Returns:
        None
    """
    try:
        # Get input from user
        # min_commitment = float(input("Enter the minimum commitment value: "))
        min_commitment = 60000000
        # output_csv = input("Enter the path for the output CSV file (press Enter for default): ")
        
        # if not output_csv:
        output_csv = 'output_proportions.csv'
        
        # Read input CSV
        input_csv = '../data_files/aop_budget.csv'
        df = pd.read_csv(input_csv)
        
        # Keep only the required columns
        df = df[['account_id', 'month', 'aop_amount']]
        
        # Calculate total AOP across all accounts
        total_aop = df['aop_amount'].sum()
        print(total_aop)
        
        # Calculate AOP proportions percentage
        df['proportion_pct'] = ((df['aop_amount'] / total_aop) * 100)
        total_aop_pct = df['proportion_pct'].sum()
        print(total_aop_pct)
        # Calculate minimum commitment shares
        df['minimum_commitment'] = ((df['proportion_pct'] * min_commitment) / 100)
        total_min_comm = df['minimum_commitment'].sum()
        print(total_min_comm)
        df['minimum_commitment'] = df['minimum_commitment'].round(2)
        total_min_comm = df['minimum_commitment'].sum()
        print(total_min_comm)

        df['proportion_pct'] = df['proportion_pct'].round(3)
        total_aop_pct = df['proportion_pct'].sum()
        print(total_aop_pct)
        final_df = df[['account_id', 'month', 'proportion_pct', 'minimum_commitment']]
        # Save the output
        final_df.to_csv(output_csv, index=False)
        
        print(f"\nSuccessfully created output file: {output_csv}")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    calculate_proportions()