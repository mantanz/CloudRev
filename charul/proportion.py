import pandas as pd

def calculate_proportions():
    """
    Calculate proportions for each account based on AOP and minimum commitment.
    
    Returns:
        None
    """
    try:
        # Get input from user
        min_commitment = float(input("Enter the minimum commitment value: "))
        output_csv = input("Enter the path for the output CSV file (press Enter for default): ")
        
        if not output_csv:
            output_csv = 'output_proportions.csv'
        
        # Read input CSV
        input_csv = '/Users/charulmauni/Desktop/CloudRev/data_files/aop_budget.csv'
        df = pd.read_csv(input_csv)
        
        # Keep only the required columns
        df = df[['account_id', 'month', 'aop_amount']]
        
        # Calculate total AOP across all accounts
        total_aop = df['aop_amount'].sum()
        
        # Calculate AOP proportions percentage
        df['aop_proportion_pct'] = (df['aop_amount'] / total_aop) * 100
        # Calculate minimum commitment shares
        df['min_comm_proportion'] = (df['aop_proportion_pct'] * min_commitment) / 100
        # Calculate totals
        total_aop_pct = df['aop_proportion_pct'].sum()
        total_min_comm = df['min_comm_proportion'].sum()
        # Round all numerical values to 2 decimal places
        df['aop_amount'] = df['aop_amount'].round(2)
        df['aop_proportion_pct'] = df['aop_proportion_pct'].round(2)
        df['min_comm_proportion'] = df['min_comm_proportion'].round(2)
        
        # Save the output
        df.to_csv(output_csv, index=False)
        
        print(f"\nSuccessfully created output file: {output_csv}")
        print("\nSample of calculated proportions:")
        print("-" * 80)
        
        # Display totals
        print("\nTotals:")
        print("-" * 80)
        print("\nTotal AOP amount:", total_aop)
        print("\nmin_comm_proportion_sum:", total_min_comm)
        print("\nTotal AOP proportion:", total_aop_pct)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    calculate_proportions()