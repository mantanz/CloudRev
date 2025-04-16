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
        input_csv = './data_files/aop_budget.csv'
        df = pd.read_csv(input_csv)
        
        # Keep only the required columns
        df = df[['account_id', 'month', 'aop_amount']]
        
        # Calculate total AOP across all accounts
        total_aop = df['aop_amount'].sum()
        print(total_aop)
        
        # Calculate AOP proportions percentage
        df['aop_proportion_pct'] = ((df['aop_amount'] / total_aop) * 100)
        total_aop_pct = df['aop_proportion_pct'].sum()
        print(total_aop_pct)
        # Calculate minimum commitment shares
        df['min_comm_proportion'] = ((df['aop_proportion_pct'] * min_commitment) / 100)
        total_min_comm = df['min_comm_proportion'].sum()
        print(total_min_comm)
        df['min_comm_proportion'] = df['min_comm_proportion'].round(2)
        total_min_comm = df['min_comm_proportion'].sum()
        print(total_min_comm)

        df['aop_proportion_pct'] = df['aop_proportion_pct'].round(3)
        total_aop_pct = df['aop_proportion_pct'].sum()
        print(total_aop_pct)
        

        # Save the output
        df.to_csv(output_csv, index=False)
        
        print(f"\nSuccessfully created output file: {output_csv}")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    calculate_proportions()