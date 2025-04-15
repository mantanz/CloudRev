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
        
        # Group by account_id and sum AOP amounts
        account_totals = df.groupby('account_id')['aop_amount'].sum().reset_index()
        
        # Calculate total AOP across all accounts
        total_aop = account_totals['aop_amount'].sum()
        
        # Calculate AOP proportions percentage
        account_totals['aop_proportion_pct'] = (account_totals['aop_amount'] / total_aop) * 100 
        # Calculate minimum commitment shares
        account_totals['min_comm_proportion'] = (account_totals['aop_proportion_pct'] * min_commitment) / 100
        
        # Round all numerical values to 2 decimal places
        account_totals['aop_amount'] = account_totals['aop_amount'].round(2)
        account_totals['aop_proportion_pct'] = account_totals['aop_proportion_pct'].round(2)
        account_totals['min_comm_proportion'] = account_totals['min_comm_proportion'].round(2)
        
        # Calculate totals
        total_aop_pct = account_totals['aop_proportion_pct'].sum()
        total_min_comm = account_totals['min_comm_proportion'].sum()
        
        # Add totals row
        totals_row = pd.DataFrame({
            'account_id': ['Total'],
            'aop_amount': [total_aop],
            'aop_proportion_pct': [total_aop_pct],
            'min_comm_proportion': [total_min_comm]
        })
        
        # Concatenate with totals row
        account_totals = pd.concat([account_totals, totals_row], ignore_index=True)
        
        # Save the output
        account_totals.to_csv(output_csv, index=False)
        
        print(f"\nSuccessfully created output file: {output_csv}")
        print("\nSample of calculated proportions:")
        print("-" * 80)
        
        # Display first few rows with formatted percentages
        display_df = account_totals[['account_id', 'aop_amount', 'aop_proportion_pct', 'min_comm_proportion']].head()
        display_df['aop_proportion_pct'] = display_df['aop_proportion_pct'].apply(lambda x: f"{x:.2f}%")
        display_df['min_comm_proportion'] = display_df['min_comm_proportion'].apply(lambda x: f"{x:.2f}")
        print(display_df)
        
        # Display totals
        print("\nTotals:")
        print("-" * 80)
        totals_display = account_totals.tail(1)
        totals_display['aop_proportion_pct'] = totals_display['aop_proportion_pct'].apply(lambda x: f"{x:.2f}%")
        totals_display['min_comm_proportion'] = totals_display['min_comm_proportion'].apply(lambda x: f"{x:.2f}")
        print(totals_display)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    calculate_proportions()