import pandas as pd
import os
from pathlib import Path

def add_service_ids():
    """
    Add service_id column to APR2025 daily spend data by mapping from service_details.
    """
    # Define paths
    SCRIPT_DIR = Path(__file__).parent.resolve()
    DATA_DIR = SCRIPT_DIR.parent / "data_files"
    
    # Read the service details mapping
    service_details_path = DATA_DIR / "service_details_new.csv"
    service_mapping = pd.read_csv(service_details_path)
    
    # Read the daily spend data
    daily_spend_path = DATA_DIR / "APR2025_daily_spend.csv"
    daily_spend = pd.read_csv(daily_spend_path)
    
    # Create a mapping dictionary from service_name to service_id
    service_id_map = dict(zip(service_mapping['service_name'], service_mapping['service_id']))
    
    # Add service_id column by mapping service_name
    daily_spend['service_id'] = daily_spend['service_name'].map(service_id_map)
    
    # Reorder columns to put service_id first
    daily_spend = daily_spend[['account_id', 'day', 'spend','service_id']]
    
    # Save the updated data
    output_path = DATA_DIR / "APR2025_daily_spend_with_service_ids.csv"
    daily_spend.to_csv(output_path, index=False)
    print(f"\nUpdated data with service IDs has been saved to: {output_path}")
  

if __name__ == "__main__":
    add_service_ids()
