import sqlite3
import pandas as pd
from typing import Tuple, Optional

def get_or_create_service_id(service_name: str, db_path: str) -> Tuple[bool, int, str]:
    """
    Get existing service_id or create new entry in service_details table.
    Returns: (success, service_id, service_type)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First try to get existing service
        cursor.execute("""
            SELECT service_id, service_type 
            FROM service_details 
            WHERE service_name = ?
        """, (service_name,))
        
        result = cursor.fetchone()
        
        if result:
            # Service exists, return its details
            service_id, service_type = result
            conn.close()
            return True, service_id, service_type
            
        # Service doesn't exist, create new entry
        # First get the max service_id
        cursor.execute("SELECT MAX(service_id) FROM service_details")
        max_id = cursor.fetchone()[0]
        new_service_id = 1 if max_id is None else max_id + 1
        
        # Determine service type using AI categorization
        service_type = categorize_service(service_name)
        
        # Insert new service
        cursor.execute("""
            INSERT INTO service_details (service_id, service_name, service_type)
            VALUES (?, ?, ?)
        """, (new_service_id, service_name, service_type))
        
        conn.commit()
        conn.close()
        
        return True, new_service_id, service_type
        
    except Exception as e:
        print(f"Error in get_or_create_service_id: {str(e)}")
        return False, -1, ""

def categorize_service(service_name: str) -> str:
    """
    Categorize service into one of the predefined types.
    This is a simplified version - you may want to use more sophisticated AI categorization.
    """
    service_name = service_name.lower()
    
    # Define service categories and their keywords
    categories = {
        'Compute': ['ec2', 'lambda', 'fargate', 'compute', 'instance'],
        'Storage': ['s3', 'ebs', 'storage', 'glacier', 'efs'],
        'Database': ['rds', 'dynamodb', 'database', 'redshift', 'elasticache'],
        'Network': ['vpc', 'route53', 'cloudfront', 'network', 'direct connect'],
        'Security': ['iam', 'kms', 'security', 'waf', 'shield'],
        'Analytics': ['athena', 'quicksight', 'analytics', 'emr', 'kinesis'],
        'Management': ['cloudwatch', 'cloudtrail', 'management', 'config', 'organizations'],
        'Integration': ['sns', 'sqs', 'api gateway', 'integration', 'eventbridge'],
        'Developer Tools': ['codebuild', 'codepipeline', 'developer', 'tools', 'x-ray'],
        'Other': []  # Default category
    }
    
    # Check each category's keywords
    for category, keywords in categories.items():
        if any(keyword in service_name for keyword in keywords):
            return category
            
    return 'Other'  # Default category if no match found

def update_service_details_in_df(df: pd.DataFrame, db_path: str) -> Tuple[bool, pd.DataFrame]:
    """
    Update DataFrame with service_ids from service_details table.
    Returns: (success, updated_df)
    """
    try:
        # Create a copy of the DataFrame
        df_updated = df.copy()
        
        # Add service_id column if it doesn't exist
        if 'service_id' not in df_updated.columns:
            df_updated['service_id'] = None
            
        # Process each unique service name
        for service_name in df_updated['service_name'].unique():
            success, service_id, _ = get_or_create_service_id(service_name, db_path)
            if not success:
                print(f"Failed to get/create service_id for {service_name}")
                return False, df
                
            # Update service_id in DataFrame
            df_updated.loc[df_updated['service_name'] == service_name, 'service_id'] = service_id
            
        return True, df_updated
        
    except Exception as e:
        print(f"Error in update_service_details_in_df: {str(e)}")
        return False, df

if __name__ == "__main__":
    # This module is meant to be imported and used by other modules
    print("This module should be imported and used by other modules.") 