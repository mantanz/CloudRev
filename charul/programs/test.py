import pandas as pd
import os
import re
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AWSTransformer:
    """Base class for AWS data transformation operations."""
    
    def __init__(self, input_path: str, output_path: Optional[str] = None):
        self.input_path = input_path
        self.output_path = output_path or self._generate_output_path()
        
    def _generate_output_path(self) -> str:
        """Generate default output path based on input path."""
        base_name = os.path.basename(self.input_path)
        return f"transformed_{base_name}"

class AccountWiseTransformer(AWSTransformer):
    """Handles transformation of account-wise monthly data."""
    
    def transform(self) -> pd.DataFrame:
        """Transform account-wise monthly data."""
        try:
            df = pd.read_csv(self.input_path, header=None)
            
            # Extract account information
            account_ids = df.iloc[0, :-1].tolist()
            account_names = df.iloc[1, :-1].tolist()
            total_spends = df.iloc[2, :-1].tolist()
            
            # Transform data
            transposed_data = []
            for i in range(3, len(df)):
                month = df.iloc[i, 0]
                for j in range(2, len(df.columns)):
                    account_id = account_ids[j - 1]
                    account_name = account_names[j - 1]
                    spend = df.iloc[i, j-1]
                    transposed_data.append([account_id, account_name, month, spend])
            
            # Create DataFrame
            transposed_df = pd.DataFrame(transposed_data, 
                                       columns=['account_id', 'account_name', 'month', 'spend'])
            
            # Clean data
            transposed_df['account_name'] = transposed_df['account_name'].str.replace(' ($)', '', regex=False)
            transposed_df['spend'] = transposed_df['spend'].fillna(0).replace('', 0).astype(float).round(4)
            
            # Validate data
            self._validate_totals(transposed_df, account_ids, total_spends)
            
            # Save results
            transposed_df[['account_id', 'month', 'spend']].to_csv(self.output_path, index=False)
            logger.info(f"Transformed data saved to {self.output_path}")
            
            return transposed_df
            
        except Exception as e:
            logger.error(f"Error transforming account-wise data: {str(e)}")
            raise
            
    def _validate_totals(self, df: pd.DataFrame, account_ids: List[str], total_spends: List[float]) -> None:
        """Validate that total spends match monthly spends for each account."""
        for account_id, total_spend in zip(account_ids[1:], total_spends[1:]):
            total_spend = pd.to_numeric(total_spend, errors='coerce')
            monthly_spend_sum = pd.to_numeric(
                df[df['account_id'] == account_id]['spend'], 
                errors='coerce'
            ).sum()
            
            if pd.isna(total_spend) or pd.isna(monthly_spend_sum):
                logger.warning(f"Validation Warning: Total spend or monthly spend for account {account_id} is not a valid number.")
            elif abs(total_spend - monthly_spend_sum) > 0.01:
                logger.error(f"Validation Error: Total spend for account {account_id} ({total_spend}) does not match the sum of monthly spends ({monthly_spend_sum}).")

class AccountServiceWiseTransformer(AWSTransformer):
    """Handles transformation of account service-wise data."""
    
    def __init__(self, input_path: str, output_path: Optional[str] = None):
        super().__init__(input_path, output_path)
        self.script_dir = Path(__file__).parent.resolve()
        self.input_dir = self.script_dir.parent / "data_files/APR2024_transposed_data"
        self.output_dir = self.script_dir.parent / "data_files/OP_APR2024_transposed_data"
        self.output_dir.mkdir(exist_ok=True)
        
    def transform(self) -> None:
        """Transform account service-wise monthly data."""
        try:
            mapping = self._load_account_mapping()
            file_map_df = self._map_files_to_account_ids(mapping)
            self._process_files(file_map_df)
            logger.info("Processing complete.")
            
        except Exception as e:
            logger.error(f"Error transforming account service-wise data: {str(e)}")
            raise
            
    def _load_account_mapping(self) -> Dict[str, str]:
        """Load account name to ID mapping."""
        df = pd.read_csv(self.input_dir / "Account Details.csv")
        df['Account Name'] = df['Account Name'].str.strip().str.lower()
        return dict(zip(df['Account Name'], df['Account ID']))
        
    def _map_files_to_account_ids(self, mapping: Dict[str, str]) -> pd.DataFrame:
        """Map files to account IDs."""
        mapping_rows = []
        for file in self.input_dir.glob("*.csv"):
            if file.name == "Account Details.csv":
                continue
            account_name = file.stem.strip().lower()
            account_id = mapping.get(account_name)
            if account_id:
                mapping_rows.append({'filename': file.name, 'account_id': account_id})
        return pd.DataFrame(mapping_rows)
        
    def _process_files(self, file_map_df: pd.DataFrame) -> None:
        """Process all files and merge results."""
        merged = []
        for _, row in file_map_df.iterrows():
            file = self.input_dir / row['filename']
            account_id = row['account_id']
            try:
                transposed = self._transpose_and_validate(file, account_id)
                out_name = f"transposed_ocl_{file.stem}.csv"
                transposed.to_csv(self.output_dir / out_name, index=False)
                merged.append(transposed)
            except Exception as e:
                logger.warning(f"Skipping {file.name}: {str(e)}")
                continue
                
        if merged:
            df_merged = pd.concat(merged, ignore_index=True)
            df_merged.to_csv(self.output_dir / "transposed_merged_ocl.csv", index=False)

class AccountServiceWiseDailyTransformer(AWSTransformer):
    """Handles transformation of account service-wise daily data."""
    
    def transform(self) -> pd.DataFrame:
        """Transform account service-wise daily data."""
        try:
            # Read and clean data
            df = pd.read_csv(self.input_path, dtype={'account_id': str})
            df.columns = [col.strip() for col in df.columns]
            
            if 'total_cost' in df.columns:
                df = df.drop(columns=['total_cost'])
                
            # Filter and process data
            df_filtered = self._filter_and_process_data(df)
            df_long = self._melt_data(df_filtered)
            
            # Save and validate
            df_long.to_csv(self.output_path, index=False)
            logger.info(f"Transposed file saved to {self.output_path}")
            
            self._validate_totals(df_filtered, df_long)
            
            return df_long
            
        except Exception as e:
            logger.error(f"Error transforming daily data: {str(e)}")
            raise
            
    def _filter_and_process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter and process the input data."""
        id_col = 'account_id'
        df_filtered = df[df[id_col].notna() & (df[id_col].astype(str).str.strip() != '')].copy()
        
        value_vars = [col for col in df_filtered.columns if col not in [id_col, 'service_name']]
        for col in value_vars:
            df_filtered[col] = df_filtered[col].astype(str).str.replace(',', '', regex=False)
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').fillna(0)
            
        return df_filtered
        
    def _melt_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Melt the data into long format."""
        value_vars = [col for col in df.columns if col not in ['account_id', 'service_name']]
        df_long = df.melt(
            id_vars=['account_id', 'service_name'],
            value_vars=value_vars,
            var_name='day',
            value_name='spend'
        )
        df_long['account_id'] = df_long['account_id'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        return df_long
        
    def _validate_totals(self, original_df: pd.DataFrame, transposed_df: pd.DataFrame) -> None:
        """Validate totals between original and transposed data."""
        value_vars = [col for col in original_df.columns if col not in ['account_id', 'service_name']]
        
        # Compare totals by day
        orig_totals_day = original_df[value_vars].sum()
        trans_totals_day = transposed_df.groupby('day')['spend'].sum()
        
        logger.info("\nTotals by day (original vs transposed):")
        for day in value_vars:
            orig = orig_totals_day[day]
            trans = trans_totals_day.get(day, 0)
            is_match = abs(orig - trans) < 0.01
            logger.info(f"{day}: original={orig:.2f}, transposed={trans:.2f}, match={is_match}")
            
        # Compare totals by account
        orig_totals_account = original_df.set_index('account_id')[value_vars].sum(axis=1)
        trans_totals_account = transposed_df.groupby('account_id')['spend'].sum()
        
        logger.info("\nTotals by account (showing mismatches):")
        mismatch = False
        for acc in orig_totals_account.index:
            orig = orig_totals_account.loc[acc].sum()
            trans = trans_totals_account.get(str(acc), 0)
            if abs(orig - trans) > 0.01:
                logger.info(f"Account {acc}: original={orig:.2f}, transposed={trans:.2f}")
                mismatch = True
        if not mismatch:
            logger.info("All account totals match.")

def main():
    """Main function to handle user input and execute transformations."""
    try:
        file_type = input("Enter the file type (accountwise monthly/account service wise monthly/account service wise daily): ").lower()
        
        if file_type == "accountwise monthly":
            file_path = input("Enter the input file path: ")
            transformer = AccountWiseTransformer(file_path)
            transformer.transform()
            
        elif file_type == "account service wise monthly":
            file_path = input("Enter the input file path: ")
            transformer = AccountServiceWiseTransformer(file_path)
            transformer.transform()
            
        elif file_type == "account service wise daily":
            input_file = '../data_files/Daily AWS Service Wise Cost - April 1-22 - Daily AWS Service Wise Cost - April 1-22.csv'
            output_file = '../data_files/Demo_Daily AWS Service Wise Cost - April 1-22.csv'
            transformer = AccountServiceWiseDailyTransformer(input_file, output_file)
            transformer.transform()
            
        else:
            logger.error("Invalid file type. Please choose from: accountwise monthly, account service wise monthly, account service wise daily")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()



