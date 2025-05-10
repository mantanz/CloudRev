# CloudRev - Cloud Revenue Management System

## Overview
CloudRev is a comprehensive cloud revenue management system designed to process and manage cloud spending data across multiple entities. The system handles various types of data including Annual Operating Plans (AOP), monthly spend data, and service-level spending information.

## Features
- **Multi-Entity Support**: Process data for multiple entities (OCL, PPSL, PIBPL, Nearbuy, PML, Creditmate, PaiPai)
- **Data Processing Types**:
  - Annual Operating Plan (AOP) processing
  - Account-level spend processing
  - Service-level spend processing
- **Automated Account Management**:
  - Automatic account creation and updates
  - HOD (Head of Department) management
  - Entity-based account organization
- **Data Validation**:
  - Pre-transpose validation
  - Post-transpose validation
  - Data integrity checks
- **Reporting**:
  - New accounts tracking
  - HOD entries tracking
  - Transformed data output

## Prerequisites
- Python 3.8 or higher
- SQLite3
- Required Python packages (install using `pip install -r requirements.txt`):
  - pandas
  - numpy
  - sqlite3

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/mantanz/CloudRev.git
   cd CloudRev
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the database:
   ```bash
   cd final/src
   python create_db.py
   ```

## Usage
1. Navigate to the source directory:
   ```bash
   cd final/src
   ```

2. Run the main script:
   ```bash
   python main.py
   ```

3. Follow the interactive prompts:
   - Select file type (AOP/Spend)
   - Choose entity
   - Select file type (for Spend)
   - Provide input file path

## File Structure
```
CloudRev/
├── final/
│   ├── src/
│   │   ├── main.py
│   │   ├── transform_aop.py
│   │   ├── transform_spend.py
│   │   ├── update_account_details.py
│   │   └── ...
│   ├── data_files/
│   │   ├── Spend/
│   │   ├── new_accounts/
│   │   └── new_hod_entries/
│   └── sqlite/
│       └── mydatabase.db
└── README.md
```

## Input File Formats

### AOP Files
- CSV format
- Contains annual operating plan data
- Required columns: account_id, account_name, monthly_budget

### Spend Files
- CSV format
- Three types supported:
  1. Account Service Daily
  2. Account Service Monthly
  3. Account Monthly
- Contains spend data with account details and monthly costs

## Output Files
- Transformed data files: `transformed_{original_filename}.csv`
- New accounts file: `new_accounts_{timestamp}.csv`
- New HOD entries file: `new_hod_entries_{timestamp}.csv`

## Database Schema
- `account_details`: Stores account information
- `hod_details`: Stores HOD information
- `as_acct_monthly`: Stores monthly spend data
- `aop_budget`: Stores AOP budget data

## Error Handling
- File validation errors
- Data integrity checks
- Database operation errors
- Transformation errors

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Copyright and License
© 2024 CloudRev. All Rights Reserved.

This software and its documentation are proprietary and confidential. Unauthorized copying, distribution, or use of this software, via any medium, is strictly prohibited.

### Usage Restrictions
- This code is not allowed to be reused for any corporate, individual, or student projects without prior written permission from the copyright holder.
- Any unauthorized use, reproduction, or distribution of this software is strictly prohibited.
- For permission to use this code, please contact the repository owner.

## Support
For support, please create an issue in the repository or contact the repository owner.

## Version
Current Version: 1.0.0
