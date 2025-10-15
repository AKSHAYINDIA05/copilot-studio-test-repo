import pandas as pd
import sys
import argparse
from datetime import datetime
from typing import Dict, Any
from utils import load_config, log_activity, timing_decorator, safe_divide

@timing_decorator
def validate_data(file_path: str) -> bool:
    """Validate CSV data integrity with comprehensive checks"""
    try:
        log_activity(f"Starting validation for {file_path}")
        df = pd.read_csv(file_path)
        
        # Basic validation
        if df.empty:
            raise ValueError("Data file is empty")
        
        log_activity(f"Loaded {len(df)} rows from {file_path}")
        
        # Check missing values
        missing_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns))
        if missing_percentage > 0.1:  # 10% threshold
            raise ValueError(f"Too many missing values: {missing_percentage:.2%}")
        
        # Check for duplicate IDs if ID column exists
        if 'id' in df.columns:
            duplicates = df['id'].duplicated().sum()
            if duplicates > 0:
                log_activity(f"Warning: Found {duplicates} duplicate IDs", "WARNING")
        
        log_activity("Data validation passed")
        return True
        
    except Exception as e:
        log_activity(f"Validation failed: {e}", "ERROR")
        sys.exit(1)

@timing_decorator
def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and transform data"""
    log_activity("Starting data processing")
    
    # Add processing timestamp
    df['processed_date'] = datetime.now().isoformat()
    
    # Categorize values if value column exists
    if 'value' in df.columns:
        df['value_category'] = df['value'].apply(lambda x: 
            'high' if x > 200 else 'medium' if x > 100 else 'low')
    
    # Add row number
    df['row_number'] = range(1, len(df) + 1)
    
    log_activity(f"Processing completed for {len(df)} records")
    return df

def generate_report(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summary report from processed data"""
    log_activity("Generating data report")
    
    report = {
        'total_records': len(df),
        'processing_date': datetime.now().isoformat(),
        'columns': list(df.columns)
    }
    
    # Status analysis if status column exists
    if 'status' in df.columns:
        status_counts = df['status'].value_counts().to_dict()
        report['status_breakdown'] = status_counts
        report['active_records'] = status_counts.get('active', 0)
    
    # Value analysis if value column exists
    if 'value' in df.columns:
        report['average_value'] = float(df['value'].mean())
        report['max_value'] = float(df['value'].max())
        report['min_value'] = float(df['value'].min())
        report['total_value'] = float(df['value'].sum())
    
    log_activity("Report generation completed")
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data Processing and Validation Tool')
    parser.add_argument('--validate', required=True, help='File to validate')
    parser.add_argument('--config', default='data/config.json', help='Configuration file path')
    parser.add_argument('--output', help='Output file path for processed data')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Validate data
    if validate_data(args.validate):
        # Load and process data
        df = pd.read_csv(args.validate)
        processed_df = process_data(df)
        
        # Generate report
        report = generate_report(processed_df)
        
        # Save processed data if output specified
        if args.output:
            processed_df.to_csv(args.output, index=False)
            log_activity(f"Processed data saved to {args.output}")
        
        # Print report summary
        print(f"\n--- DATA PROCESSING REPORT ---")
        print(f"Total Records: {report['total_records']}")
        if 'active_records' in report:
            print(f"Active Records: {report['active_records']}")
        if 'average_value' in report:
            print(f"Average Value: {report['average_value']:.2f}")
        print(f"Processing Date: {report['processing_date']}")
