import pandas as pd
import sys
import argparse

def validate_data(file_path):
    """Validate CSV data integrity"""
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded {len(df)} rows from {file_path}")
        
        # Basic validation
        if df.empty:
            raise ValueError("Data file is empty")
        
        if df.isnull().sum().sum() > len(df) * 0.1:
            raise ValueError("Too many missing values")
        
        print("Data validation passed")
        return True
        
    except Exception as e:
        print(f"Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--validate', required=True, help='File to validate')
    args = parser.parse_args()
    
    validate_data(args.validate)
