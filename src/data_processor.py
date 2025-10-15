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
        
        # Check missing values - FIXED: Calculate across all cells
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        missing_percentage = missing_cells / total_cells if total_cells > 0 else 0
        
        log_activity(f"Missing data percentage: {missing_percentage:.2%}")
        
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
