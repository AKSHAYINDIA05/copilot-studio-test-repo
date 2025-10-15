import json
import re
import os
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Union
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration data
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
    except FileNotFoundError:
        logger.warning(f"Configuration file {config_path} not found, returning empty config")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        return {}

def save_config(config: Dict[str, Any], config_path: str) -> bool:
    """
    Save configuration to JSON file
    
    Args:
        config: Configuration dictionary to save
        config_path: Path where to save configuration
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
            logger.info(f"Configuration saved to {config_path}")
            return True
    except Exception as e:
        logger.error(f"Failed to save configuration: {e}")
        return False

def format_currency(amount: float, currency: str = 'USD') -> str:
    """
    Format amount as currency string
    
    Args:
        amount: Numeric amount to format
        currency: Currency code (USD, EUR, INR, etc.)
        
    Returns:
        Formatted currency string
    """
    currency_symbols = {
        'USD': '$',
        'EUR': '€',
        'GBP': '£',
        'INR': '₹',
        'JPY': '¥'
    }
    
    symbol = currency_symbols.get(currency, currency)
    
    if amount < 0:
        return f"-{symbol}{abs(amount):,.2f}"
    else:
        return f"{symbol}{amount:,.2f}"

def validate_email(email: str) -> bool:
    """
    Validate email address format
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid email format, False otherwise
    """
    if not email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def calculate_percentage(value: float, total: float) -> float:
    """
    Calculate percentage of value relative to total
    
    Args:
        value: Value to calculate percentage for
        total: Total value (denominator)
        
    Returns:
        Percentage as float
        
    Raises:
        ValueError: If total is zero
    """
    if total == 0:
        raise ValueError("Total cannot be zero")
    
    return (value / total) * 100

def clean_text(text: str) -> str:
    """
    Clean and normalize text data
    
    Args:
        text: Input text to clean
        
    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    
    # Convert to lowercase
    cleaned = text.lower()
    
    # Replace newlines with spaces
    cleaned = re.sub(r'\n+', ' ', cleaned)
    
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Strip leading/trailing whitespace
    cleaned = cleaned.strip()
    
    return cleaned

def parse_date(date_string: str) -> Optional[datetime]:
    """
    Parse date string into datetime object
    
    Args:
        date_string: Date string in various formats
        
    Returns:
        datetime object if parsing successful, None otherwise
    """
    if not date_string:
        return None
    
    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%m/%d/%Y',
        '%d-%m-%Y',
        '%d/%m/%Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date string: {date_string}")
    return None

def retry_operation(operation, max_attempts: int = 3, delay: float = 1.0):
    """
    Retry operation with exponential backoff
    
    Args:
        operation: Function to retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
        
    Returns:
        Result of successful operation
        
    Raises:
        Last exception if all attempts fail
    """
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as e:
            if attempt == max_attempts - 1:
                logger.error(f"Operation failed after {max_attempts} attempts: {e}")
                raise
            
            wait_time = delay * (2 ** attempt)  # Exponential backoff
            logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
            time.sleep(wait_time)

def log_activity(message: str, level: str = "INFO") -> None:
    """
    Log activity with timestamp
    
    Args:
        message: Message to log
        level: Log level (INFO, WARNING, ERROR)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {level}: {message}"
    print(formatted_message)
    
    # Also log to logger
    if level.upper() == "ERROR":
        logger.error(message)
    elif level.upper() == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)

def get_file_size(file_path: str) -> Optional[int]:
    """
    Get file size in bytes
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in bytes, or None if file doesn't exist
    """
    try:
        return os.path.getsize(file_path)
    except FileNotFoundError:
        logger.warning(f"File not found: {file_path}")
        return None

def timing_decorator(func):
    """
    Decorator to measure function execution time
    
    Args:
        func: Function to measure
        
    Returns:
        Wrapped function that logs execution time
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
        return result
    
    return wrapper

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if division by zero
    
    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Value to return if denominator is zero
        
    Returns:
        Result of division or default value
    """
    if denominator == 0:
        logger.warning("Division by zero attempted, returning default value")
        return default
    
    return numerator / denominator

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    Flatten nested dictionary
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key for recursion
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
