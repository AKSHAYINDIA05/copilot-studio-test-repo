import pytest
import json
import os
import sys
from unittest.mock import patch, mock_open
from datetime import datetime, timedelta

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils import (
    load_config, save_config, format_currency, validate_email, 
    calculate_percentage, retry_operation, log_activity,
    clean_text, parse_date, get_file_size
)

class TestConfigurationUtils:
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration data for testing"""
        return {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'test_db'
            },
            'api': {
                'timeout': 30,
                'retries': 3,
                'base_url': 'https://api.example.com'
            },
            'processing': {
                'batch_size': 1000,
                'enable_logging': True
            }
        }
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_config_success(self, mock_json_load, mock_file, sample_config):
        """Test successful configuration loading"""
        mock_json_load.return_value = sample_config
        
        result = load_config('config.json')
        
        assert result == sample_config
        mock_file.assert_called_once_with('config.json', 'r')
        mock_json_load.assert_called_once()
    
    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_file):
        """Test configuration loading with missing file"""
        result = load_config('missing_config.json')
        assert result == {}
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    def test_save_config_success(self, mock_json_dump, mock_file, sample_config):
        """Test successful configuration saving"""
        save_config(sample_config, 'output_config.json')
        
        mock_file.assert_called_once_with('output_config.json', 'w')
        mock_json_dump.assert_called_once_with(sample_config, mock_file.return_value, indent=2)

class TestFormattingUtils:
    
    @pytest.mark.parametrize("amount,currency,expected", [
        (1000, 'USD', '$1,000.00'),
        (1500.50, 'EUR', '€1,500.50'),
        (2000, 'INR', '₹2,000.00'),
        (0, 'USD', '$0.00'),
        (-500, 'USD', '-$500.00'),
    ])
    def test_format_currency(self, amount, currency, expected):
        """Test currency formatting with different amounts and currencies"""
        result = format_currency(amount, currency)
        assert result == expected
    
    @pytest.mark.parametrize("email,expected", [
        ('test@example.com', True),
        ('user.name@domain.co.uk', True),
        ('invalid-email', False),
        ('@domain.com', False),
        ('user@', False),
        ('', False),
        ('user@domain', False),  # Missing TLD
    ])
    def test_validate_email(self, email, expected):
        """Test email validation with various formats"""
        result = validate_email(email)
        assert result == expected
    
    @pytest.mark.parametrize("value,total,expected", [
        (25, 100, 25.0),
        (50, 200, 25.0),
        (0, 100, 0.0),
        (100, 100, 100.0),
        (75, 300, 25.0),
    ])
    def test_calculate_percentage(self, value, total, expected):
        """Test percentage calculation"""
        result = calculate_percentage(value, total)
        assert result == expected
    
    def test_calculate_percentage_zero_total(self):
        """Test percentage calculation with zero total"""
        with pytest.raises(ValueError, match="Total cannot be zero"):
            calculate_percentage(50, 0)

class TestTextProcessingUtils:
    
    @pytest.mark.parametrize("input_text,expected", [
        ('  Hello World  ', 'Hello World'),
        ('Text\nWith\nNewlines', 'Text With Newlines'),
        ('Multiple   Spaces', 'Multiple Spaces'),
        ('UPPERCASE', 'uppercase'),
        ('MiXeD cAsE', 'mixed case'),
        ('', ''),
    ])
    def test_clean_text(self, input_text, expected):
        """Test text cleaning functionality"""
        result = clean_text(input_text)
        assert result == expected
    
    @pytest.mark.parametrize("date_string,expected_valid", [
        ('2024-01-15', True),
        ('2024/01/15', True),
        ('01-15-2024', True),
        ('invalid-date', False),
        ('2024-13-01', False),  # Invalid month
        ('', False),
    ])
    def test_parse_date(self, date_string, expected_valid):
        """Test date parsing with various formats"""
        result = parse_date(date_string)
        
        if expected_valid:
            assert isinstance(result, datetime)
        else:
            assert result is None

class TestRetryUtils:
    
    def test_retry_operation_success(self):
        """Test retry mechanism with successful operation"""
        call_count = 0
        
        def mock_operation():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = retry_operation(mock_operation, max_attempts=3)
        assert result == "success"
        assert call_count == 1
    
    def test_retry_operation_eventual_success(self):
        """Test retry mechanism with eventual success"""
        call_count = 0
        
        def mock_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary failure")
            return "success"
        
        result = retry_operation(mock_operation, max_attempts=3)
        assert result == "success"
        assert call_count == 3
    
    def test_retry_operation_max_attempts_exceeded(self):
        """Test retry mechanism when max attempts are exceeded"""
        def mock_operation():
            raise Exception("Persistent failure")
        
        with pytest.raises(Exception, match="Persistent failure"):
            retry_operation(mock_operation, max_attempts=2)

class TestFileUtils:
    
    @patch('os.path.getsize')
    def test_get_file_size(self, mock_getsize):
        """Test file size calculation"""
        mock_getsize.return_value = 1024
        
        result = get_file_size('test_file.txt')
        assert result == 1024
        mock_getsize.assert_called_once_with('test_file.txt')
    
    @patch('os.path.getsize', side_effect=FileNotFoundError)
    def test_get_file_size_not_found(self, mock_getsize):
        """Test file size calculation with missing file"""
        result = get_file_size('missing_file.txt')
        assert result is None

class TestLoggingUtils:
    
    @patch('builtins.print')
    def test_log_activity(self, mock_print):
        """Test activity logging"""
        log_activity("Test message", "INFO")
        
        # Verify that print was called with timestamp and message
        args = mock_print.call_args[0][0]
        assert "INFO" in args
        assert "Test message" in args
        assert datetime.now().strftime("%Y-%m-%d") in args

