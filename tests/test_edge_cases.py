import pytest
import pandas as pd
import sys
import os
from unittest.mock import patch
import tempfile

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_processor import validate_data, process_data, generate_report

class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_validate_large_dataset(self):
        """Test validation with large dataset"""
        # Create large dataset
        large_df = pd.DataFrame({
            'id': range(10000),
            'value': range(10000),
            'status': ['active'] * 10000
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_df.to_csv(f.name, index=False)
            result = validate_data(f.name)
            assert result is True
        os.unlink(f.name)
    
    def test_validate_with_special_characters(self):
        """Test validation with special characters in data"""
        special_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test "quotes"', 'Test,comma', 'Test\nnewline'],
            'value': [100, 200, 300]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            special_df.to_csv(f.name, index=False)
            result = validate_data(f.name)
            assert result is True
        os.unlink(f.name)
    
    @patch('sys.exit')
    def test_validate_corrupted_csv(self, mock_exit):
        """Test validation with corrupted CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("corrupted,csv\ndata,with,wrong,column,count\n")
            f.flush()
            
            validate_data(f.name)
            mock_exit.assert_called_once_with(1)
        os.unlink(f.name)
