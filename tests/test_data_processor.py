import pytest
import pandas as pd
import os
import sys
from unittest.mock import patch, mock_open
import tempfile

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_processor import validate_data, process_data, generate_report
from utils import load_config, format_currency, validate_email

class TestDataProcessor:
    
    @pytest.fixture
    def sample_data(self):
        """Create sample DataFrame for testing"""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Record A', 'Record B', 'Record C', 'Record D', 'Record E'],
            'value': [100, 200, 150, 300, 250],
            'status': ['active', 'active', 'inactive', 'active', 'pending'],
            'email': ['test@example.com', 'user@test.com', 'invalid-email', 'admin@company.org', 'support@help.net']
        })
    
    @pytest.fixture
    def empty_data(self):
        """Create empty DataFrame for testing edge cases"""
        return pd.DataFrame()
    
    @pytest.fixture
    def config_data(self):
        """Sample configuration for testing"""
        return {
            'validation': {
                'max_missing_percentage': 0.1,
                'required_columns': ['id', 'name', 'value'],
                'value_ranges': {'value': {'min': 0, 'max': 1000}}
            },
            'processing': {
                'output_format': 'csv',
                'include_summary': True
            }
        }

    def test_validate_data_success(self, sample_data):
        """Test successful data validation"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_data.to_csv(f.name, index=False)
            result = validate_data(f.name)
            assert result is True
        os.unlink(f.name)
    
    def test_validate_data_empty_file(self, empty_data):
        """Test validation with empty data file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            empty_data.to_csv(f.name, index=False)
            with pytest.raises(SystemExit):
                validate_data(f.name)
        os.unlink(f.name)
    
    def test_validate_data_missing_file(self):
        """Test validation with non-existent file"""
        with pytest.raises(SystemExit):
            validate_data('nonexistent_file.csv')
    
    @pytest.mark.parametrize("missing_percentage,should_pass", [
        (0.05, True),   # 5% missing - should pass
        (0.15, False),  # 15% missing - should fail
        (0.20, False),  # 20% missing - should fail
    ])
    def test_validate_missing_data_threshold(self, missing_percentage, should_pass):
        """Test validation with different missing data percentages"""
        # Create data with specified missing percentage
        df = pd.DataFrame({
            'col1': [1, 2, 3, 4, 5] * 20,  # 100 rows
            'col2': ['A', 'B', 'C', 'D', 'E'] * 20
        })
        
        # Add missing values
        missing_count = int(len(df) * missing_percentage)
        df.iloc[:missing_count, 0] = None
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            if should_pass:
                result = validate_data(f.name)
                assert result is True
            else:
                with pytest.raises(SystemExit):
                    validate_data(f.name)
        os.unlink(f.name)

    def test_process_data_transformation(self, sample_data):
        """Test data processing and transformation"""
        result = process_data(sample_data)
        
        # Check that processed data has expected columns
        assert 'processed_date' in result.columns
        assert 'value_category' in result.columns
        assert len(result) == len(sample_data)
        
        # Check value categorization
        high_value_count = len(result[result['value_category'] == 'high'])
        assert high_value_count > 0
    
    def test_generate_report(self, sample_data):
        """Test report generation"""
        report = generate_report(sample_data)
        
        assert 'total_records' in report
        assert 'active_records' in report
        assert 'average_value' in report
        assert report['total_records'] == 5
        assert report['active_records'] == 3  # 3 active records in sample data
        assert isinstance(report['average_value'], float)

    @patch('builtins.open', new_callable=mock_open, read_data='invalid,csv,data')
    def test_validate_data_invalid_csv(self, mock_file):
        """Test validation with invalid CSV format"""
        with pytest.raises(SystemExit):
            validate_data('invalid.csv')

class TestDataProcessorIntegration:
    """Integration tests for complete data processing workflow"""
    
    def test_full_pipeline(self):
        """Test complete data processing pipeline"""
        # Create test data
        test_data = pd.DataFrame({
            'id': range(1, 11),
            'name': [f'Record {i}' for i in range(1, 11)],
            'value': [i * 10 for i in range(1, 11)],
            'status': ['active'] * 7 + ['inactive'] * 3
        })
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            
            # Test validation
            assert validate_data(f.name) is True
            
            # Test processing
            processed = process_data(test_data)
            assert len(processed) == 10
            
            # Test reporting
            report = generate_report(processed)
            assert report['total_records'] == 10
            assert report['active_records'] == 7
            
        os.unlink(f.name)
