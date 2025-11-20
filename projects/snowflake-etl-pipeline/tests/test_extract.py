"""
Unit tests for the extract module
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from extract import DataExtractor


class TestDataExtractor:
    """Test cases for DataExtractor class"""
    
    @pytest.fixture
    def extractor(self):
        """Create a DataExtractor instance"""
        return DataExtractor({})
    
    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a temporary CSV file for testing"""
        csv_file = tmp_path / "test_data.csv"
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        df.to_csv(csv_file, index=False)
        return str(csv_file)
    
    def test_extract_csv_success(self, extractor, sample_csv):
        """Test successful CSV extraction"""
        df = extractor.extract_csv(sample_csv)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'value']
    
    def test_extract_csv_file_not_found(self, extractor):
        """Test CSV extraction with non-existent file"""
        with pytest.raises(FileNotFoundError):
            extractor.extract_csv('nonexistent.csv')
    
    def test_extract_json_success(self, extractor, tmp_path):
        """Test successful JSON extraction"""
        json_file = tmp_path / "test_data.json"
        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob']
        })
        df.to_json(json_file, orient='records')
        
        result = extractor.extract_json(str(json_file))
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
