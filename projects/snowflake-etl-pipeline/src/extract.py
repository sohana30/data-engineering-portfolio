"""
Snowflake ETL Pipeline - Data Extraction Module

This module handles data extraction from various sources including:
- CSV files
- JSON files
- REST APIs
- Database connections
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional
import requests
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from multiple sources"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize DataExtractor
        
        Args:
            config: Configuration dictionary containing source settings
        """
        self.config = config
        logger.info("DataExtractor initialized")
    
    def extract_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error extracting CSV data: {str(e)}")
            raise
    
    def extract_json(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from JSON file
        
        Args:
            file_path: Path to JSON file
            **kwargs: Additional arguments for pd.read_json
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            logger.info(f"Extracting data from JSON: {file_path}")
            df = pd.read_json(file_path, **kwargs)
            logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error extracting JSON data: {str(e)}")
            raise
    
    def extract_from_api(
        self, 
        url: str, 
        method: str = 'GET',
        headers: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Extract data from REST API
        
        Args:
            url: API endpoint URL
            method: HTTP method (GET, POST, etc.)
            headers: Request headers
            params: Query parameters
            
        Returns:
            DataFrame containing the API response data
        """
        try:
            logger.info(f"Extracting data from API: {url}")
            
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            logger.info(f"Successfully extracted {len(df)} rows from API")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing API data: {str(e)}")
            raise
    
    def extract_multiple_files(self, directory: str, pattern: str = "*.csv") -> pd.DataFrame:
        """
        Extract and combine data from multiple files
        
        Args:
            directory: Directory containing files
            pattern: File pattern to match (e.g., "*.csv")
            
        Returns:
            Combined DataFrame from all matching files
        """
        try:
            logger.info(f"Extracting multiple files from {directory} with pattern {pattern}")
            
            path = Path(directory)
            files = list(path.glob(pattern))
            
            if not files:
                logger.warning(f"No files found matching pattern {pattern}")
                return pd.DataFrame()
            
            dfs = []
            for file in files:
                if file.suffix == '.csv':
                    df = self.extract_csv(str(file))
                elif file.suffix == '.json':
                    df = self.extract_json(str(file))
                else:
                    logger.warning(f"Unsupported file type: {file.suffix}")
                    continue
                
                dfs.append(df)
            
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Combined {len(files)} files into {len(combined_df)} total rows")
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Error extracting multiple files: {str(e)}")
            raise


def extract_csv_data(file_path: str) -> pd.DataFrame:
    """
    Convenience function to extract CSV data
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        DataFrame with extracted data
    """
    extractor = DataExtractor({})
    return extractor.extract_csv(file_path)


if __name__ == "__main__":
    # Example usage
    extractor = DataExtractor({})
    
    # Extract from CSV
    # df = extractor.extract_csv('data/raw/transactions.csv')
    # print(f"Extracted {len(df)} rows")
    
    print("Data extraction module loaded successfully")
