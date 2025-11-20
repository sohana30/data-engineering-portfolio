"""
Snowflake ETL Pipeline - Data Transformation Module

This module handles data cleaning, validation, and enrichment operations.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation and cleaning operations"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize DataTransformer
        
        Args:
            config: Configuration dictionary for transformation rules
        """
        self.config = config or {}
        logger.info("DataTransformer initialized")
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the input DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting data cleaning for {len(df)} rows")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Remove duplicate rows
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.drop_duplicates()
        duplicates_removed = initial_count - len(cleaned_df)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows")
        
        # Strip whitespace from string columns
        string_columns = cleaned_df.select_dtypes(include=['object']).columns
        for col in string_columns:
            cleaned_df[col] = cleaned_df[col].str.strip()
        
        # Convert column names to uppercase (Snowflake convention)
        cleaned_df.columns = cleaned_df.columns.str.upper()
        
        logger.info(f"Data cleaning completed. Final row count: {len(cleaned_df)}")
        return cleaned_df
    
    def validate_data(self, df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
        """
        Validate data against specified rules
        
        Args:
            df: Input DataFrame
            rules: Dictionary of validation rules
            
        Returns:
            Validated DataFrame (invalid rows removed)
        """
        logger.info("Starting data validation")
        validated_df = df.copy()
        initial_count = len(validated_df)
        
        # Check for required columns
        if 'required_columns' in rules:
            missing_cols = set(rules['required_columns']) - set(validated_df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Remove rows with null values in critical columns
        if 'not_null_columns' in rules:
            for col in rules['not_null_columns']:
                validated_df = validated_df[validated_df[col].notna()]
        
        # Validate data types
        if 'data_types' in rules:
            for col, dtype in rules['data_types'].items():
                try:
                    validated_df[col] = validated_df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Could not convert {col} to {dtype}: {str(e)}")
        
        rows_removed = initial_count - len(validated_df)
        if rows_removed > 0:
            logger.warning(f"Removed {rows_removed} invalid rows during validation")
        
        logger.info(f"Validation completed. Valid rows: {len(validated_df)}")
        return validated_df
    
    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich data with calculated fields and metadata
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Starting data enrichment")
        enriched_df = df.copy()
        
        # Add processing timestamp
        enriched_df['PROCESSED_AT'] = datetime.now()
        
        # Add data quality score (example)
        enriched_df['DATA_QUALITY_SCORE'] = self._calculate_quality_score(enriched_df)
        
        logger.info("Data enrichment completed")
        return enriched_df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate a data quality score for each row
        
        Args:
            df: Input DataFrame
            
        Returns:
            Series with quality scores (0-100)
        """
        # Simple quality score based on completeness
        total_columns = len(df.columns)
        non_null_counts = df.notna().sum(axis=1)
        quality_scores = (non_null_counts / total_columns * 100).round(2)
        
        return quality_scores
    
    def aggregate_data(
        self, 
        df: pd.DataFrame, 
        group_by: List[str], 
        aggregations: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Aggregate data by specified columns
        
        Args:
            df: Input DataFrame
            group_by: List of columns to group by
            aggregations: Dictionary of {column: aggregation_function}
            
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Aggregating data by {group_by}")
        
        try:
            aggregated_df = df.groupby(group_by).agg(aggregations).reset_index()
            logger.info(f"Aggregation completed. Result has {len(aggregated_df)} rows")
            return aggregated_df
        except Exception as e:
            logger.error(f"Aggregation failed: {str(e)}")
            raise


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function for data cleaning
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    transformer = DataTransformer()
    return transformer.clean_data(df)


def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function for data enrichment
    
    Args:
        df: Input DataFrame
        
    Returns:
        Enriched DataFrame
    """
    transformer = DataTransformer()
    return transformer.enrich_data(df)


if __name__ == "__main__":
    # Example usage
    print("Data transformation module loaded successfully")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'transaction_id': [1, 2, 3, 2],  # Duplicate
        'amount': [100.50, 200.75, 150.00, 200.75],
        'customer_name': ['  John Doe  ', 'Jane Smith', 'Bob Johnson', 'Jane Smith'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-02']
    })
    
    transformer = DataTransformer()
    cleaned = transformer.clean_data(sample_data)
    print(f"\nCleaned data:\n{cleaned}")
