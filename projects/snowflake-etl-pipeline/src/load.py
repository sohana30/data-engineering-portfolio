"""
Snowflake ETL Pipeline - Data Loading Module

This module handles loading data into Snowflake Data Warehouse.
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Handles data loading operations to Snowflake"""
    
    def __init__(self):
        """Initialize Snowflake connection"""
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        }
        self.conn = None
        logger.info("SnowflakeLoader initialized")
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")
            self.conn = connect(**self.connection_params)
            logger.info("Successfully connected to Snowflake")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")
    
    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'append',
        create_table: bool = False
    ) -> bool:
        """
        Load DataFrame to Snowflake table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: Action if table exists ('append', 'replace', 'fail')
            create_table: Whether to create table if it doesn't exist
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading {len(df)} rows to table {table_name}")
            
            if not self.conn:
                self.connect()
            
            # Use Snowflake's optimized write_pandas function
            success, nchunks, nrows, _ = write_pandas(
                conn=self.conn,
                df=df,
                table_name=table_name,
                auto_create_table=create_table,
                overwrite=(if_exists == 'replace')
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows in {nchunks} chunks to {table_name}")
                return True
            else:
                logger.error(f"Failed to load data to {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        try:
            if not self.conn:
                self.connect()
            
            logger.info(f"Executing query: {query[:100]}...")
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetch_pandas_all()
            cursor.close()
            
            logger.info(f"Query returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def truncate_table(self, table_name: str):
        """
        Truncate a Snowflake table
        
        Args:
            table_name: Name of table to truncate
        """
        try:
            query = f"TRUNCATE TABLE {table_name}"
            logger.info(f"Truncating table {table_name}")
            self.execute_query(query)
            logger.info(f"Successfully truncated {table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table: {str(e)}")
            raise
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get row count of a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        try:
            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            result = self.execute_query(query)
            count = result['ROW_COUNT'].iloc[0]
            logger.info(f"Table {table_name} has {count} rows")
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {str(e)}")
            raise


def load_to_snowflake(
    df: pd.DataFrame, 
    table_name: str,
    if_exists: str = 'append'
) -> bool:
    """
    Convenience function to load data to Snowflake
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        if_exists: Action if table exists
        
    Returns:
        True if successful
    """
    loader = SnowflakeLoader()
    try:
        result = loader.load_dataframe(df, table_name, if_exists)
        return result
    finally:
        loader.disconnect()


if __name__ == "__main__":
    print("Snowflake loading module loaded successfully")
    
    # Example usage (commented out - requires credentials)
    # loader = SnowflakeLoader()
    # loader.connect()
    # 
    # # Create sample data
    # sample_df = pd.DataFrame({
    #     'ID': [1, 2, 3],
    #     'NAME': ['Alice', 'Bob', 'Charlie'],
    #     'AMOUNT': [100.50, 200.75, 150.00]
    # })
    # 
    # # Load to Snowflake
    # loader.load_dataframe(sample_df, 'TEST_TABLE', create_table=True)
    # 
    # loader.disconnect()
