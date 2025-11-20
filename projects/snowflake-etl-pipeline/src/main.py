"""
Snowflake ETL Pipeline - Main Orchestration

This is the main entry point for the ETL pipeline.
Orchestrates the Extract, Transform, and Load operations.
"""

import logging
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from extract import DataExtractor
from transform import DataTransformer
from load import SnowflakeLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, config: dict = None):
        """
        Initialize ETL Pipeline
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = SnowflakeLoader()
        logger.info("ETL Pipeline initialized")
    
    def run(
        self, 
        source_path: str, 
        target_table: str,
        source_type: str = 'csv'
    ):
        """
        Run the complete ETL pipeline
        
        Args:
            source_path: Path to source data
            target_table: Target Snowflake table name
            source_type: Type of source ('csv', 'json', 'api')
        """
        try:
            logger.info("=" * 50)
            logger.info("Starting ETL Pipeline")
            logger.info("=" * 50)
            
            # EXTRACT
            logger.info("STEP 1: EXTRACT")
            if source_type == 'csv':
                raw_data = self.extractor.extract_csv(source_path)
            elif source_type == 'json':
                raw_data = self.extractor.extract_json(source_path)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            logger.info(f"Extracted {len(raw_data)} rows")
            
            # TRANSFORM
            logger.info("STEP 2: TRANSFORM")
            cleaned_data = self.transformer.clean_data(raw_data)
            
            # Validation rules
            validation_rules = {
                'not_null_columns': self.config.get('required_columns', [])
            }
            
            if validation_rules['not_null_columns']:
                validated_data = self.transformer.validate_data(
                    cleaned_data, 
                    validation_rules
                )
            else:
                validated_data = cleaned_data
            
            enriched_data = self.transformer.enrich_data(validated_data)
            logger.info(f"Transformed data: {len(enriched_data)} rows")
            
            # LOAD
            logger.info("STEP 3: LOAD")
            self.loader.connect()
            
            # Get initial row count
            try:
                initial_count = self.loader.get_row_count(target_table)
            except:
                initial_count = 0
                logger.info(f"Table {target_table} may not exist yet")
            
            # Load data
            success = self.loader.load_dataframe(
                enriched_data,
                target_table,
                if_exists='append',
                create_table=True
            )
            
            if success:
                # Verify load
                final_count = self.loader.get_row_count(target_table)
                rows_added = final_count - initial_count
                
                logger.info("=" * 50)
                logger.info("ETL Pipeline completed successfully!")
                logger.info(f"Rows processed: {len(enriched_data)}")
                logger.info(f"Rows added to {target_table}: {rows_added}")
                logger.info("=" * 50)
            else:
                logger.error("ETL Pipeline failed during load step")
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
        
        finally:
            self.loader.disconnect()


def main():
    """Main execution function"""
    
    # Configuration
    config = {
        'required_columns': []  # Add required column names here
    }
    
    # Initialize pipeline
    pipeline = ETLPipeline(config)
    
    # Example: Run pipeline with sample data
    # Uncomment and modify for your use case
    
    # pipeline.run(
    #     source_path='data/raw/transactions.csv',
    #     target_table='RETAIL_TRANSACTIONS',
    #     source_type='csv'
    # )
    
    logger.info("Pipeline ready. Configure source and target, then run.")


if __name__ == "__main__":
    main()
