from pyspark.sql import SparkSession, DataFrame
from typing import Optional

class DataLoader:
    """Module for loading data from Delta Lake with time travel support"""
    
    @staticmethod
    def load_data(table_path: str, version: Optional[int] = None) -> DataFrame:
        """Load data from a Delta table, optionally specifying a version
        
        Args:
            table_path: Path to the Delta table
            version: Optional version number (None for latest version)
            
        Returns:
            Spark DataFrame with the requested data
        """
        spark = SparkSession.builder.getOrCreate()
        
        try:
            # If version is specified, use time travel
            if version is not None:
                print(f"Loading version {version} from {table_path}")
                df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
            else:
                print(f"Loading latest version from {table_path}")
                df = spark.read.format("delta").load(table_path)
            
            # Cache the DataFrame for better performance
            df.cache()
            
            # Trigger an action to cache the data
            row_count = df.count()
            print(f"Loaded {row_count} rows")
            
            return df
        
        except Exception as e:
            error_msg = f"Error loading data from {table_path}: {str(e)}"
            raise Exception(error_msg)
    
    @staticmethod
    def get_latest_version(table_path: str) -> int:
        """Get the latest version number of a Delta table
        
        Args:
            table_path: Path to the Delta table
            
        Returns:
            Latest version number as an integer
        """
        spark = SparkSession.builder.getOrCreate()
        
        try:
            # Read Delta table history
            history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
            
            # Get the latest version
            latest_version = history.select("version").orderBy("version", ascending=False).first()[0]
            
            return latest_version
        
        except Exception as e:
            error_msg = f"Error getting latest version of {table_path}: {str(e)}"
            raise Exception(error_msg)
    
    @staticmethod
    def sample_data(df: DataFrame, sample_size: int = 100000, seed: int = 42) -> DataFrame:
        """Sample a DataFrame to a specific size
        
        Args:
            df: DataFrame to sample
            sample_size: Number of rows to sample (0 for no sampling)
            seed: Random seed for reproducibility
            
        Returns:
            Sampled DataFrame
        """
        if sample_size <= 0 or df.count() <= sample_size:
            return df
        
        # Calculate sampling fraction
        total_rows = df.count()
        fraction = min(1.0, sample_size / total_rows)
        
        # Sample the DataFrame
        sampled_df = df.sample(withReplacement=False, fraction=fraction, seed=seed)
        
        # Ensure we get exactly the sample size
        return sampled_df.limit(sample_size) 