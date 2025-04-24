from pyspark.sql import SparkSession, DataFrame
import sys
from typing import Dict, List, Tuple, Optional
import time
import math
import datetime
import json

from column_analyzer import ColumnAnalyzer
from data_loader import DataLoader
from numerical_analyzer import NumericalAnalyzer
from categorical_analyzer import CategoricalAnalyzer
from result_handler import ResultHandler
from correlation_analyzer import CorrelationAnalyzer
from config_manager import ConfigManager
from group_analyzer import GroupAnalyzer

class DataDriftDetector:
    """A simple, automatic data drift detector for Delta tables"""
    
    @staticmethod
    def detect_drift(
        config_path: str,
        output_table: str,
        analyze_correlations: bool = True,
        adaptive_thresholds: bool = True,
        batch_size: int = 50
    ) -> DataFrame:
        """Detect data drift between two versions of a table
        
        Args:
            config_path: Path to configuration file (JSON format) in DBFS
            output_table: Delta table path to write results
            analyze_correlations: Whether to analyze correlations between columns
            adaptive_thresholds: Whether to adjust thresholds based on data size
            batch_size: Number of columns to process in each batch to prevent memory issues
            
        Returns:
            DataFrame with drift detection results
        """
        # Start timing
        start_time = datetime.datetime.now()
        
        print(f"Starting drift detection at {start_time}")
        
        # Load configuration
        config = ConfigManager.load_config_and_defaults(config_path)
        print(f"Loaded configuration from {config_path}")
        
        # Load datasets
        print(f"Loading reference data (version {config['reference_version']})")
        df_ref = DataLoader.load_data(config["table_path"], config["reference_version"])
        
        print(f"Loading current data (version {config['current_version']})")
        df_curr = DataLoader.load_data(config["table_path"], config["current_version"])
        
        # Check for schema drift
        schema_diffs = DataDriftDetector._analyze_schema_drift(df_ref, df_curr)
        if schema_diffs:
            print(f"WARNING: Schema differences detected between versions:")
            for diff in schema_diffs:
                print(f"  - {diff}")
                
        # Get common columns only
        ref_cols = set(df_ref.columns)
        curr_cols = set(df_curr.columns)
        common_cols = list(ref_cols.intersection(curr_cols))
        
        # Infer column types
        column_types = ColumnAnalyzer.infer_column_types(df_curr, config)
        print(f"Inferred types for {len(column_types)} columns")
        
        # Apply adaptive thresholds if requested
        if adaptive_thresholds:
            ref_count = df_ref.count()
            curr_count = df_curr.count()
            max_count = max(ref_count, curr_count)
            
            # Apply row count based threshold adjustments
            if max_count < 1000:
                # For small datasets, be more lenient
                print(f"Small dataset detected ({max_count} rows). Adjusting thresholds.")
                config["thresholds"]["numerical"]["std_dev_diff"] *= 1.5
                config["thresholds"]["numerical"]["mean_diff"] *= 1.5
                config["thresholds"]["categorical"]["distribution_diff"] *= 1.5
            elif max_count > 10000000:
                # For very large datasets, be more strict
                print(f"Very large dataset detected ({max_count} rows). Adjusting thresholds.")
                config["thresholds"]["numerical"]["std_dev_diff"] *= 0.7
                config["thresholds"]["numerical"]["mean_diff"] *= 0.7
                config["thresholds"]["categorical"]["distribution_diff"] *= 0.7
        
        # Collect all results
        all_results = []
        
        # Split columns into batches to avoid memory issues
        numerical_columns = [col for col in common_cols if column_types.get(col) == "numerical"]
        categorical_columns = [col for col in common_cols if column_types.get(col) == "categorical"]
        
        # Process numerical columns in batches
        print(f"Analyzing {len(numerical_columns)} numerical columns")
        for i in range(0, len(numerical_columns), batch_size):
            batch = numerical_columns[i:i+batch_size]
            print(f"Processing numerical batch {i//batch_size + 1}/{math.ceil(len(numerical_columns)/batch_size)}")
            numerical_results = NumericalAnalyzer.analyze_numerical_columns(
                df_ref, df_curr, batch, config
            )
            all_results.extend(numerical_results)
        
        # Process categorical columns in batches
        print(f"Analyzing {len(categorical_columns)} categorical columns")
        for i in range(0, len(categorical_columns), batch_size):
            batch = categorical_columns[i:i+batch_size]
            print(f"Processing categorical batch {i//batch_size + 1}/{math.ceil(len(categorical_columns)/batch_size)}")
            categorical_results = CategoricalAnalyzer.analyze_categorical_columns(
                df_ref, df_curr, batch, config
            )
            all_results.extend(categorical_results)
            
        # Run correlation analysis if requested and if there are enough numerical columns
        if analyze_correlations and len(numerical_columns) >= 2:
            print(f"Analyzing correlations between {len(numerical_columns)} numerical columns")
            correlation_results = CorrelationAnalyzer.analyze_correlations(
                df_ref, df_curr, numerical_columns
            )
            all_results.extend(correlation_results)
        
        # Run dimensional analysis if dimensions are specified
        if "dimensions" in config and config["dimensions"]:
            print(f"Running dimensional analysis for {len(config['dimensions'])} dimensions")
            for dimension in config["dimensions"]:
                dimension_id = dimension["id"]
                dimension_col = dimension["column"]
                dimension_values = dimension.get("values", [])
                
                print(f"Analyzing dimension: {dimension_id} (column: {dimension_col})")
                dimension_results = GroupAnalyzer.analyze_grouped_data(
                    df_ref, df_curr, dimension_col, dimension_values, 
                    numerical_columns, categorical_columns, config
                )
                all_results.extend(dimension_results)
        
        # Add schema drift results
        for diff in schema_diffs:
            all_results.append({
                "column_name": diff.get("column", "schema"),
                "column_type": "schema",
                "dimension_id": "all",
                "drift_score": 1.0,
                "metrics": diff
            })
            
        # Create result DataFrame
        spark = SparkSession.builder.getOrCreate()
        result_df = spark.createDataFrame(all_results)
        
        # Add metadata
        result_df = result_df.withColumn("detection_time", 
                                        spark.sql("current_timestamp()"))
        result_df = result_df.withColumn("config_path", 
                                        spark.sql(f"lit('{config_path}')"))
        result_df = result_df.withColumn("reference_version", 
                                        spark.sql(f"lit({config['reference_version']})"))
        result_df = result_df.withColumn("current_version", 
                                        spark.sql(f"lit({config['current_version']})"))
        
        # Write results to Delta
        if output_table:
            print(f"Writing results to {output_table}")
            result_df.write.format("delta").mode("append").save(output_table)
        
        # Log execution time
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Drift detection completed in {duration:.2f} seconds")
        
        return result_df
    
    @staticmethod
    def _analyze_schema_drift(df_ref: DataFrame, df_curr: DataFrame) -> List[Dict[str, Any]]:
        """Analyze schema differences between reference and current DataFrames
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            
        Returns:
            List of schema differences
        """
        differences = []
        
        # Get columns and data types
        ref_cols = set(df_ref.columns)
        curr_cols = set(df_curr.columns)
        
        # Find added columns
        added_cols = curr_cols - ref_cols
        for col in added_cols:
            differences.append({
                "column": col,
                "change_type": "added",
                "details": f"Column added in current version"
            })
            
        # Find removed columns
        removed_cols = ref_cols - curr_cols
        for col in removed_cols:
            differences.append({
                "column": col,
                "change_type": "removed",
                "details": f"Column removed in current version"
            })
            
        # Check for data type changes in common columns
        common_cols = ref_cols.intersection(curr_cols)
        ref_types = {f.name: f.dataType.simpleString() for f in df_ref.schema.fields}
        curr_types = {f.name: f.dataType.simpleString() for f in df_curr.schema.fields}
        
        for col in common_cols:
            if ref_types[col] != curr_types[col]:
                differences.append({
                    "column": col,
                    "change_type": "type_change",
                    "details": f"Data type changed from {ref_types[col]} to {curr_types[col]}"
                })
                
        return differences

def main():
    """Command line interface"""
    if len(sys.argv) < 2:
        print("Usage: python main.py <config_path> <output_table>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    output_table = sys.argv[2]
    
    DataDriftDetector.detect_drift(config_path, output_table)

if __name__ == "__main__":
    main() 