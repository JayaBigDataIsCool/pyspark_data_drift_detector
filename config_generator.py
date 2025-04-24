from pyspark.sql import SparkSession, DataFrame
import json
import os
from typing import Dict, Any, Optional

def generate_sample_config(
    table_path: str,
    reference_version: int,
    current_version: int,
    output_path: str,
    output_table: Optional[str] = None,
    profile: str = "standard"
) -> None:
    """Generate a sample configuration file for data drift detection
    
    Args:
        table_path: Path to the Delta table to analyze
        reference_version: Version number for reference data
        current_version: Version number for current data
        output_path: Path to save the configuration file
        output_table: Optional path to Delta table for saving results
        profile: Analysis profile (summary, standard, deep_dive)
    """
    # Create sample configuration
    config = {
        "table_path": table_path,
        "reference_version": reference_version,
        "current_version": current_version,
        "profile": profile,
        "analyze_distributions": True,
        "analyze_correlations": True,
        "analyze_groups": True,
        "analyze_feature_importance": False,
        "target_column": None,  # Set this to enable feature importance analysis
        "include_columns": [],  # Empty list means analyze all columns
        "exclude_columns": [],  # Columns to exclude from analysis
        "custom_column_types": {},  # Override automatic type inference, e.g. {"col1": "numerical"}
        "group_columns": [],  # Columns to use for group analysis, empty means use top categorical columns
        "sample_size": 100000,  # Max rows to analyze, use 0 for no sampling
        "thresholds": {
            "summary": {
                "numerical": {
                    "mean_threshold": 0.1,
                    "median_threshold": 0.1,
                    "std_threshold": 0.2,
                    "iqr_threshold": 0.2,
                    "null_threshold": 0.01
                },
                "categorical": {
                    "category_threshold": 0.05,
                    "chi_square_pvalue": 0.01,
                    "null_threshold": 0.01
                },
                "correlation_threshold": 0.7,
                "correlation_change_threshold": 0.3,
                "js_distance_threshold": 0.1,
                "rare_value_threshold": 0.01,
                "analyze_distributions": False,
                "detect_rare_values": False
            },
            "standard": {
                "numerical": {
                    "mean_threshold": 0.05,
                    "median_threshold": 0.05,
                    "std_threshold": 0.1,
                    "iqr_threshold": 0.1,
                    "null_threshold": 0.005
                },
                "categorical": {
                    "category_threshold": 0.03,
                    "chi_square_pvalue": 0.05,
                    "null_threshold": 0.005
                },
                "correlation_threshold": 0.7,
                "correlation_change_threshold": 0.2,
                "js_distance_threshold": 0.1,
                "rare_value_threshold": 0.01,
                "analyze_distributions": True,
                "detect_rare_values": True,
                "gen_distribution_summaries": False
            },
            "deep_dive": {
                "numerical": {
                    "mean_threshold": 0.03,
                    "median_threshold": 0.03,
                    "std_threshold": 0.05,
                    "iqr_threshold": 0.05,
                    "null_threshold": 0.001
                },
                "categorical": {
                    "category_threshold": 0.01,
                    "chi_square_pvalue": 0.05,
                    "null_threshold": 0.001
                },
                "correlation_threshold": 0.6,
                "correlation_change_threshold": 0.15,
                "js_distance_threshold": 0.05,
                "rare_value_threshold": 0.005,
                "analyze_distributions": True,
                "detect_rare_values": True,
                "gen_distribution_summaries": True
            }
        }
    }
    
    # Add output table if provided
    if output_table:
        config["output_table"] = output_table
    
    # Save configuration to file
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Sample configuration generated at: {output_path}")
    print(f"You can now use this configuration with the data_drift_detector.py script.")

def infer_and_generate_config(
    table_path: str,
    reference_version: int,
    current_version: int,
    output_path: str,
    output_table: Optional[str] = None,
    profile: str = "standard",
    sample_size: int = 1000
) -> None:
    """Generate a configuration file with column types inferred from the data
    
    Args:
        table_path: Path to the Delta table to analyze
        reference_version: Version number for reference data
        current_version: Version number for current data
        output_path: Path to save the configuration file
        output_table: Optional path to Delta table for saving results
        profile: Analysis profile (summary, standard, deep_dive)
        sample_size: Number of rows to sample for inference
    """
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Load a sample of the current data
        df = spark.read.format("delta").option("versionAsOf", current_version).load(table_path)
        
        if sample_size > 0:
            df = df.limit(sample_size)
        
        # Infer column types
        inferred_types = {}
        numeric_types = ["int", "long", "float", "double", "decimal"]
        datetime_types = ["date", "timestamp"]
        
        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType.simpleString().lower()
            
            # Determine column type based on data type
            if any(t in data_type for t in numeric_types):
                # Check if it might be a categorical variable encoded as number
                try:
                    distinct_count = df.select(col_name).distinct().count()
                    total_count = df.count()
                    
                    if total_count > 0 and distinct_count / total_count < 0.05:
                        inferred_types[col_name] = "categorical"
                    else:
                        inferred_types[col_name] = "numerical"
                except:
                    # Default to numerical if check fails
                    inferred_types[col_name] = "numerical"
            
            elif any(t in data_type for t in datetime_types):
                inferred_types[col_name] = "temporal"
            
            elif "string" in data_type or "char" in data_type:
                # Check if it might be a temporal column
                try:
                    # Try to cast to timestamp to see if it's a date/time string
                    timestamp_check = df.select(col_name).limit(10)
                    timestamp_count = timestamp_check.where(
                        f"cast({col_name} as timestamp) is not null").count()
                    
                    if timestamp_count > 0:
                        # See if more than half can be cast to date/time
                        total_check = min(1000, df.count())
                        sample_check = df.select(col_name).limit(total_check)
                        timestamp_ratio = sample_check.where(
                            f"cast({col_name} as timestamp) is not null").count() / total_check
                        
                        if timestamp_ratio > 0.5:
                            inferred_types[col_name] = "temporal"
                            continue
                except:
                    # Ignore errors, will default to categorical
                    pass
                
                # Default strings to categorical
                inferred_types[col_name] = "categorical"
            
            else:
                # Default to categorical for other types
                inferred_types[col_name] = "categorical"
        
        # Generate config with inferred types
        config = {
            "table_path": table_path,
            "reference_version": reference_version,
            "current_version": current_version,
            "profile": profile,
            "analyze_distributions": True,
            "analyze_correlations": True,
            "analyze_groups": True,
            "analyze_feature_importance": False,
            "target_column": None,
            "include_columns": [],
            "exclude_columns": [],
            "custom_column_types": inferred_types,
            "sample_size": 100000
        }
        
        # Add output table if provided
        if output_table:
            config["output_table"] = output_table
            
        # Add standard thresholds
        config["thresholds"] = {
            "summary": {
                "numerical": {
                    "mean_threshold": 0.1,
                    "median_threshold": 0.1,
                    "std_threshold": 0.2,
                    "iqr_threshold": 0.2,
                    "null_threshold": 0.01
                },
                "categorical": {
                    "category_threshold": 0.05,
                    "chi_square_pvalue": 0.01,
                    "null_threshold": 0.01
                },
                "correlation_threshold": 0.7,
                "correlation_change_threshold": 0.3,
                "js_distance_threshold": 0.1,
                "analyze_distributions": False
            },
            "standard": {
                "numerical": {
                    "mean_threshold": 0.05,
                    "median_threshold": 0.05,
                    "std_threshold": 0.1,
                    "iqr_threshold": 0.1,
                    "null_threshold": 0.005
                },
                "categorical": {
                    "category_threshold": 0.03,
                    "chi_square_pvalue": 0.05,
                    "null_threshold": 0.005
                },
                "correlation_threshold": 0.7,
                "correlation_change_threshold": 0.2,
                "js_distance_threshold": 0.1,
                "analyze_distributions": True
            },
            "deep_dive": {
                "numerical": {
                    "mean_threshold": 0.03,
                    "median_threshold": 0.03,
                    "std_threshold": 0.05,
                    "iqr_threshold": 0.05,
                    "null_threshold": 0.001
                },
                "categorical": {
                    "category_threshold": 0.01,
                    "chi_square_pvalue": 0.05,
                    "null_threshold": 0.001
                },
                "correlation_threshold": 0.6,
                "correlation_change_threshold": 0.15,
                "js_distance_threshold": 0.05,
                "analyze_distributions": True,
                "gen_distribution_summaries": True
            }
        }
        
        # Save configuration to file
        with open(output_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Configuration with inferred column types generated at: {output_path}")
        print(f"Inferred {len(inferred_types)} column types")
        print(f"Numerical columns: {sum(1 for t in inferred_types.values() if t == 'numerical')}")
        print(f"Categorical columns: {sum(1 for t in inferred_types.values() if t == 'categorical')}")
        print(f"Temporal columns: {sum(1 for t in inferred_types.values() if t == 'temporal')}")
        
    except Exception as e:
        print(f"Error generating configuration: {str(e)}")
        # Fall back to basic config
        generate_sample_config(table_path, reference_version, current_version, 
                              output_path, output_table, profile)

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate data drift detector configuration')
    parser.add_argument('--table_path', required=True, help='Path to Delta table')
    parser.add_argument('--ref_version', type=int, required=True, help='Reference version number')
    parser.add_argument('--curr_version', type=int, required=True, help='Current version number')
    parser.add_argument('--output_path', required=True, help='Path to save configuration file')
    parser.add_argument('--output_table', help='Path to Delta table for saving results')
    parser.add_argument('--profile', default='standard', choices=['summary', 'standard', 'deep_dive'],
                       help='Analysis profile')
    parser.add_argument('--infer', action='store_true', help='Infer column types from data')
    
    args = parser.parse_args()
    
    if args.infer:
        infer_and_generate_config(
            args.table_path, args.ref_version, args.curr_version,
            args.output_path, args.output_table, args.profile
        )
    else:
        generate_sample_config(
            args.table_path, args.ref_version, args.curr_version,
            args.output_path, args.output_table, args.profile
        ) 