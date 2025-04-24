from pyspark.sql import DataFrame, types
from typing import Dict, Any, List, Set


class ColumnAnalyzer:
    """Module for analyzing columns and inferring their types"""
    
    @staticmethod
    def infer_column_types(df: DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """Infer column types and determine which columns to analyze
        
        Args:
            df: DataFrame to analyze
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with column type information
        """
        # Get configuration
        include_columns = config.get("include_columns", [])
        exclude_columns = config.get("exclude_columns", [])
        custom_column_types = config.get("custom_column_types", {})
        
        # Result dictionaries
        column_types = {}  # Stores inferred type for each column
        numerical_columns = []
        categorical_columns = []
        temporal_columns = []
        included_columns = []  # Final list of columns to analyze
        
        # Process all columns in the DataFrame
        for field in df.schema.fields:
            col_name = field.name
            
            # Skip if column is explicitly excluded
            if col_name in exclude_columns:
                continue
            
            # Skip if include_columns is specified and this column is not in it
            if include_columns and col_name not in include_columns:
                continue
            
            # Add to included columns
            included_columns.append(col_name)
            
            # Use custom type if provided
            if col_name in custom_column_types:
                col_type = custom_column_types[col_name].lower()
            else:
                # Infer type based on Spark data type
                col_type = ColumnAnalyzer._infer_type_from_spark_type(field.dataType, df, col_name)
            
            # Store column type
            column_types[col_name] = col_type
            
            # Add to type-specific lists
            if col_type == "numerical":
                numerical_columns.append(col_name)
            elif col_type == "categorical":
                categorical_columns.append(col_name)
            elif col_type == "temporal":
                temporal_columns.append(col_name)
        
        return {
            "column_types": column_types,
            "numerical_columns": numerical_columns,
            "categorical_columns": categorical_columns,
            "temporal_columns": temporal_columns,
            "included_columns": included_columns
        }
    
    @staticmethod
    def _infer_type_from_spark_type(
        data_type: types.DataType, 
        df: DataFrame, 
        col_name: str
    ) -> str:
        """Infer column type based on Spark data type and additional analysis
        
        Args:
            data_type: Spark data type
            df: DataFrame containing the column
            col_name: Name of the column to analyze
            
        Returns:
            Inferred column type ('numerical', 'categorical', or 'temporal')
        """
        # Convert type to string for easier comparison
        type_str = str(data_type).lower()
        
        # Check for temporal types
        if any(t in type_str for t in ["date", "timestamp"]):
            return "temporal"
        
        # Check for numerical types
        numeric_types = ["int", "long", "float", "double", "decimal", "short", "byte"]
        if any(t in type_str for t in numeric_types):
            # For potential numeric categorical columns, check cardinality
            try:
                distinct_count = df.select(col_name).where(f"{col_name} is not null").distinct().count()
                total_count = df.select(col_name).where(f"{col_name} is not null").count()
                
                # If low cardinality, treat as categorical
                if total_count > 0 and distinct_count / total_count < 0.05:
                    return "categorical"
                    
                # If high cardinality, treat as numerical
                return "numerical"
            except:
                # If analysis fails, default to numerical
                return "numerical"
        
        # Check for boolean
        if "boolean" in type_str:
            return "categorical"
        
        # Check for string types that might be temporal
        if any(t in type_str for t in ["string", "char", "varchar"]):
            try:
                # Try to cast a sample to timestamp
                sample_cast = df.select(
                    df[col_name].cast("timestamp").alias("cast_col")
                ).limit(100)
                
                # Count successful casts
                cast_count = sample_cast.where("cast_col is not null").count()
                sample_size = sample_cast.count()
                
                # If most values can be cast to timestamp, treat as temporal
                if sample_size > 0 and cast_count / sample_size > 0.9:
                    return "temporal"
            except:
                # If casting fails, proceed to next check
                pass
            
            # For string columns, check cardinality
            try:
                distinct_count = df.select(col_name).where(f"{col_name} is not null").distinct().count()
                total_count = df.select(col_name).where(f"{col_name} is not null").count()
                
                # If very high cardinality, it might be an ID or free text
                if total_count > 0 and distinct_count / total_count > 0.9:
                    return "categorical"  # Still treat as categorical, but might be excluded later
                
                return "categorical"
            except:
                # If analysis fails, default to categorical
                return "categorical"
        
        # Default to categorical for any other types
        return "categorical"
    
    @staticmethod
    def analyze_column_metadata(df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """Analyze metadata for each column
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with metadata for each column
        """
        metadata = {}
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            
            # Initialize column metadata
            metadata[col_name] = {
                "data_type": col_type,
                "nullable": field.nullable
            }
            
            # Add comments or metadata if available
            if hasattr(field, "metadata") and field.metadata:
                metadata[col_name]["metadata"] = field.metadata
            
            if hasattr(field, "comment") and field.comment:
                metadata[col_name]["comment"] = field.comment
        
        return metadata 