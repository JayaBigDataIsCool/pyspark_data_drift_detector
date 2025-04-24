from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, DataType, StringType
from typing import Dict, List, Set, Any, Tuple

class SchemaAnalyzer:
    """Analyzer for schema evolution and complex data types"""
    
    @staticmethod
    def detect_schema_changes(df_ref: DataFrame, 
                            df_curr: DataFrame) -> Dict[str, Any]:
        """Detect schema changes between two DataFrame versions
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            
        Returns:
            Dictionary with schema change details
        """
        # Get schema details
        ref_schema = df_ref.schema
        curr_schema = df_curr.schema
        
        ref_fields = {field.name: field.dataType for field in ref_schema.fields}
        curr_fields = {field.name: field.dataType for field in curr_schema.fields}
        
        # Find columns in reference but not in current
        removed_columns = []
        for col_name in ref_fields:
            if col_name not in curr_fields:
                removed_columns.append(col_name)
                
        # Find columns in current but not in reference
        added_columns = []
        for col_name in curr_fields:
            if col_name not in ref_fields:
                added_columns.append(col_name)
                
        # Find columns with type changes
        type_changes = []
        for col_name in ref_fields:
            if col_name in curr_fields:
                ref_type = ref_fields[col_name]
                curr_type = curr_fields[col_name]
                
                if str(ref_type) != str(curr_type):
                    type_changes.append({
                        "column": col_name,
                        "reference_type": str(ref_type),
                        "current_type": str(curr_type)
                    })
                    
        return {
            "removed_columns": removed_columns,
            "added_columns": added_columns,
            "type_changes": type_changes,
            "has_schema_changes": len(removed_columns) > 0 or len(added_columns) > 0 or len(type_changes) > 0
        }
    
    @staticmethod
    def analyze_complex_types(df_ref: DataFrame, 
                            df_curr: DataFrame, 
                            config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze changes in complex data types (structs, arrays, maps)
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            config: Configuration dictionary
            
        Returns:
            List of complex type change results
        """
        results = []
        
        # Identify complex columns in both DataFrames
        ref_complex_cols = SchemaAnalyzer._identify_complex_columns(df_ref)
        curr_complex_cols = SchemaAnalyzer._identify_complex_columns(df_curr)
        
        # Get all unique complex column names
        all_complex_cols = set(ref_complex_cols.keys()).union(set(curr_complex_cols.keys()))
        
        for col_name in all_complex_cols:
            col_results = {}
            
            # Check if column exists in both DataFrames
            if col_name not in ref_complex_cols:
                col_results = {
                    "column": col_name,
                    "issue": "added_complex_column",
                    "type": str(curr_complex_cols[col_name])
                }
            elif col_name not in curr_complex_cols:
                col_results = {
                    "column": col_name,
                    "issue": "removed_complex_column",
                    "type": str(ref_complex_cols[col_name])
                }
            else:
                # Compare the structure and contents
                ref_type = ref_complex_cols[col_name]
                curr_type = curr_complex_cols[col_name]
                
                if str(ref_type) != str(curr_type):
                    col_results = {
                        "column": col_name,
                        "issue": "complex_type_changed",
                        "reference_type": str(ref_type),
                        "current_type": str(curr_type)
                    }
                else:
                    # For structType, check for field distribution changes
                    if isinstance(ref_type, StructType):
                        col_results = SchemaAnalyzer._analyze_struct_changes(
                            df_ref, df_curr, col_name, config
                        )
                    
                    # For arrayType, check for changes in array sizes and distributions
                    elif isinstance(ref_type, ArrayType):
                        col_results = SchemaAnalyzer._analyze_array_changes(
                            df_ref, df_curr, col_name, config
                        )
                    
                    # For mapType, check for changes in keys and distributions
                    elif isinstance(ref_type, MapType):
                        col_results = SchemaAnalyzer._analyze_map_changes(
                            df_ref, df_curr, col_name, config
                        )
            
            if col_results:
                results.append(col_results)
                
        return results
                
    @staticmethod
    def _identify_complex_columns(df: DataFrame) -> Dict[str, DataType]:
        """Identify columns with complex data types
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping column names to their complex data types
        """
        complex_cols = {}
        
        for field in df.schema.fields:
            data_type = field.dataType
            
            if isinstance(data_type, (StructType, ArrayType, MapType)):
                complex_cols[field.name] = data_type
                
        return complex_cols
    
    @staticmethod
    def _analyze_struct_changes(df_ref: DataFrame, 
                              df_curr: DataFrame, 
                              column: str,
                              config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze changes in struct column distribution
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            column: Struct column to analyze
            config: Configuration dictionary
            
        Returns:
            Dictionary with struct change results
        """
        # Get struct fields
        struct_fields = [field.name for field in df_ref.schema[column].dataType.fields]
        
        # Check for null counts in the struct
        ref_null_count = df_ref.filter(F.col(column).isNull()).count()
        curr_null_count = df_curr.filter(F.col(column).isNull()).count()
        
        ref_total = df_ref.count()
        curr_total = df_curr.count()
        
        ref_null_ratio = ref_null_count / ref_total if ref_total > 0 else 0
        curr_null_ratio = curr_null_count / curr_total if curr_total > 0 else 0
        
        # Check for null counts in each field
        field_changes = []
        
        for field in struct_fields:
            field_path = f"{column}.{field}"
            
            ref_field_null = df_ref.filter(F.col(field_path).isNull()).count()
            curr_field_null = df_curr.filter(F.col(field_path).isNull()).count()
            
            ref_field_null_ratio = ref_field_null / (ref_total - ref_null_count) if ref_total - ref_null_count > 0 else 0
            curr_field_null_ratio = curr_field_null / (curr_total - curr_null_count) if curr_total - curr_null_count > 0 else 0
            
            null_ratio_change = abs(curr_field_null_ratio - ref_field_null_ratio)
            
            # Get threshold from config
            struct_field_threshold = config.get("struct_field_null_change_threshold", 0.1)
            
            if null_ratio_change >= struct_field_threshold:
                field_changes.append({
                    "field": field,
                    "reference_null_ratio": ref_field_null_ratio,
                    "current_null_ratio": curr_field_null_ratio,
                    "null_ratio_change": null_ratio_change,
                    "threshold": struct_field_threshold
                })
                
        # Get overall threshold
        struct_null_threshold = config.get("struct_null_change_threshold", 0.1)
        
        return {
            "column": column,
            "type": "struct",
            "reference_null_ratio": ref_null_ratio,
            "current_null_ratio": curr_null_ratio,
            "null_ratio_change": abs(curr_null_ratio - ref_null_ratio),
            "struct_null_change_significant": abs(curr_null_ratio - ref_null_ratio) >= struct_null_threshold,
            "field_changes": field_changes,
            "field_changes_significant": len(field_changes) > 0
        }
    
    @staticmethod
    def _analyze_array_changes(df_ref: DataFrame, 
                             df_curr: DataFrame, 
                             column: str,
                             config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze changes in array column distribution
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            column: Array column to analyze
            config: Configuration dictionary
            
        Returns:
            Dictionary with array change results
        """
        # Check for null counts
        ref_null_count = df_ref.filter(F.col(column).isNull()).count()
        curr_null_count = df_curr.filter(F.col(column).isNull()).count()
        
        ref_total = df_ref.count()
        curr_total = df_curr.count()
        
        ref_null_ratio = ref_null_count / ref_total if ref_total > 0 else 0
        curr_null_ratio = curr_null_count / curr_total if curr_total > 0 else 0
        
        # Check for empty arrays
        ref_empty_count = df_ref.filter(F.size(F.col(column)) == 0).count()
        curr_empty_count = df_curr.filter(F.size(F.col(column)) == 0).count()
        
        ref_empty_ratio = ref_empty_count / (ref_total - ref_null_count) if ref_total - ref_null_count > 0 else 0
        curr_empty_ratio = curr_empty_count / (curr_total - curr_null_count) if curr_total - curr_null_count > 0 else 0
        
        # Check for array sizes
        ref_size_avg = df_ref.filter(F.col(column).isNotNull()).select(F.avg(F.size(F.col(column)))).collect()[0][0] or 0
        curr_size_avg = df_curr.filter(F.col(column).isNotNull()).select(F.avg(F.size(F.col(column)))).collect()[0][0] or 0
        
        # Get thresholds from config
        array_null_threshold = config.get("array_null_change_threshold", 0.1)
        array_empty_threshold = config.get("array_empty_change_threshold", 0.1)
        array_size_threshold = config.get("array_size_change_threshold", 0.2)
        
        size_change_ratio = abs(curr_size_avg - ref_size_avg) / ref_size_avg if ref_size_avg > 0 else 0
        
        return {
            "column": column,
            "type": "array",
            "reference_null_ratio": ref_null_ratio,
            "current_null_ratio": curr_null_ratio,
            "null_ratio_change": abs(curr_null_ratio - ref_null_ratio),
            "null_change_significant": abs(curr_null_ratio - ref_null_ratio) >= array_null_threshold,
            "reference_empty_ratio": ref_empty_ratio,
            "current_empty_ratio": curr_empty_ratio,
            "empty_ratio_change": abs(curr_empty_ratio - ref_empty_ratio),
            "empty_change_significant": abs(curr_empty_ratio - ref_empty_ratio) >= array_empty_threshold,
            "reference_avg_size": ref_size_avg,
            "current_avg_size": curr_size_avg,
            "size_change_ratio": size_change_ratio,
            "size_change_significant": size_change_ratio >= array_size_threshold
        }
    
    @staticmethod
    def _analyze_map_changes(df_ref: DataFrame, 
                           df_curr: DataFrame, 
                           column: str,
                           config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze changes in map column distribution
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            column: Map column to analyze
            config: Configuration dictionary
            
        Returns:
            Dictionary with map change results
        """
        # Check for null counts
        ref_null_count = df_ref.filter(F.col(column).isNull()).count()
        curr_null_count = df_curr.filter(F.col(column).isNull()).count()
        
        ref_total = df_ref.count()
        curr_total = df_curr.count()
        
        ref_null_ratio = ref_null_count / ref_total if ref_total > 0 else 0
        curr_null_ratio = curr_null_count / curr_total if curr_total > 0 else 0
        
        # Check for empty maps
        ref_empty_count = df_ref.filter(F.size(F.col(column)) == 0).count()
        curr_empty_count = df_curr.filter(F.size(F.col(column)) == 0).count()
        
        ref_empty_ratio = ref_empty_count / (ref_total - ref_null_count) if ref_total - ref_null_count > 0 else 0
        curr_empty_ratio = curr_empty_count / (curr_total - curr_null_count) if curr_total - curr_null_count > 0 else 0
        
        # Check for map sizes
        ref_size_avg = df_ref.filter(F.col(column).isNotNull()).select(F.avg(F.size(F.col(column)))).collect()[0][0] or 0
        curr_size_avg = df_curr.filter(F.col(column).isNotNull()).select(F.avg(F.size(F.col(column)))).collect()[0][0] or 0
        
        # Get thresholds from config
        map_null_threshold = config.get("map_null_change_threshold", 0.1)
        map_empty_threshold = config.get("map_empty_change_threshold", 0.1)
        map_size_threshold = config.get("map_size_change_threshold", 0.2)
        
        size_change_ratio = abs(curr_size_avg - ref_size_avg) / ref_size_avg if ref_size_avg > 0 else 0
        
        return {
            "column": column,
            "type": "map",
            "reference_null_ratio": ref_null_ratio,
            "current_null_ratio": curr_null_ratio,
            "null_ratio_change": abs(curr_null_ratio - ref_null_ratio),
            "null_change_significant": abs(curr_null_ratio - ref_null_ratio) >= map_null_threshold,
            "reference_empty_ratio": ref_empty_ratio,
            "current_empty_ratio": curr_empty_ratio,
            "empty_ratio_change": abs(curr_empty_ratio - ref_empty_ratio),
            "empty_change_significant": abs(curr_empty_ratio - ref_empty_ratio) >= map_empty_threshold,
            "reference_avg_size": ref_size_avg,
            "current_avg_size": curr_size_avg,
            "size_change_ratio": size_change_ratio,
            "size_change_significant": size_change_ratio >= map_size_threshold
        }

    @staticmethod
    def analyze_schema_drift(df_ref: DataFrame, df_curr: DataFrame) -> List[Dict[str, Any]]:
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
                "details": f"Column added in current version",
                "severity": "high"
            })
            
        # Find removed columns
        removed_cols = ref_cols - curr_cols
        for col in removed_cols:
            differences.append({
                "column": col,
                "change_type": "removed",
                "details": f"Column removed in current version",
                "severity": "high"
            })
            
        # Check for data type changes in common columns
        common_cols = ref_cols.intersection(curr_cols)
        
        # Get schema information
        ref_fields = {f.name: f for f in df_ref.schema.fields}
        curr_fields = {f.name: f for f in df_curr.schema.fields}
        
        for col in common_cols:
            ref_field = ref_fields[col]
            curr_field = curr_fields[col]
            
            # Simple type check
            if ref_field.dataType.simpleString() != curr_field.dataType.simpleString():
                # Get more detailed differences for complex types
                type_diff = SchemaAnalyzer._analyze_type_difference(
                    ref_field.dataType, curr_field.dataType, col
                )
                
                differences.extend(type_diff)
                
            # Nullable property changed
            if ref_field.nullable != curr_field.nullable:
                severity = "high" if ref_field.nullable and not curr_field.nullable else "medium"
                differences.append({
                    "column": col,
                    "change_type": "nullable_change",
                    "details": f"Nullable property changed from {ref_field.nullable} to {curr_field.nullable}",
                    "severity": severity
                })
                
            # Metadata changes
            if ref_field.metadata != curr_field.metadata:
                differences.append({
                    "column": col,
                    "change_type": "metadata_change",
                    "details": f"Metadata changed",
                    "ref_metadata": str(ref_field.metadata),
                    "curr_metadata": str(curr_field.metadata),
                    "severity": "low"
                })
                
        return differences
    
    @staticmethod
    def _analyze_type_difference(ref_type, curr_type, path: str) -> List[Dict[str, Any]]:
        """Recursively analyze differences between complex types
        
        Args:
            ref_type: Reference data type
            curr_type: Current data type
            path: Current path in the schema
            
        Returns:
            List of type differences
        """
        differences = []
        
        # Check basic string representation
        if ref_type.simpleString() == curr_type.simpleString():
            return differences
            
        # Different base types
        if type(ref_type) != type(curr_type):
            differences.append({
                "column": path,
                "change_type": "type_change",
                "details": f"Data type changed from {ref_type.simpleString()} to {curr_type.simpleString()}",
                "severity": "high"
            })
            return differences
            
        # Handle StructType (recursively check fields)
        if isinstance(ref_type, StructType) and isinstance(curr_type, StructType):
            ref_fields = {f.name: f for f in ref_type.fields}
            curr_fields = {f.name: f for f in curr_type.fields}
            
            # Fields added
            for name in set(curr_fields.keys()) - set(ref_fields.keys()):
                differences.append({
                    "column": f"{path}.{name}",
                    "change_type": "field_added",
                    "details": f"Field added to struct",
                    "severity": "medium"
                })
                
            # Fields removed
            for name in set(ref_fields.keys()) - set(curr_fields.keys()):
                differences.append({
                    "column": f"{path}.{name}",
                    "change_type": "field_removed",
                    "details": f"Field removed from struct",
                    "severity": "high"
                })
                
            # Compare common fields
            for name in set(ref_fields.keys()) & set(curr_fields.keys()):
                ref_field = ref_fields[name]
                curr_field = curr_fields[name]
                
                # Recursive check for this field's type
                field_diffs = SchemaAnalyzer._analyze_type_difference(
                    ref_field.dataType, curr_field.dataType, f"{path}.{name}"
                )
                differences.extend(field_diffs)
                
                # Nullable property changed
                if ref_field.nullable != curr_field.nullable:
                    severity = "high" if ref_field.nullable and not curr_field.nullable else "medium"
                    differences.append({
                        "column": f"{path}.{name}",
                        "change_type": "nullable_change",
                        "details": f"Nullable property changed from {ref_field.nullable} to {curr_field.nullable}",
                        "severity": severity
                    })
        
        # Handle ArrayType (check element type)
        elif isinstance(ref_type, ArrayType) and isinstance(curr_type, ArrayType):
            # Check element type
            element_diffs = SchemaAnalyzer._analyze_type_difference(
                ref_type.elementType, curr_type.elementType, f"{path}[]"
            )
            differences.extend(element_diffs)
            
            # Containsnull property changed
            if ref_type.containsNull != curr_type.containsNull:
                severity = "high" if ref_type.containsNull and not curr_type.containsNull else "medium"
                differences.append({
                    "column": f"{path}",
                    "change_type": "array_nullability_change",
                    "details": f"Array containsNull changed from {ref_type.containsNull} to {curr_type.containsNull}",
                    "severity": severity
                })
        
        # Handle MapType (check key and value types)
        elif isinstance(ref_type, MapType) and isinstance(curr_type, MapType):
            # Check key type
            key_diffs = SchemaAnalyzer._analyze_type_difference(
                ref_type.keyType, curr_type.keyType, f"{path}[key]"
            )
            differences.extend(key_diffs)
            
            # Check value type
            value_diffs = SchemaAnalyzer._analyze_type_difference(
                ref_type.valueType, curr_type.valueType, f"{path}[value]"
            )
            differences.extend(value_diffs)
            
            # Valuenull property changed
            if ref_type.valueContainsNull != curr_type.valueContainsNull:
                severity = "high" if ref_type.valueContainsNull and not curr_type.valueContainsNull else "medium"
                differences.append({
                    "column": f"{path}",
                    "change_type": "map_nullability_change",
                    "details": f"Map valueContainsNull changed from {ref_type.valueContainsNull} to {curr_type.valueContainsNull}",
                    "severity": severity
                })
        
        # For other types, report the basic change
        else:
            differences.append({
                "column": path,
                "change_type": "type_change",
                "details": f"Data type changed from {ref_type.simpleString()} to {curr_type.simpleString()}",
                "severity": "high"
            })
            
        return differences
        
    @staticmethod
    def get_schema_profile(df: DataFrame) -> Dict[str, Any]:
        """Generate a schema profile with useful statistics
        
        Args:
            df: The DataFrame to analyze
            
        Returns:
            Dictionary with schema statistics
        """
        # Get basic info
        columns = df.columns
        fields = df.schema.fields
        
        # Count by type
        type_counts = {}
        for field in fields:
            type_name = field.dataType.simpleString()
            base_type = type_name.split("(")[0]
            type_counts[base_type] = type_counts.get(base_type, 0) + 1
            
        # Count complex types
        complex_types = {}
        for field in fields:
            if isinstance(field.dataType, StructType):
                complex_types[field.name] = {"type": "struct", "fields": len(field.dataType.fields)}
            elif isinstance(field.dataType, ArrayType):
                complex_types[field.name] = {"type": "array", "element_type": field.dataType.elementType.simpleString()}
            elif isinstance(field.dataType, MapType):
                complex_types[field.name] = {
                    "type": "map", 
                    "key_type": field.dataType.keyType.simpleString(),
                    "value_type": field.dataType.valueType.simpleString()
                }
                
        # Non-nullable fields
        non_nullable = [field.name for field in fields if not field.nullable]
        
        # Fields with metadata
        with_metadata = [field.name for field in fields if field.metadata]
        
        return {
            "column_count": len(columns),
            "type_counts": type_counts,
            "complex_columns": complex_types,
            "non_nullable_columns": non_nullable,
            "columns_with_metadata": with_metadata
        }
        
    @staticmethod
    def compare_schema_profiles(profile1: Dict[str, Any], profile2: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two schema profiles and report differences
        
        Args:
            profile1: First schema profile
            profile2: Second schema profile
            
        Returns:
            Dictionary with schema differences
        """
        diff = {}
        
        # Compare column counts
        diff["column_count_diff"] = profile2["column_count"] - profile1["column_count"]
        
        # Compare type counts
        diff["type_count_changes"] = {}
        all_types = set(profile1["type_counts"].keys()) | set(profile2["type_counts"].keys())
        for type_name in all_types:
            count1 = profile1["type_counts"].get(type_name, 0)
            count2 = profile2["type_counts"].get(type_name, 0)
            if count1 != count2:
                diff["type_count_changes"][type_name] = count2 - count1
                
        # Compare non-nullable columns
        non_nullable1 = set(profile1["non_nullable_columns"])
        non_nullable2 = set(profile2["non_nullable_columns"])
        diff["non_nullable_added"] = list(non_nullable2 - non_nullable1)
        diff["non_nullable_removed"] = list(non_nullable1 - non_nullable2)
        
        # Compare complex columns
        complex1 = set(profile1["complex_columns"].keys())
        complex2 = set(profile2["complex_columns"].keys())
        diff["complex_added"] = list(complex2 - complex1)
        diff["complex_removed"] = list(complex1 - complex2)
        diff["complex_changed"] = []
        
        for col in complex1 & complex2:
            if profile1["complex_columns"][col] != profile2["complex_columns"][col]:
                diff["complex_changed"].append({
                    "column": col,
                    "before": profile1["complex_columns"][col],
                    "after": profile2["complex_columns"][col]
                })
                
        return diff

    @staticmethod
    def analyze_schema(df_ref: DataFrame, df_curr: DataFrame) -> Dict[str, Any]:
        """Analyze schema changes between reference and current DataFrames
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            
        Returns:
            Dictionary with schema analysis results
        """
        # Get schemas
        ref_schema = df_ref.schema
        curr_schema = df_curr.schema
        
        # Get column sets
        ref_columns = {field.name for field in ref_schema.fields}
        curr_columns = {field.name for field in curr_schema.fields}
        
        # Find added, removed, and common columns
        added_columns = curr_columns - ref_columns
        removed_columns = ref_columns - curr_columns
        common_columns = ref_columns & curr_columns
        
        # Create dictionaries for faster lookups
        ref_schema_dict = {field.name: field for field in ref_schema.fields}
        curr_schema_dict = {field.name: field for field in curr_schema.fields}
        
        # Find type changes
        type_changes = []
        for col in common_columns:
            ref_type = ref_schema_dict[col].dataType
            curr_type = curr_schema_dict[col].dataType
            
            if str(ref_type) != str(curr_type):
                type_changes.append({
                    "column": col,
                    "ref_type": str(ref_type),
                    "curr_type": str(curr_type),
                    "is_compatible": SchemaAnalyzer._is_compatible_type_change(ref_type, curr_type)
                })
        
        # Find nullable changes
        nullable_changes = []
        for col in common_columns:
            ref_nullable = ref_schema_dict[col].nullable
            curr_nullable = curr_schema_dict[col].nullable
            
            if ref_nullable != curr_nullable:
                nullable_changes.append({
                    "column": col,
                    "ref_nullable": ref_nullable,
                    "curr_nullable": curr_nullable,
                    "risk": "high" if ref_nullable and not curr_nullable else "low"
                })
        
        # Find metadata changes
        metadata_changes = []
        for col in common_columns:
            ref_metadata = ref_schema_dict[col].metadata
            curr_metadata = curr_schema_dict[col].metadata
            
            if ref_metadata != curr_metadata:
                metadata_changes.append({
                    "column": col,
                    "ref_metadata": dict(ref_metadata),
                    "curr_metadata": dict(curr_metadata)
                })
        
        # Generate overall schema drift risk assessment
        schema_drift_risk = SchemaAnalyzer._assess_schema_drift_risk(
            added_columns, removed_columns, type_changes, nullable_changes
        )
        
        # Generate recommendation for handling schema changes
        recommendations = SchemaAnalyzer._generate_recommendations(
            added_columns, removed_columns, type_changes, nullable_changes
        )
        
        return {
            "added_columns": sorted(list(added_columns)),
            "removed_columns": sorted(list(removed_columns)),
            "type_changes": type_changes,
            "nullable_changes": nullable_changes,
            "metadata_changes": metadata_changes,
            "schema_drift_risk": schema_drift_risk,
            "recommendations": recommendations,
            "schema_version_compatibility": "compatible" if not (removed_columns or [c for c in type_changes if not c["is_compatible"]]) else "incompatible"
        }
    
    @staticmethod
    def _is_compatible_type_change(ref_type: DataType, curr_type: DataType) -> bool:
        """Check if type change is compatible
        
        Args:
            ref_type: Reference column type
            curr_type: Current column type
            
        Returns:
            True if change is compatible, False otherwise
        """
        # Convert types to strings for easier comparison
        ref_str = str(ref_type)
        curr_str = str(curr_type)
        
        # Same type is always compatible
        if ref_str == curr_str:
            return True
        
        # Compatible numeric type widening
        numeric_widening = [
            ("ByteType", "ShortType"),
            ("ByteType", "IntegerType"),
            ("ByteType", "LongType"),
            ("ByteType", "FloatType"),
            ("ByteType", "DoubleType"),
            ("ShortType", "IntegerType"),
            ("ShortType", "LongType"),
            ("ShortType", "FloatType"),
            ("ShortType", "DoubleType"),
            ("IntegerType", "LongType"),
            ("IntegerType", "FloatType"),
            ("IntegerType", "DoubleType"),
            ("LongType", "FloatType"),
            ("LongType", "DoubleType"),
            ("FloatType", "DoubleType")
        ]
        
        # Check for compatible numeric widening
        for narrow, wide in numeric_widening:
            if ref_str.startswith(narrow) and curr_str.startswith(wide):
                return True
        
        # String to other types is often incompatible
        if ref_str.startswith("StringType") and not curr_str.startswith("StringType"):
            return False
        
        # Other type changes are generally incompatible
        return False
    
    @staticmethod
    def _assess_schema_drift_risk(
        added_columns: Set[str], 
        removed_columns: Set[str], 
        type_changes: List[Dict[str, Any]], 
        nullable_changes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Assess the risk level of schema drift
        
        Args:
            added_columns: Set of added column names
            removed_columns: Set of removed column names
            type_changes: List of type changes
            nullable_changes: List of nullable changes
            
        Returns:
            Dictionary with risk assessment
        """
        # Initialize risk counters
        risk_levels = {
            "high": 0,
            "medium": 0,
            "low": 0
        }
        
        # Removed columns are high risk
        risk_levels["high"] += len(removed_columns)
        
        # Added columns are low risk
        risk_levels["low"] += len(added_columns)
         
        # Type changes
        for change in type_changes:
            if not change["is_compatible"]:
                risk_levels["high"] += 1
            else:
                risk_levels["medium"] += 1
        
        # Nullable changes
        for change in nullable_changes:
            if change["risk"] == "high":
                risk_levels["high"] += 1
            else:
                risk_levels["low"] += 1
        
        # Determine overall risk level
        overall_risk = "low"
        if risk_levels["high"] > 0:
            overall_risk = "high"
        elif risk_levels["medium"] > 0:
            overall_risk = "medium"
        
        return {
            "overall_risk": overall_risk,
            "high_risk_count": risk_levels["high"],
            "medium_risk_count": risk_levels["medium"],
            "low_risk_count": risk_levels["low"]
        }
    
    @staticmethod
    def _generate_recommendations(
        added_columns: Set[str], 
        removed_columns: Set[str], 
        type_changes: List[Dict[str, Any]], 
        nullable_changes: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate recommendations for handling schema changes
        
        Args:
            added_columns: Set of added column names
            removed_columns: Set of removed column names
            type_changes: List of type changes
            nullable_changes: List of nullable changes
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Handle removed columns
        if removed_columns:
            recommendations.append(
                f"Address {len(removed_columns)} removed columns - data depending on these columns may break"
            )
        
        # Handle incompatible type changes
        incompatible_changes = [c for c in type_changes if not c["is_compatible"]]
        if incompatible_changes:
            recommendations.append(
                f"Review {len(incompatible_changes)} incompatible type changes that may cause data conversion errors"
            )
        
        # Handle nullable to non-nullable changes
        risky_nullable_changes = [c for c in nullable_changes if c["risk"] == "high"]
        if risky_nullable_changes:
            recommendations.append(
                f"Examine {len(risky_nullable_changes)} columns that changed from nullable to non-nullable which may reject null values"
            )
        
        # Handle added columns
        if added_columns:
            recommendations.append(
                f"Review {len(added_columns)} new columns to understand their purpose and data quality"
            )
        
        # Add general recommendation if no specific issues
        if not recommendations:
            recommendations.append("No critical schema changes detected. Normal monitoring recommended.")
        
        return recommendations 