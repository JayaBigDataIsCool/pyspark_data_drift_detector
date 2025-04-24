from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Any, Tuple

class AdaptiveThreshold:
    """Handles adaptive thresholds that adjust based on dataset characteristics"""
    
    @staticmethod
    def calculate_adaptive_thresholds(df_ref: DataFrame, 
                                    df_curr: DataFrame, 
                                    config: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate adaptive thresholds based on dataset characteristics
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            config: Configuration dictionary
            
        Returns:
            Dictionary with adaptive thresholds
        """
        # Get base thresholds from config
        base_numerical_threshold = config.get("numerical_drift_threshold", 0.1)
        base_categorical_threshold = config.get("categorical_drift_threshold", 0.05)
        
        # Calculate dataset size factors
        ref_count = df_ref.count()
        curr_count = df_curr.count()
        
        # Smaller datasets need higher thresholds to avoid false positives
        size_factor = AdaptiveThreshold._calculate_size_factor(min(ref_count, curr_count))
        
        # Calculate complexity factor based on number of columns and their types
        complexity_factor = AdaptiveThreshold._calculate_complexity_factor(df_ref, df_curr)
        
        # Calculate adaptive thresholds
        adjusted_numerical_threshold = base_numerical_threshold * size_factor * complexity_factor
        adjusted_categorical_threshold = base_categorical_threshold * size_factor * complexity_factor
        
        # Calculate column-specific thresholds
        column_thresholds = AdaptiveThreshold._calculate_column_specific_thresholds(
            df_ref, df_curr, config, size_factor, complexity_factor
        )
        
        return {
            "global_numerical_threshold": adjusted_numerical_threshold,
            "global_categorical_threshold": adjusted_categorical_threshold,
            "column_thresholds": column_thresholds,
            "size_factor": size_factor,
            "complexity_factor": complexity_factor
        }
    
    @staticmethod
    def _calculate_size_factor(row_count: int) -> float:
        """Calculate size factor based on dataset size
        
        Args:
            row_count: Number of rows in the dataset
            
        Returns:
            Size factor for thresholds
        """
        # Smaller datasets need more lenient thresholds to avoid false positives
        if row_count < 100:
            return 2.0  # Very small datasets need much higher thresholds
        elif row_count < 1000:
            return 1.5  # Small datasets need higher thresholds
        elif row_count < 10000:
            return 1.2  # Medium datasets need slightly higher thresholds
        elif row_count < 100000:
            return 1.0  # Standard threshold for medium-large datasets
        elif row_count < 1000000:
            return 0.8  # Large datasets can have stricter thresholds
        else:
            return 0.6  # Very large datasets can have very strict thresholds
    
    @staticmethod
    def _calculate_complexity_factor(df_ref: DataFrame, df_curr: DataFrame) -> float:
        """Calculate complexity factor based on dataset characteristics
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            
        Returns:
            Complexity factor for thresholds
        """
        # Count columns of different types
        ref_columns = len(df_ref.columns)
        
        # More complex datasets need higher thresholds
        if ref_columns < 10:
            return 0.9  # Simple datasets can have stricter thresholds
        elif ref_columns < 50:
            return 1.0  # Standard threshold for moderate complexity
        elif ref_columns < 100:
            return 1.1  # More complex datasets need slightly higher thresholds
        else:
            return 1.2  # Very complex datasets need higher thresholds
    
    @staticmethod
    def _calculate_column_specific_thresholds(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        config: Dict[str, Any],
        size_factor: float,
        complexity_factor: float
    ) -> Dict[str, Dict[str, float]]:
        """Calculate column-specific thresholds based on data characteristics
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            config: Configuration dictionary
            size_factor: Size factor for thresholds
            complexity_factor: Complexity factor for thresholds
            
        Returns:
            Dictionary mapping column names to their thresholds
        """
        column_thresholds = {}
        
        # Get columns to analyze
        columns_to_analyze = config.get("columns_to_analyze", df_ref.columns)
        
        # Get base thresholds
        base_numerical_threshold = config.get("numerical_drift_threshold", 0.1)
        base_categorical_threshold = config.get("categorical_drift_threshold", 0.05)
        
        # Calculate column-specific thresholds
        for column in columns_to_analyze:
            if column not in df_ref.columns or column not in df_curr.columns:
                continue
                
            # Calculate column-specific factors
            volatility_factor = AdaptiveThreshold._calculate_column_volatility(df_ref, column)
            cardinality_factor = AdaptiveThreshold._calculate_column_cardinality(df_ref, column)
            null_factor = AdaptiveThreshold._calculate_column_null_factor(df_ref, column)
            
            # Determine if column is numerical or categorical
            col_type = AdaptiveThreshold._infer_column_type(df_ref, column)
            
            if col_type == "numerical":
                base_threshold = base_numerical_threshold
                # Calculate adjusted threshold
                adjusted_threshold = (
                    base_threshold * 
                    size_factor * 
                    complexity_factor * 
                    volatility_factor * 
                    cardinality_factor * 
                    null_factor
                )
                
                column_thresholds[column] = {
                    "type": "numerical",
                    "threshold": adjusted_threshold,
                    "volatility_factor": volatility_factor,
                    "cardinality_factor": cardinality_factor,
                    "null_factor": null_factor
                }
            else:  # categorical
                base_threshold = base_categorical_threshold
                # Calculate adjusted threshold
                adjusted_threshold = (
                    base_threshold * 
                    size_factor * 
                    complexity_factor * 
                    volatility_factor * 
                    cardinality_factor * 
                    null_factor
                )
                
                column_thresholds[column] = {
                    "type": "categorical",
                    "threshold": adjusted_threshold,
                    "volatility_factor": volatility_factor,
                    "cardinality_factor": cardinality_factor,
                    "null_factor": null_factor
                }
                
        return column_thresholds
    
    @staticmethod
    def _calculate_column_volatility(df: DataFrame, column: str) -> float:
        """Calculate column volatility factor
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Volatility factor for thresholds
        """
        try:
            # Try to calculate coefficient of variation for numerical columns
            if AdaptiveThreshold._infer_column_type(df, column) == "numerical":
                stats = df.select(
                    F.stddev(F.col(column)).alias("stddev"),
                    F.avg(F.col(column)).alias("mean")
                ).collect()[0]
                
                stddev = stats["stddev"] or 0
                mean = stats["mean"] or 0
                
                if mean == 0:
                    return 1.5  # Default for unstable columns
                
                cv = abs(stddev / mean)
                
                # Higher coefficient of variation means more volatile data
                if cv < 0.1:
                    return 0.8  # Very stable data can have stricter thresholds
                elif cv < 0.5:
                    return 1.0  # Standard threshold for moderate volatility
                elif cv < 1.0:
                    return 1.2  # More volatile data needs higher thresholds
                else:
                    return 1.5  # Very volatile data needs much higher thresholds
            else:
                # For categorical columns, use entropy as a measure of volatility
                counts = df.select(column).groupBy(column).count().collect()
                total = df.count()
                
                if total == 0:
                    return 1.0
                
                # Calculate entropy
                entropy = 0
                for row in counts:
                    if row[column] is not None:
                        p = row["count"] / total
                        entropy -= p * (p.log() if p > 0 else 0)
                
                # Normalize entropy (0-1 scale)
                unique_count = len(counts)
                max_entropy = (unique_count.log()) if unique_count > 0 else 0
                normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0
                
                # Higher entropy means more uniform distribution (less volatile)
                if normalized_entropy < 0.3:
                    return 1.3  # Low entropy (more skewed) needs higher thresholds
                elif normalized_entropy < 0.7:
                    return 1.0  # Standard threshold for moderate entropy
                else:
                    return 0.9  # High entropy (more uniform) can have slightly stricter thresholds
        except Exception:
            # In case of errors, return a safe default
            return 1.0
    
    @staticmethod
    def _calculate_column_cardinality(df: DataFrame, column: str) -> float:
        """Calculate column cardinality factor
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Cardinality factor for thresholds
        """
        try:
            # Get distinct count
            distinct_count = df.select(column).distinct().count()
            total_count = df.count()
            
            if total_count == 0:
                return 1.0
                
            # Calculate cardinality ratio
            cardinality_ratio = distinct_count / total_count
            
            # Higher cardinality needs higher thresholds
            if cardinality_ratio < 0.01:
                return 0.8  # Very low cardinality can have stricter thresholds
            elif cardinality_ratio < 0.1:
                return 0.9  # Low cardinality can have slightly stricter thresholds
            elif cardinality_ratio < 0.5:
                return 1.0  # Standard threshold for moderate cardinality
            elif cardinality_ratio < 0.9:
                return 1.2  # High cardinality needs higher thresholds
            else:
                return 1.5  # Very high cardinality needs much higher thresholds
        except Exception:
            # In case of errors, return a safe default
            return 1.0
    
    @staticmethod
    def _calculate_column_null_factor(df: DataFrame, column: str) -> float:
        """Calculate column null factor
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Null factor for thresholds
        """
        try:
            # Calculate null ratio
            null_count = df.filter(F.col(column).isNull()).count()
            total_count = df.count()
            
            if total_count == 0:
                return 1.0
                
            null_ratio = null_count / total_count
            
            # Higher null ratio needs higher thresholds
            if null_ratio < 0.01:
                return 0.9  # Very few nulls can have slightly stricter thresholds
            elif null_ratio < 0.1:
                return 1.0  # Standard threshold for moderate null ratio
            elif null_ratio < 0.3:
                return 1.1  # More nulls need slightly higher thresholds
            elif null_ratio < 0.5:
                return 1.3  # High null ratio needs higher thresholds
            else:
                return 1.5  # Very high null ratio needs much higher thresholds
        except Exception:
            # In case of errors, return a safe default
            return 1.0
    
    @staticmethod
    def _infer_column_type(df: DataFrame, column: str) -> str:
        """Infer column type (numerical or categorical)
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            'numerical' or 'categorical'
        """
        try:
            # Check if column is numerical
            col_type = df.schema[column].dataType
            if (str(col_type).startswith("IntegerType") or 
                str(col_type).startswith("LongType") or 
                str(col_type).startswith("DoubleType") or 
                str(col_type).startswith("FloatType") or 
                str(col_type).startswith("DecimalType")):
                
                # Further check if it might be a categorical variable encoded as number
                distinct_count = df.select(column).distinct().count()
                total_count = df.count()
                
                if total_count > 0 and distinct_count / total_count < 0.05:
                    return "categorical"  # Likely a categorical encoded as number
                else:
                    return "numerical"
            else:
                return "categorical"
        except Exception:
            # In case of errors, assume categorical
            return "categorical"

    @staticmethod
    def get_adaptive_numerical_threshold(df_ref: DataFrame, column: str, 
                                       base_threshold: float = 0.2,
                                       min_threshold: float = 0.05,
                                       max_threshold: float = 0.5) -> float:
        """Calculate an adaptive threshold for a numerical column
        
        Args:
            df_ref: Reference DataFrame
            column: Column name
            base_threshold: Base threshold for relative change
            min_threshold: Minimum threshold to use
            max_threshold: Maximum threshold to use
            
        Returns:
            Adaptive threshold value
        """
        if column not in df_ref.columns:
            return base_threshold
            
        try:
            # Calculate coefficient of variation (CV) to measure relative variability
            stats = df_ref.select(
                F.mean(column).alias("mean"),
                F.stddev(column).alias("stddev")
            ).collect()[0]
            
            mean = stats["mean"]
            stddev = stats["stddev"]
            
            # Handle special cases
            if mean is None or stddev is None or mean == 0:
                return base_threshold
                
            # Calculate coefficient of variation
            cv = abs(stddev / mean)
            
            # Adjust threshold based on CV
            # Higher CV = higher threshold (more variability)
            # Lower CV = lower threshold (less variability)
            if cv < 0.1:  # Low variability
                threshold = min_threshold
            elif cv > 1.0:  # High variability
                threshold = max_threshold
            else:
                # Linear interpolation between min and max thresholds
                threshold = min_threshold + (max_threshold - min_threshold) * (cv - 0.1) / 0.9
                
            return threshold
            
        except Exception:
            # Fall back to base threshold if any calculation issues
            return base_threshold
            
    @staticmethod
    def get_adaptive_categorical_threshold(df_ref: DataFrame, column: str,
                                         base_threshold: float = 0.2,
                                         min_threshold: float = 0.05,
                                         max_threshold: float = 0.5) -> float:
        """Calculate an adaptive threshold for a categorical column
        
        Args:
            df_ref: Reference DataFrame
            column: Column name
            base_threshold: Base threshold for distribution change
            min_threshold: Minimum threshold to use
            max_threshold: Maximum threshold to use
            
        Returns:
            Adaptive threshold value
        """
        if column not in df_ref.columns:
            return base_threshold
            
        try:
            # Get the number of distinct values
            distinct_count = df_ref.select(column).distinct().count()
            total_count = df_ref.count()
            
            if total_count == 0:
                return base_threshold
                
            # Calculate entropy as a measure of distribution concentration
            value_counts = df_ref.groupBy(column).count()
            value_counts = value_counts.withColumn("probability", F.col("count") / total_count)
            
            # Calculate entropy: -sum(p * log(p))
            entropy_df = value_counts.select(
                F.sum(
                    F.col("probability") * F.log(F.col("probability"))
                ).alias("neg_entropy")
            )
            neg_entropy = entropy_df.collect()[0]["neg_entropy"]
            
            if neg_entropy is None:
                return base_threshold
                
            entropy = -neg_entropy
            
            # Normalize entropy by maximum possible entropy (log of number of categories)
            max_entropy = F.log(F.lit(distinct_count))
            max_entropy_val = df_ref.select(max_entropy.alias("max_entropy")).collect()[0]["max_entropy"]
            
            if max_entropy_val is None or max_entropy_val == 0:
                return base_threshold
                
            normalized_entropy = entropy / max_entropy_val
            
            # Adjust threshold based on normalized entropy
            # Higher normalized entropy (more uniform distribution) = lower threshold
            # Lower normalized entropy (more concentrated) = higher threshold
            if normalized_entropy > 0.8:  # Very uniform
                threshold = min_threshold
            elif normalized_entropy < 0.3:  # Very concentrated
                threshold = max_threshold
            else:
                # Linear interpolation between min and max thresholds
                threshold = max_threshold - (max_threshold - min_threshold) * (normalized_entropy - 0.3) / 0.5
                
            return threshold
            
        except Exception:
            # Fall back to base threshold if any calculation issues
            return base_threshold
            
    @staticmethod
    def get_adaptive_column_thresholds(df_ref: DataFrame, 
                                      num_columns: List[str], 
                                      cat_columns: List[str],
                                      base_thresholds: Dict[str, float]) -> Dict[str, float]:
        """Get adaptive thresholds for all columns
        
        Args:
            df_ref: Reference DataFrame
            num_columns: List of numerical columns
            cat_columns: List of categorical columns
            base_thresholds: Dictionary of base thresholds by column or column type
            
        Returns:
            Dictionary of adaptive thresholds by column
        """
        thresholds = {}
        
        # Get base thresholds for numerical and categorical columns
        num_base = base_thresholds.get("numerical", 0.2)
        cat_base = base_thresholds.get("categorical", 0.2)
        
        # Calculate adaptive thresholds for numerical columns
        for col in num_columns:
            # Use column-specific base threshold if available
            col_base = base_thresholds.get(col, num_base)
            thresholds[col] = AdaptiveThreshold.get_adaptive_numerical_threshold(
                df_ref, col, base_threshold=col_base
            )
            
        # Calculate adaptive thresholds for categorical columns
        for col in cat_columns:
            # Use column-specific base threshold if available
            col_base = base_thresholds.get(col, cat_base)
            thresholds[col] = AdaptiveThreshold.get_adaptive_categorical_threshold(
                df_ref, col, base_threshold=col_base
            )
            
        return thresholds
        
    @staticmethod
    def adjust_threshold_for_sample_size(threshold: float, sample_size: int, 
                                       full_size: int, 
                                       min_factor: float = 0.5,
                                       max_factor: float = 2.0) -> float:
        """Adjust threshold based on sample size relative to full dataset
        
        Args:
            threshold: Base threshold
            sample_size: Size of the sample
            full_size: Size of the full dataset
            min_factor: Minimum adjustment factor
            max_factor: Maximum adjustment factor
            
        Returns:
            Adjusted threshold
        """
        if full_size == 0 or sample_size >= full_size:
            return threshold
            
        # Calculate sampling ratio
        ratio = sample_size / full_size
        
        # Calculate adjustment factor (inversely proportional to square root of ratio)
        # Smaller samples need higher thresholds
        if ratio <= 0.01:  # Very small sample
            factor = max_factor
        elif ratio >= 0.5:  # Large sample
            factor = min_factor
        else:
            # Adjust based on square root of ratio (statistical principle)
            # as sample size decreases, variance increases proportionally to 1/sqrt(n)
            factor = min_factor + (max_factor - min_factor) * (1 - (ratio / 0.5) ** 0.5)
            
        return threshold * factor 