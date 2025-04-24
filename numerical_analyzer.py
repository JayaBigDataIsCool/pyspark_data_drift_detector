from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List, Tuple
import math

class NumericalAnalyzer:
    """Module for analyzing numerical columns and detecting drift"""
    
    @staticmethod
    def analyze_numerical_columns(
        df_ref: DataFrame,
        df_curr: DataFrame,
        columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Analyze numerical columns for drift
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of numerical columns to analyze
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with analysis results by column
        """
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {}).get(profile, {}).get("numerical", {})
        
        # Default thresholds if not specified
        mean_threshold = thresholds.get("mean_threshold", 0.05)
        median_threshold = thresholds.get("median_threshold", 0.05)
        std_threshold = thresholds.get("std_threshold", 0.1)
        iqr_threshold = thresholds.get("iqr_threshold", 0.1)
        null_threshold = thresholds.get("null_threshold", 0.005)
        range_threshold = thresholds.get("range_threshold", 0.1)
        
        # Initialize results dictionary
        results = {}
        
        # Analyze each numerical column
        for column in columns:
            # Skip columns not in both dataframes
            if column not in df_ref.columns or column not in df_curr.columns:
                continue
                
            try:
                # Calculate statistics for reference data
                ref_stats = NumericalAnalyzer._calculate_stats(df_ref, column)
                
                # Calculate statistics for current data
                curr_stats = NumericalAnalyzer._calculate_stats(df_curr, column)
                
                # Calculate percentile statistics
                ref_percentiles = NumericalAnalyzer._calculate_percentiles(df_ref, column)
                curr_percentiles = NumericalAnalyzer._calculate_percentiles(df_curr, column)
                
                # Merge all stats
                ref_stats.update(ref_percentiles)
                curr_stats.update(curr_percentiles)
                
                # Calculate drift metrics
                drift_metrics = NumericalAnalyzer._calculate_drift_metrics(
                    ref_stats, curr_stats, thresholds
                )
                
                # Check if drift was detected based on thresholds
                drift_detected = (
                    abs(drift_metrics.get("mean_relative_diff", 0)) > mean_threshold or
                    abs(drift_metrics.get("median_relative_diff", 0)) > median_threshold or
                    abs(drift_metrics.get("std_relative_diff", 0)) > std_threshold or
                    abs(drift_metrics.get("iqr_relative_diff", 0)) > iqr_threshold or
                    abs(drift_metrics.get("null_diff", 0)) > null_threshold or
                    abs(drift_metrics.get("range_relative_diff", 0)) > range_threshold
                )
                
                # Determine which metrics contributed to drift
                drift_causes = []
                if abs(drift_metrics.get("mean_relative_diff", 0)) > mean_threshold:
                    drift_causes.append("mean")
                if abs(drift_metrics.get("median_relative_diff", 0)) > median_threshold:
                    drift_causes.append("median")
                if abs(drift_metrics.get("std_relative_diff", 0)) > std_threshold:
                    drift_causes.append("std_dev")
                if abs(drift_metrics.get("iqr_relative_diff", 0)) > iqr_threshold:
                    drift_causes.append("iqr")
                if abs(drift_metrics.get("null_diff", 0)) > null_threshold:
                    drift_causes.append("null_proportion")
                if abs(drift_metrics.get("range_relative_diff", 0)) > range_threshold:
                    drift_causes.append("range")
                
                # Store all results for this column
                results[column] = {
                    "ref_stats": ref_stats,
                    "curr_stats": curr_stats,
                    "drift_metrics": drift_metrics,
                    "drift_detected": drift_detected,
                    "drift_causes": drift_causes,
                    "drift_score": drift_metrics.get("drift_score", 0)
                }
                
            except Exception as e:
                # Store error for this column
                results[column] = {
                    "error": str(e),
                    "drift_detected": False
                }
        
        return results
    
    @staticmethod
    def _calculate_stats(df: DataFrame, column: str) -> Dict[str, Any]:
        """Calculate basic statistics for a numerical column
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Dictionary with statistics
        """
        # Count total rows and null values
        total_count = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        
        # Calculate null proportion
        null_proportion = null_count / total_count if total_count > 0 else 0
        
        # Calculate basic statistics
        stats = df.select(
            F.count(column).alias("count"),
            F.min(column).alias("min"),
            F.max(column).alias("max"),
            F.avg(column).alias("mean"),
            F.stddev(column).alias("std_dev"),
            F.expr(f"percentile({column}, 0.5)").alias("median")
        ).collect()[0].asDict()
        
        # Calculate IQR (75th percentile - 25th percentile)
        percentiles = df.select(
            F.expr(f"percentile({column}, array(0.25, 0.75))").alias("quartiles")
        ).collect()[0]["quartiles"]
        
        if percentiles and len(percentiles) == 2:
            q1 = percentiles[0]
            q3 = percentiles[1]
            iqr = q3 - q1
        else:
            q1 = None
            q3 = None
            iqr = None
        
        # Add null stats and IQR to results
        stats.update({
            "null_count": null_count,
            "null_proportion": null_proportion,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "range": stats["max"] - stats["min"] if stats["max"] is not None and stats["min"] is not None else None
        })
        
        return stats
    
    @staticmethod
    def _calculate_percentiles(df: DataFrame, column: str) -> Dict[str, Any]:
        """Calculate percentiles for a numerical column
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Dictionary with percentile values
        """
        # Define percentiles to calculate
        percentiles = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        percentile_expr = f"percentile({column}, array({','.join(str(p) for p in percentiles)}))"
        
        # Calculate percentiles
        percentile_values = df.select(
            F.expr(percentile_expr).alias("percentiles")
        ).collect()[0]["percentiles"]
        
        # Map percentiles to their values
        result = {}
        if percentile_values:
            for i, p in enumerate(percentiles):
                result[f"p{int(p*100)}"] = percentile_values[i]
        
        return result
    
    @staticmethod
    def _calculate_drift_metrics(
        ref_stats: Dict[str, Any],
        curr_stats: Dict[str, Any],
        thresholds: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate drift metrics between reference and current statistics
        
        Args:
            ref_stats: Reference statistics
            curr_stats: Current statistics
            thresholds: Thresholds for drift detection
            
        Returns:
            Dictionary with drift metrics
        """
        # Calculate absolute and relative differences for key statistics
        drift_metrics = {}
        
        # Helper function to calculate relative difference
        def relative_diff(ref_val, curr_val):
            if ref_val is None or curr_val is None:
                return None
            if ref_val == 0:
                return 1.0 if curr_val != 0 else 0.0
            return (curr_val - ref_val) / abs(ref_val)
        
        # Calculate differences for all statistics
        for stat in ["mean", "median", "std_dev", "min", "max", "iqr", "range"]:
            ref_val = ref_stats.get(stat)
            curr_val = curr_stats.get(stat)
            
            if ref_val is not None and curr_val is not None:
                # Calculate absolute difference
                abs_diff = curr_val - ref_val
                drift_metrics[f"{stat}_abs_diff"] = abs_diff
                
                # Calculate relative difference
                rel_diff = relative_diff(ref_val, curr_val)
                drift_metrics[f"{stat}_relative_diff"] = rel_diff
        
        # Calculate null proportion difference
        ref_null = ref_stats.get("null_proportion", 0)
        curr_null = curr_stats.get("null_proportion", 0)
        drift_metrics["null_diff"] = curr_null - ref_null
        
        # Calculate percentile differences
        for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
            p_key = f"p{p}"
            if p_key in ref_stats and p_key in curr_stats:
                ref_val = ref_stats[p_key]
                curr_val = curr_stats[p_key]
                
                if ref_val is not None and curr_val is not None:
                    # Calculate relative difference
                    rel_diff = relative_diff(ref_val, curr_val)
                    drift_metrics[f"{p_key}_relative_diff"] = rel_diff
        
        # Calculate overall drift score as weighted sum of key metrics
        weights = {
            "mean_relative_diff": 0.25,
            "median_relative_diff": 0.2,
            "std_dev_relative_diff": 0.15,
            "iqr_relative_diff": 0.15,
            "null_diff": 0.1,
            "range_relative_diff": 0.15
        }
        
        weighted_sum = 0
        total_weight = 0
        
        for metric, weight in weights.items():
            if metric in drift_metrics and drift_metrics[metric] is not None:
                weighted_sum += abs(drift_metrics[metric]) * weight
                total_weight += weight
        
        if total_weight > 0:
            drift_score = min(1.0, weighted_sum / total_weight)
            drift_metrics["drift_score"] = drift_score
        else:
            drift_metrics["drift_score"] = 0
        
        return drift_metrics

    @staticmethod
    def calculate_drift_scores(df_ref: DataFrame, df_curr: DataFrame, columns: List[str], 
                              dimension_id: str = "all", detect_outliers: bool = False) -> List[Dict]:
        """Calculate drift scores for numerical columns
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of numerical columns to analyze
            dimension_id: ID for the dimension slice being analyzed
            detect_outliers: Whether to run outlier detection
            
        Returns:
            List of dictionaries with drift scores
        """
        results = []
        
        for col in columns:
            try:
                # Get statistics from both datasets with error handling
                try:
                    stats_ref = df_ref.select(
                        F.count(col).alias("count"),
                        F.count(F.when(F.col(col).isNull(), 1)).alias("null_count"),
                        F.min(col).alias("min"),
                        F.max(col).alias("max"),
                        F.mean(col).alias("mean"),
                        F.stddev(col).alias("stddev"),
                        F.expr(f"percentile_approx({col}, 0.5, 0.01)").alias("median"),
                        F.expr(f"percentile_approx({col}, array(0.25, 0.75, 0.95, 0.99), 0.01)").alias("percentiles")
                    ).na.fill({
                        "mean": 0.0,
                        "stddev": 0.0,
                        "median": 0.0
                    }).collect()[0]
                    
                    stats_curr = df_curr.select(
                        F.count(col).alias("count"),
                        F.count(F.when(F.col(col).isNull(), 1)).alias("null_count"),
                        F.min(col).alias("min"),
                        F.max(col).alias("max"),
                        F.mean(col).alias("mean"),
                        F.stddev(col).alias("stddev"),
                        F.expr(f"percentile_approx({col}, 0.5, 0.01)").alias("median"),
                        F.expr(f"percentile_approx({col}, array(0.25, 0.75, 0.95, 0.99), 0.01)").alias("percentiles")
                    ).na.fill({
                        "mean": 0.0,
                        "stddev": 0.0,
                        "median": 0.0
                    }).collect()[0]
                except Exception as e:
                    raise ValueError(f"Failed to calculate basic statistics for column {col}: {str(e)}")
                
                # Calculate individual components of the drift score
                drift_components = []
                
                # 1. Null fraction change
                ref_null_fraction = stats_ref["null_count"] / stats_ref["count"] if stats_ref["count"] > 0 else 0
                curr_null_fraction = stats_curr["null_count"] / stats_curr["count"] if stats_curr["count"] > 0 else 0
                null_fraction_diff = abs(curr_null_fraction - ref_null_fraction)
                drift_components.append(null_fraction_diff)
                
                # Use a small epsilon to avoid division by zero
                epsilon = 1e-10
                
                # 2. Mean shift (normalized by standard deviation)
                if (stats_ref["mean"] is not None and stats_curr["mean"] is not None):
                    # Use max of stddev and epsilon to avoid division by zero
                    stddev_safe = max(stats_ref["stddev"], epsilon) if stats_ref["stddev"] is not None else epsilon
                    mean_diff = abs(stats_curr["mean"] - stats_ref["mean"]) / stddev_safe
                    drift_components.append(min(mean_diff, 1.0))  # Cap at 1.0
                
                # 3. Standard deviation ratio (capped)
                if (stats_ref["stddev"] is not None and stats_curr["stddev"] is not None):
                    stddev_safe = max(stats_ref["stddev"], epsilon)
                    # Calculate log ratio to handle both increases and decreases equally
                    if stats_curr["stddev"] > epsilon:
                        log_stddev_ratio = abs(math.log(stats_curr["stddev"] / stddev_safe))
                        # Normalize: log(2) means doubled or halved std dev
                        stddev_diff = min(log_stddev_ratio / math.log(2), 1.0)
                        drift_components.append(stddev_diff)
                
                # 4. Median shift (normalized)
                if (stats_ref["median"] is not None and stats_curr["median"] is not None):
                    stddev_safe = max(stats_ref["stddev"], epsilon) if stats_ref["stddev"] is not None else epsilon
                    median_diff = abs(stats_curr["median"] - stats_ref["median"]) / stddev_safe
                    drift_components.append(min(median_diff, 1.0))  # Cap at 1.0
                
                # 5. Range change
                if (stats_ref["min"] is not None and stats_curr["min"] is not None and
                    stats_ref["max"] is not None and stats_curr["max"] is not None):
                    ref_range = stats_ref["max"] - stats_ref["min"]
                    curr_range = stats_curr["max"] - stats_curr["min"]
                    if abs(ref_range) > epsilon:
                        # Use log ratio to handle both increases and decreases equally
                        if curr_range > epsilon:
                            log_range_ratio = abs(math.log((curr_range + epsilon) / (ref_range + epsilon)))
                            # Normalize: log(2) means doubled or halved range
                            range_diff = min(log_range_ratio / math.log(2), 1.0)
                            drift_components.append(range_diff)
                    elif abs(curr_range) > epsilon:
                        # If ref_range was near zero but curr_range is not
                        drift_components.append(1.0)
                    
                # 6. Percentile shifts
                if stats_ref["percentiles"] is not None and stats_curr["percentiles"] is not None:
                    ref_percentiles = stats_ref["percentiles"]
                    curr_percentiles = stats_curr["percentiles"]
                    
                    # Check if percentiles are valid arrays
                    if isinstance(ref_percentiles, list) and isinstance(curr_percentiles, list):
                        if len(ref_percentiles) >= 2 and len(curr_percentiles) >= 2:
                            # Calculate IQR
                            ref_iqr = ref_percentiles[1] - ref_percentiles[0]  # 75th - 25th
                            curr_iqr = curr_percentiles[1] - curr_percentiles[0]
                            
                            if abs(ref_iqr) > epsilon:
                                # Use log ratio for IQR
                                if curr_iqr > epsilon:
                                    log_iqr_ratio = abs(math.log((curr_iqr + epsilon) / (ref_iqr + epsilon)))
                                    # Normalize: log(2) means doubled or halved IQR
                                    iqr_diff = min(log_iqr_ratio / math.log(2), 1.0)
                                    drift_components.append(iqr_diff)
                                elif curr_iqr <= epsilon and ref_iqr > epsilon:
                                    # IQR collapsed to near zero
                                    drift_components.append(1.0)
                            
                            # Check shifts in tail percentiles (95th, 99th)
                            for i in range(2, min(len(ref_percentiles), len(curr_percentiles))):
                                if (ref_percentiles[i] is not None and 
                                    curr_percentiles[i] is not None and 
                                    stats_ref["stddev"] is not None and 
                                    stats_ref["stddev"] > epsilon):
                                    
                                    tail_diff = abs(curr_percentiles[i] - ref_percentiles[i]) / stats_ref["stddev"]
                                    drift_components.append(min(tail_diff, 1.0))
                
                # 7. Outlier detection (if requested)
                outlier_metrics = {}
                if detect_outliers:
                    try:
                        # Define outlier bounds based on IQR if we have valid percentiles
                        if (stats_ref["percentiles"] is not None and 
                            isinstance(stats_ref["percentiles"], list) and 
                            len(stats_ref["percentiles"]) >= 2):
                            
                            ref_q1, ref_q3 = stats_ref["percentiles"][0], stats_ref["percentiles"][1]
                            ref_iqr = ref_q3 - ref_q1
                            
                            if ref_iqr > epsilon:
                                ref_lower_bound = ref_q1 - 1.5 * ref_iqr
                                ref_upper_bound = ref_q3 + 1.5 * ref_iqr
                                
                                # Count outliers in both datasets
                                ref_outliers_df = df_ref.filter(
                                    (F.col(col).isNotNull()) & 
                                    ((F.col(col) < ref_lower_bound) | (F.col(col) > ref_upper_bound))
                                )
                                curr_outliers_df = df_curr.filter(
                                    (F.col(col).isNotNull()) & 
                                    ((F.col(col) < ref_lower_bound) | (F.col(col) > ref_upper_bound))
                                )
                                
                                ref_outliers = ref_outliers_df.count()
                                curr_outliers = curr_outliers_df.count()
                                
                                # Calculate outlier rates
                                ref_non_null = stats_ref["count"] - stats_ref["null_count"]
                                curr_non_null = stats_curr["count"] - stats_curr["null_count"]
                                
                                ref_outlier_rate = ref_outliers / ref_non_null if ref_non_null > 0 else 0
                                curr_outlier_rate = curr_outliers / curr_non_null if curr_non_null > 0 else 0
                                
                                # Add to metrics
                                outlier_metrics = {
                                    "ref_outlier_count": ref_outliers,
                                    "curr_outlier_count": curr_outliers,
                                    "ref_outlier_rate": ref_outlier_rate,
                                    "curr_outlier_rate": curr_outlier_rate,
                                    "outlier_rate_diff": abs(curr_outlier_rate - ref_outlier_rate)
                                }
                                
                                # Add to drift components
                                outlier_rate_diff = abs(curr_outlier_rate - ref_outlier_rate)
                                drift_components.append(min(outlier_rate_diff * 10, 1.0))  # Scale up since outlier rates are usually small
                                
                                # Additional analysis - extremely rare values (beyond 3*IQR)
                                ref_extreme_bound_upper = ref_q3 + 3 * ref_iqr
                                ref_extreme_bound_lower = ref_q1 - 3 * ref_iqr
                                
                                ref_extreme_df = df_ref.filter(
                                    (F.col(col).isNotNull()) & 
                                    ((F.col(col) < ref_extreme_bound_lower) | (F.col(col) > ref_extreme_bound_upper))
                                )
                                curr_extreme_df = df_curr.filter(
                                    (F.col(col).isNotNull()) & 
                                    ((F.col(col) < ref_extreme_bound_lower) | (F.col(col) > ref_extreme_bound_upper))
                                )
                                
                                ref_extreme = ref_extreme_df.count()
                                curr_extreme = curr_extreme_df.count()
                                
                                ref_extreme_rate = ref_extreme / ref_non_null if ref_non_null > 0 else 0
                                curr_extreme_rate = curr_extreme / curr_non_null if curr_non_null > 0 else 0
                                
                                # Add to metrics
                                outlier_metrics.update({
                                    "ref_extreme_count": ref_extreme,
                                    "curr_extreme_count": curr_extreme,
                                    "ref_extreme_rate": ref_extreme_rate,
                                    "curr_extreme_rate": curr_extreme_rate,
                                    "extreme_rate_diff": abs(curr_extreme_rate - ref_extreme_rate)
                                })
                                
                                # Add to drift components (with higher weight for extreme values)
                                extreme_rate_diff = abs(curr_extreme_rate - ref_extreme_rate)
                                drift_components.append(min(extreme_rate_diff * 20, 1.0))  # Higher scale for extremes
                    except Exception as e:
                        print(f"Warning: Outlier detection failed for column {col}: {str(e)}")
                
                # Calculate overall drift score (average of components)
                drift_score = sum(drift_components) / len(drift_components) if drift_components else 0.0
                
                # Ensure drift score is in [0,1] range (in case of numerical errors)
                drift_score = max(0.0, min(1.0, drift_score))
                
                # Add detailed metrics for context
                metrics = {
                    "null_fraction_ref": ref_null_fraction,
                    "null_fraction_curr": curr_null_fraction,
                    "mean_ref": stats_ref["mean"],
                    "mean_curr": stats_curr["mean"],
                    "stddev_ref": stats_ref["stddev"],
                    "stddev_curr": stats_curr["stddev"],
                    "median_ref": stats_ref["median"],
                    "median_curr": stats_curr["median"],
                    "min_ref": stats_ref["min"],
                    "min_curr": stats_curr["min"],
                    "max_ref": stats_ref["max"],
                    "max_curr": stats_curr["max"]
                }
                
                # Add percentile metrics if available
                if stats_ref["percentiles"] is not None and stats_curr["percentiles"] is not None:
                    if (isinstance(stats_ref["percentiles"], list) and 
                        isinstance(stats_curr["percentiles"], list) and
                        len(stats_ref["percentiles"]) >= 4 and
                        len(stats_curr["percentiles"]) >= 4):
                        
                        percentile_labels = ["p25", "p75", "p95", "p99"]
                        for i, label in enumerate(percentile_labels):
                            metrics[f"{label}_ref"] = stats_ref["percentiles"][i]
                            metrics[f"{label}_curr"] = stats_curr["percentiles"][i]
                
                # Add outlier metrics if calculated
                if outlier_metrics:
                    metrics.update(outlier_metrics)
                
                # Add result
                results.append({
                    "column_name": col,
                    "column_type": "numerical",
                    "dimension_id": dimension_id,
                    "drift_score": drift_score,
                    "metrics": metrics
                })
                
            except Exception as e:
                print(f"Error analyzing numerical column {col}: {str(e)}")
                # Add a minimal result with error info
                results.append({
                    "column_name": col,
                    "column_type": "numerical",
                    "dimension_id": dimension_id,
                    "drift_score": 0.0,
                    "metrics": {
                        "error": str(e),
                        "analysis_failed": True
                    }
                })
        
        return results 