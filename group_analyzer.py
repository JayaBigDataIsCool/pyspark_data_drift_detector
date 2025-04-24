from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Dict, Any, List, Tuple, Set
import math

class GroupAnalyzer:
    """Module to analyze data drift within groups defined by categorical dimensions"""
    
    @staticmethod
    def analyze_grouped_data(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        group_columns: List[str],
        metrics_columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze data drift within groups defined by categorical dimensions
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            group_columns: List of columns to define groups
            metrics_columns: List of columns to calculate metrics for
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with group analysis results
        """
        results = {
            "group_metrics": {},
            "drifted_groups": [],
            "most_drifted_metrics": {},
            "top_drifted_groups": []
        }
        
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {})
        
        # Skip if no group columns
        if not group_columns:
            results["status"] = "skipped_no_group_columns"
            return results
        
        # Skip if no metrics columns
        if not metrics_columns:
            results["status"] = "skipped_no_metrics_columns"
            return results
        
        try:
            # Analyze each group dimension individually
            for dimension in group_columns:
                if dimension not in df_ref.columns or dimension not in df_curr.columns:
                    continue
                
                # Get top N categories by count to prevent explosion
                top_categories = GroupAnalyzer._get_top_categories(
                    df_ref, df_curr, dimension, top_n=20
                )
                
                dimension_results = {}
                
                # Analyze each category
                for category in top_categories:
                    # Filter dataframes to only include this category
                    ref_filter = df_ref.filter(F.col(dimension) == category)
                    curr_filter = df_curr.filter(F.col(dimension) == category)
                    
                    # Skip if not enough data in either dataset
                    ref_count = ref_filter.count()
                    curr_count = curr_filter.count()
                    if ref_count < 20 or curr_count < 20:
                        continue
                    
                    # Calculate metrics for this group
                    metrics = GroupAnalyzer._calculate_group_metrics(
                        ref_filter, curr_filter, metrics_columns
                    )
                    
                    # Apply group-specific drift detection
                    group_drift_detected = GroupAnalyzer._detect_group_drift(
                        metrics, thresholds
                    )
                    
                    category_key = str(category)
                    dimension_results[category_key] = {
                        "metrics": metrics,
                        "ref_count": ref_count,
                        "curr_count": curr_count,
                        "drift_detected": group_drift_detected,
                        "drift_score": metrics.get("overall_drift_score", 0)
                    }
                    
                    # Add to drifted groups if drift detected
                    if group_drift_detected:
                        results["drifted_groups"].append({
                            "dimension": dimension,
                            "category": category_key,
                            "drift_score": metrics.get("overall_drift_score", 0),
                            "ref_count": ref_count,
                            "curr_count": curr_count
                        })
                
                # Add dimension results to main results
                results["group_metrics"][dimension] = dimension_results
                
                # Calculate if dimension as a whole has drift
                dimension_drift_detected = any(
                    group.get("drift_detected", False) 
                    for group in dimension_results.values()
                )
                results["group_metrics"][dimension]["drift_detected"] = dimension_drift_detected
            
            # Find most drifted metrics across all groups
            all_metrics = {}
            for dimension, groups in results["group_metrics"].items():
                for category, group_data in groups.items():
                    if category == "drift_detected":
                        continue
                    
                    metrics = group_data.get("metrics", {})
                    for metric_name, metric_value in metrics.items():
                        if metric_name not in ["overall_drift_score", "metrics_with_drift"]:
                            if metric_name not in all_metrics:
                                all_metrics[metric_name] = []
                            
                            # Store metric, dimension, category, and drift value
                            if isinstance(metric_value, dict) and "drift" in metric_value:
                                drift = metric_value["drift"]
                                all_metrics[metric_name].append({
                                    "dimension": dimension,
                                    "category": category,
                                    "drift": drift
                                })
            
            # Find top drifted metrics
            most_drifted = {}
            for metric_name, drift_values in all_metrics.items():
                sorted_drifts = sorted(drift_values, key=lambda x: x["drift"], reverse=True)
                top_drifts = sorted_drifts[:5]  # Get top 5
                if top_drifts and top_drifts[0]["drift"] > 0:
                    most_drifted[metric_name] = top_drifts
            
            results["most_drifted_metrics"] = most_drifted
            
            # Sort drifted groups by drift score
            sorted_drifted = sorted(
                results["drifted_groups"], 
                key=lambda x: x["drift_score"], 
                reverse=True
            )
            results["top_drifted_groups"] = sorted_drifted[:10]  # Top 10 drifted groups
            
            # Generate overall status
            if results["drifted_groups"]:
                results["status"] = "drift_detected"
            else:
                results["status"] = "success"
            
            return results
            
        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
            return results
    
    @staticmethod
    def _get_top_categories(
        df_ref: DataFrame,
        df_curr: DataFrame,
        column: str,
        top_n: int = 20
    ) -> List[Any]:
        """Get top categories by count from both datasets combined
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            column: Categorical column to analyze
            top_n: Number of top categories to return
            
        Returns:
            List of top category values
        """
        # Count occurrences in reference data
        ref_counts = df_ref.groupBy(column).count().collect()
        ref_dict = {row[column]: row["count"] for row in ref_counts}
        
        # Count occurrences in current data
        curr_counts = df_curr.groupBy(column).count().collect()
        curr_dict = {row[column]: row["count"] for row in curr_counts}
        
        # Combine counts
        combined_counts = {}
        all_categories = set(ref_dict.keys()) | set(curr_dict.keys())
        
        for category in all_categories:
            combined_counts[category] = ref_dict.get(category, 0) + curr_dict.get(category, 0)
        
        # Sort by count and get top N
        top_categories = sorted(
            combined_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        return [category for category, _ in top_categories]
    
    @staticmethod
    def _calculate_group_metrics(
        df_ref: DataFrame,
        df_curr: DataFrame,
        metrics_columns: List[str]
    ) -> Dict[str, Any]:
        """Calculate metrics for a group
        
        Args:
            df_ref: Reference DataFrame filtered to the group
            df_curr: Current DataFrame filtered to the group
            metrics_columns: List of columns to calculate metrics for
            
        Returns:
            Dictionary with metrics for the group
        """
        metrics = {}
        
        # Skip if no columns or empty dataframes
        if not metrics_columns or df_ref.count() == 0 or df_curr.count() == 0:
            return {"error": "Insufficient data"}
        
        # Calculate overall statistics
        metrics["row_count"] = {
            "ref": df_ref.count(),
            "curr": df_curr.count(),
            "pct_change": GroupAnalyzer._calculate_percent_change(
                df_ref.count(), df_curr.count())
        }
        
        metrics["null_counts"] = {}
        metrics["numeric_stats"] = {}
        metrics["categorical_counts"] = {}
        
        # Track drift values for overall score
        drift_values = []
        metrics_with_drift = []
        
        # Calculate metrics for each column
        for column in metrics_columns:
            if column not in df_ref.columns or column not in df_curr.columns:
                continue
            
            # Get column data type
            col_type = None
            for field in df_ref.schema.fields:
                if field.name == column:
                    data_type = field.dataType.simpleString().lower()
                    if any(t in data_type for t in ["int", "long", "float", "double", "decimal"]):
                        col_type = "numeric"
                    else:
                        col_type = "categorical"
                    break
            
            if not col_type:
                continue
                
            # Calculate null counts
            ref_null_count = df_ref.filter(F.col(column).isNull()).count()
            curr_null_count = df_curr.filter(F.col(column).isNull()).count()
            
            ref_null_pct = ref_null_count / df_ref.count() if df_ref.count() > 0 else 0
            curr_null_pct = curr_null_count / df_curr.count() if df_curr.count() > 0 else 0
            
            null_drift = abs(curr_null_pct - ref_null_pct)
            
            metrics["null_counts"][column] = {
                "ref": ref_null_count,
                "curr": curr_null_count,
                "ref_pct": ref_null_pct,
                "curr_pct": curr_null_pct,
                "drift": null_drift
            }
            
            drift_values.append(null_drift)
            if null_drift > 0.05:  # 5% threshold for null drift
                metrics_with_drift.append(f"{column}_null")
            
            # Calculate type-specific metrics
            if col_type == "numeric":
                # Skip if too many nulls
                if ref_null_pct > 0.9 or curr_null_pct > 0.9:
                    continue
                
                try:
                    # Calculate basic statistics
                    ref_stats = df_ref.select(
                        F.avg(F.col(column)).alias("mean"),
                        F.stddev(F.col(column)).alias("stddev"),
                        F.min(F.col(column)).alias("min"),
                        F.max(F.col(column)).alias("max"),
                        F.expr(f"percentile({column}, 0.5)").alias("median")
                    ).collect()[0]
                    
                    curr_stats = df_curr.select(
                        F.avg(F.col(column)).alias("mean"),
                        F.stddev(F.col(column)).alias("stddev"),
                        F.min(F.col(column)).alias("min"),
                        F.max(F.col(column)).alias("max"),
                        F.expr(f"percentile({column}, 0.5)").alias("median")
                    ).collect()[0]
                    
                    # Handle nulls in stats
                    ref_mean = ref_stats["mean"] or 0
                    ref_stddev = ref_stats["stddev"] or 0
                    ref_min = ref_stats["min"] or 0
                    ref_max = ref_stats["max"] or 0
                    ref_median = ref_stats["median"] or 0
                    
                    curr_mean = curr_stats["mean"] or 0
                    curr_stddev = curr_stats["stddev"] or 0
                    curr_min = curr_stats["min"] or 0
                    curr_max = curr_stats["max"] or 0
                    curr_median = curr_stats["median"] or 0
                    
                    # Calculate relative differences
                    mean_change = GroupAnalyzer._calculate_percent_change(ref_mean, curr_mean)
                    stddev_change = GroupAnalyzer._calculate_percent_change(ref_stddev, curr_stddev)
                    median_change = GroupAnalyzer._calculate_percent_change(ref_median, curr_median)
                    
                    # Calculate range changes
                    ref_range = ref_max - ref_min
                    curr_range = curr_max - curr_min
                    range_change = GroupAnalyzer._calculate_percent_change(ref_range, curr_range)
                    
                    # Store metrics
                    metrics["numeric_stats"][column] = {
                        "mean": {
                            "ref": ref_mean,
                            "curr": curr_mean,
                            "pct_change": mean_change,
                            "drift": abs(mean_change)
                        },
                        "stddev": {
                            "ref": ref_stddev,
                            "curr": curr_stddev,
                            "pct_change": stddev_change,
                            "drift": abs(stddev_change)
                        },
                        "median": {
                            "ref": ref_median,
                            "curr": curr_median,
                            "pct_change": median_change,
                            "drift": abs(median_change)
                        },
                        "range": {
                            "ref": ref_range,
                            "curr": curr_range,
                            "pct_change": range_change,
                            "drift": abs(range_change)
                        }
                    }
                    
                    # Add to drift values
                    drift_values.append(abs(mean_change))
                    drift_values.append(abs(stddev_change))
                    drift_values.append(abs(median_change))
                    
                    # Track metrics with significant drift
                    if abs(mean_change) > 0.1:  # 10% threshold
                        metrics_with_drift.append(f"{column}_mean")
                    if abs(stddev_change) > 0.2:  # 20% threshold
                        metrics_with_drift.append(f"{column}_stddev")
                    if abs(median_change) > 0.1:  # 10% threshold
                        metrics_with_drift.append(f"{column}_median")
                
                except Exception as e:
                    metrics["numeric_stats"][column] = {"error": str(e)}
            
            elif col_type == "categorical":
                try:
                    # Calculate top categories and their frequencies
                    ref_counts = df_ref.groupBy(column).count() \
                        .withColumn("frequency", 
                                   F.col("count") / F.sum("count").over(Window.partitionBy())) \
                        .orderBy(F.desc("count")) \
                        .limit(10) \
                        .collect()
                    
                    curr_counts = df_curr.groupBy(column).count() \
                        .withColumn("frequency", 
                                   F.col("count") / F.sum("count").over(Window.partitionBy())) \
                        .orderBy(F.desc("count")) \
                        .limit(10) \
                        .collect()
                    
                    # Convert to dictionaries for easier comparison
                    ref_dict = {str(row[column]): row["frequency"] for row in ref_counts}
                    curr_dict = {str(row[column]): row["frequency"] for row in curr_counts}
                    
                    # Find common categories
                    common_categories = set(ref_dict.keys()) & set(curr_dict.keys())
                    
                    # Calculate average frequency drift
                    if common_categories:
                        freq_drifts = [
                            abs(curr_dict[cat] - ref_dict[cat]) 
                            for cat in common_categories
                        ]
                        avg_freq_drift = sum(freq_drifts) / len(freq_drifts)
                    else:
                        avg_freq_drift = 1.0  # Maximum drift if no common categories
                    
                    # Calculate new and disappeared categories
                    new_categories = set(curr_dict.keys()) - set(ref_dict.keys())
                    disappeared_categories = set(ref_dict.keys()) - set(curr_dict.keys())
                    
                    # Store metrics
                    metrics["categorical_counts"][column] = {
                        "distinct_count": {
                            "ref": len(ref_dict),
                            "curr": len(curr_dict),
                            "pct_change": GroupAnalyzer._calculate_percent_change(
                                len(ref_dict), len(curr_dict))
                        },
                        "common_categories_count": len(common_categories),
                        "new_categories_count": len(new_categories),
                        "disappeared_categories_count": len(disappeared_categories),
                        "avg_frequency_drift": avg_freq_drift,
                        "drift": avg_freq_drift
                    }
                    
                    # Add to drift values
                    drift_values.append(avg_freq_drift)
                    
                    # Track metrics with significant drift
                    if avg_freq_drift > 0.1:  # 10% threshold
                        metrics_with_drift.append(f"{column}_frequency")
                
                except Exception as e:
                    metrics["categorical_counts"][column] = {"error": str(e)}
        
        # Calculate overall drift score
        if drift_values:
            overall_drift_score = sum(drift_values) / len(drift_values)
            metrics["overall_drift_score"] = min(1.0, overall_drift_score)
        else:
            metrics["overall_drift_score"] = 0
        
        metrics["metrics_with_drift"] = metrics_with_drift
        
        return metrics
    
    @staticmethod
    def _detect_group_drift(
        metrics: Dict[str, Any],
        thresholds: Dict[str, Any]
    ) -> bool:
        """Detect if a group has significant drift
        
        Args:
            metrics: Dictionary with group metrics
            thresholds: Thresholds from configuration
            
        Returns:
            Boolean indicating if drift was detected
        """
        # Check for drift based on metrics
        if not metrics or "overall_drift_score" not in metrics:
            return False
        
        # Check overall drift score
        overall_threshold = thresholds.get("group_drift_threshold", 0.1)
        if metrics["overall_drift_score"] >= overall_threshold:
            return True
        
        # Check number of metrics with drift
        if len(metrics.get("metrics_with_drift", [])) >= 3:
            return True
        
        # Check specific metrics that might indicate drift
        
        # Check row count change
        row_count = metrics.get("row_count", {})
        row_pct_change = row_count.get("pct_change", 0)
        if abs(row_pct_change) >= 0.25:  # 25% change in row count
            return True
        
        # Check null count drift
        for col, null_metrics in metrics.get("null_counts", {}).items():
            null_drift = null_metrics.get("drift", 0)
            if null_drift >= 0.1:  # 10% change in null proportion
                return True
        
        # Check numeric stats drift
        for col, num_metrics in metrics.get("numeric_stats", {}).items():
            # Check mean drift
            mean = num_metrics.get("mean", {})
            if abs(mean.get("pct_change", 0)) >= 0.2:  # 20% change in mean
                return True
            
            # Check median drift
            median = num_metrics.get("median", {})
            if abs(median.get("pct_change", 0)) >= 0.2:  # 20% change in median
                return True
        
        # Check categorical frequency drift
        for col, cat_metrics in metrics.get("categorical_counts", {}).items():
            freq_drift = cat_metrics.get("avg_frequency_drift", 0)
            if freq_drift >= 0.15:  # 15% drift in category frequencies
                return True
            
            # Check for significant category changes
            distinct_count = cat_metrics.get("distinct_count", {})
            pct_change = abs(distinct_count.get("pct_change", 0))
            if pct_change >= 0.25:  # 25% change in distinct count
                return True
        
        return False
    
    @staticmethod
    def _calculate_percent_change(old_value: float, new_value: float) -> float:
        """Calculate percent change between two values
        
        Args:
            old_value: Reference value
            new_value: Current value
            
        Returns:
            Percent change as a decimal
        """
        if old_value == 0:
            if new_value == 0:
                return 0
            else:
                return 1  # 100% change if old was 0 and new is not
        
        return (new_value - old_value) / abs(old_value) 