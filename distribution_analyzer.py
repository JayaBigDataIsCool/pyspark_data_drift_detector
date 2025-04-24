from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Dict, Any, List, Tuple
import math

class DistributionAnalyzer:
    """Module to analyze data distributions and detect abnormalities"""
    
    @staticmethod
    def analyze_distributions(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        numerical_columns: List[str],
        categorical_columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze distributions in datasets and detect abnormalities
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            numerical_columns: List of numerical columns to analyze
            categorical_columns: List of categorical columns to analyze
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with distribution analysis results
        """
        results = {
            "numerical_distribution_changes": {},
            "categorical_distribution_changes": {},
            "quantile_shifts": {},
            "rare_value_changes": {},
            "distribution_summaries": {}
        }
        
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {}).get(profile, {})
        
        # Analyze numerical distributions
        if numerical_columns and thresholds.get("analyze_distributions", True):
            
            # Calculate quantile shifts for numerical columns
            quantile_results = DistributionAnalyzer._analyze_quantile_shifts(
                df_ref, df_curr, numerical_columns, 
                config.get("quantiles", [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
            )
            results["quantile_shifts"] = quantile_results
            
            # Analyze distribution shape changes (skewness, kurtosis)
            shape_results = DistributionAnalyzer._analyze_distribution_shapes(
                df_ref, df_curr, numerical_columns
            )
            results["numerical_distribution_changes"] = shape_results
            
            # Generate distribution summaries
            if thresholds.get("gen_distribution_summaries", False):
                summaries = DistributionAnalyzer._generate_distribution_summaries(
                    df_ref, df_curr, numerical_columns
                )
                results["distribution_summaries"].update(summaries)
        
        # Analyze categorical distributions
        if categorical_columns and thresholds.get("analyze_distributions", True):
            # Analyze categorical distributions
            cat_results = DistributionAnalyzer._analyze_categorical_distributions(
                df_ref, df_curr, categorical_columns,
                js_distance_threshold=thresholds.get("js_distance_threshold", 0.1)
            )
            results["categorical_distribution_changes"] = cat_results
            
            # Analyze rare value changes
            if thresholds.get("detect_rare_values", False):
                rare_value_results = DistributionAnalyzer._analyze_rare_value_changes(
                    df_ref, df_curr, categorical_columns,
                    rare_threshold=thresholds.get("rare_value_threshold", 0.01)
                )
                results["rare_value_changes"] = rare_value_results
        
        return results
    
    @staticmethod
    def _analyze_quantile_shifts(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        columns: List[str],
        quantiles: List[float] = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    ) -> Dict[str, Any]:
        """Analyze shifts in quantiles between datasets
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of numerical columns to analyze
            quantiles: List of quantiles to compute
            
        Returns:
            Dictionary with quantile shift analysis by column
        """
        results = {}
        
        for column in columns:
            try:
                # Compute approx quantiles for reference dataset
                ref_quantiles = df_ref.approxQuantile(column, quantiles, 0.01)
                
                # Compute approx quantiles for current dataset
                curr_quantiles = df_curr.approxQuantile(column, quantiles, 0.01)
                
                # Calculate absolute and relative differences
                abs_diffs = [abs(c - r) for c, r in zip(curr_quantiles, ref_quantiles)]
                rel_diffs = []
                
                for i, (r, c) in enumerate(zip(ref_quantiles, curr_quantiles)):
                    if r != 0:
                        rel_diffs.append(abs((c - r) / r))
                    else:
                        # When reference value is 0, use current value or small epsilon
                        denominator = max(abs(c), 1e-10)
                        rel_diffs.append(abs(c - r) / denominator if c != 0 else 0)
                
                # Find most shifted quantiles
                max_abs_shift_idx = abs_diffs.index(max(abs_diffs))
                max_rel_shift_idx = rel_diffs.index(max(rel_diffs))
                
                results[column] = {
                    "ref_quantiles": dict(zip([str(q) for q in quantiles], ref_quantiles)),
                    "curr_quantiles": dict(zip([str(q) for q in quantiles], curr_quantiles)),
                    "abs_diffs": dict(zip([str(q) for q in quantiles], abs_diffs)),
                    "rel_diffs": dict(zip([str(q) for q in quantiles], rel_diffs)),
                    "max_abs_shift": {
                        "quantile": str(quantiles[max_abs_shift_idx]),
                        "ref_value": ref_quantiles[max_abs_shift_idx],
                        "curr_value": curr_quantiles[max_abs_shift_idx],
                        "abs_diff": abs_diffs[max_abs_shift_idx]
                    },
                    "max_rel_shift": {
                        "quantile": str(quantiles[max_rel_shift_idx]),
                        "ref_value": ref_quantiles[max_rel_shift_idx],
                        "curr_value": curr_quantiles[max_rel_shift_idx],
                        "rel_diff": rel_diffs[max_rel_shift_idx]
                    }
                }
            except Exception as e:
                results[column] = {
                    "error": str(e),
                    "status": "failed"
                }
        
        return results
    
    @staticmethod
    def _analyze_distribution_shapes(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        columns: List[str]
    ) -> Dict[str, Any]:
        """Analyze changes in distribution shapes (skewness, kurtosis)
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of numerical columns to analyze
            
        Returns:
            Dictionary with distribution shape analysis by column
        """
        results = {}
        
        for column in columns:
            try:
                # Calculate skewness and kurtosis for reference dataset
                ref_stats = df_ref.select(
                    F.skewness(F.col(column)).alias("skewness"),
                    F.kurtosis(F.col(column)).alias("kurtosis")
                ).collect()[0]
                
                ref_skewness = ref_stats["skewness"] or 0
                ref_kurtosis = ref_stats["kurtosis"] or 0
                
                # Calculate skewness and kurtosis for current dataset
                curr_stats = df_curr.select(
                    F.skewness(F.col(column)).alias("skewness"),
                    F.kurtosis(F.col(column)).alias("kurtosis")
                ).collect()[0]
                
                curr_skewness = curr_stats["skewness"] or 0
                curr_kurtosis = curr_stats["kurtosis"] or 0
                
                # Calculate differences
                skew_diff = abs(curr_skewness - ref_skewness)
                kurt_diff = abs(curr_kurtosis - ref_kurtosis)
                
                # Classify distribution changes
                skew_change = "none"
                if skew_diff > 0.5:
                    if curr_skewness > ref_skewness:
                        skew_change = "more_right_skewed"
                    else:
                        skew_change = "more_left_skewed"
                
                kurt_change = "none"
                if kurt_diff > 1.0:
                    if curr_kurtosis > ref_kurtosis:
                        kurt_change = "more_outliers"
                    else:
                        kurt_change = "fewer_outliers"
                
                results[column] = {
                    "ref_skewness": ref_skewness,
                    "curr_skewness": curr_skewness,
                    "skew_diff": skew_diff,
                    "ref_kurtosis": ref_kurtosis,
                    "curr_kurtosis": curr_kurtosis,
                    "kurt_diff": kurt_diff,
                    "skew_change": skew_change,
                    "kurt_change": kurt_change,
                }
                
            except Exception as e:
                results[column] = {
                    "error": str(e),
                    "status": "failed"
                }
        
        return results
    
    @staticmethod
    def _analyze_categorical_distributions(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        columns: List[str],
        js_distance_threshold: float = 0.1
    ) -> Dict[str, Any]:
        """Analyze changes in categorical distributions using Jensen-Shannon distance
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of categorical columns to analyze
            js_distance_threshold: Threshold for significant JS distance
            
        Returns:
            Dictionary with categorical distribution analysis by column
        """
        results = {}
        
        for column in columns:
            try:
                # Calculate frequency distributions for reference dataset
                ref_dist = df_ref.groupBy(column) \
                    .agg(F.count("*").alias("count")) \
                    .withColumn("frequency", 
                                F.col("count") / F.sum("count").over(Window.partitionBy())) \
                    .select(column, "frequency") \
                    .collect()
                
                ref_dist_dict = {str(row[column]): row["frequency"] for row in ref_dist}
                
                # Calculate frequency distributions for current dataset
                curr_dist = df_curr.groupBy(column) \
                    .agg(F.count("*").alias("count")) \
                    .withColumn("frequency", 
                                F.col("count") / F.sum("count").over(Window.partitionBy())) \
                    .select(column, "frequency") \
                    .collect()
                
                curr_dist_dict = {str(row[column]): row["frequency"] for row in curr_dist}
                
                # Get all unique values from both distributions
                all_values = set(ref_dist_dict.keys()) | set(curr_dist_dict.keys())
                
                # Calculate JS distance
                js_distance = DistributionAnalyzer._jensen_shannon_distance(
                    ref_dist_dict, curr_dist_dict, all_values
                )
                
                # Find new and disappeared values
                new_values = set(curr_dist_dict.keys()) - set(ref_dist_dict.keys())
                disappeared_values = set(ref_dist_dict.keys()) - set(curr_dist_dict.keys())
                
                # Calculate top increased and decreased values
                common_values = set(ref_dist_dict.keys()) & set(curr_dist_dict.keys())
                diffs = {}
                
                for value in common_values:
                    diffs[value] = curr_dist_dict[value] - ref_dist_dict[value]
                
                top_increased = sorted(
                    [(k, v) for k, v in diffs.items() if v > 0],
                    key=lambda x: x[1], reverse=True
                )[:5]
                
                top_decreased = sorted(
                    [(k, v) for k, v in diffs.items() if v < 0],
                    key=lambda x: x[1]
                )[:5]
                
                results[column] = {
                    "js_distance": js_distance,
                    "significant_change": js_distance > js_distance_threshold,
                    "new_values": list(new_values),
                    "disappeared_values": list(disappeared_values),
                    "top_increased": [
                        {"value": k, "increase": v} for k, v in top_increased
                    ],
                    "top_decreased": [
                        {"value": k, "decrease": abs(v)} for k, v in top_decreased
                    ]
                }
                
            except Exception as e:
                results[column] = {
                    "error": str(e),
                    "status": "failed"
                }
        
        return results
    
    @staticmethod
    def _analyze_rare_value_changes(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        columns: List[str],
        rare_threshold: float = 0.01
    ) -> Dict[str, Any]:
        """Analyze changes in rare values (values with low frequency)
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of categorical columns to analyze
            rare_threshold: Threshold for considering a value rare
            
        Returns:
            Dictionary with rare value analysis by column
        """
        results = {}
        
        for column in columns:
            try:
                # Calculate frequency distributions for reference dataset
                ref_dist = df_ref.groupBy(column) \
                    .agg(F.count("*").alias("count")) \
                    .withColumn("frequency", 
                                F.col("count") / F.sum("count").over(Window.partitionBy())) \
                    .select(column, "frequency", "count") \
                    .collect()
                
                ref_dist_dict = {str(row[column]): row["frequency"] for row in ref_dist}
                ref_count_dict = {str(row[column]): row["count"] for row in ref_dist}
                
                # Calculate frequency distributions for current dataset
                curr_dist = df_curr.groupBy(column) \
                    .agg(F.count("*").alias("count")) \
                    .withColumn("frequency", 
                                F.col("count") / F.sum("count").over(Window.partitionBy())) \
                    .select(column, "frequency", "count") \
                    .collect()
                
                curr_dist_dict = {str(row[column]): row["frequency"] for row in curr_dist}
                curr_count_dict = {str(row[column]): row["count"] for row in curr_dist}
                
                # Find rare values in both datasets
                ref_rare = {k: v for k, v in ref_dist_dict.items() if v <= rare_threshold}
                curr_rare = {k: v for k, v in curr_dist_dict.items() if v <= rare_threshold}
                
                # Find new rare values and disappeared rare values
                new_rare = {
                    k: v for k, v in curr_rare.items() 
                    if k not in ref_rare and k in ref_dist_dict
                }
                
                disappeared_rare = {
                    k: v for k, v in ref_rare.items() 
                    if k not in curr_rare and k in curr_dist_dict
                }
                
                # Calculate the change in number of rare values
                ref_rare_count = len(ref_rare)
                curr_rare_count = len(curr_rare)
                rare_count_change = curr_rare_count - ref_rare_count
                
                results[column] = {
                    "ref_rare_count": ref_rare_count,
                    "curr_rare_count": curr_rare_count,
                    "rare_count_change": rare_count_change,
                    "new_rare_values": [
                        {
                            "value": k, 
                            "prev_freq": ref_dist_dict.get(k, 0),
                            "curr_freq": v,
                            "prev_count": ref_count_dict.get(k, 0),
                            "curr_count": curr_count_dict.get(k, 0)
                        } 
                        for k, v in new_rare.items()
                    ],
                    "disappeared_rare_values": [
                        {
                            "value": k, 
                            "prev_freq": v,
                            "curr_freq": curr_dist_dict.get(k, 0),
                            "prev_count": ref_count_dict.get(k, 0),
                            "curr_count": curr_count_dict.get(k, 0)
                        } 
                        for k, v in disappeared_rare.items()
                    ]
                }
                
            except Exception as e:
                results[column] = {
                    "error": str(e),
                    "status": "failed"
                }
        
        return results
    
    @staticmethod
    def _generate_distribution_summaries(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        columns: List[str]
    ) -> Dict[str, Any]:
        """Generate summaries of data distributions for visualization
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of numerical columns to analyze
            
        Returns:
            Dictionary with distribution summaries by column
        """
        results = {}
        
        for column in columns:
            try:
                # Generate histogram data for reference dataset
                ref_hist = df_ref.select(column) \
                    .rdd \
                    .flatMap(lambda x: [float(x[0])] if x[0] is not None else []) \
                    .histogram(10)
                
                # Generate histogram data for current dataset
                curr_hist = df_curr.select(column) \
                    .rdd \
                    .flatMap(lambda x: [float(x[0])] if x[0] is not None else []) \
                    .histogram(10)
                
                # Format histogram data for easy visualization
                ref_hist_data = []
                for i in range(len(ref_hist[0]) - 1):
                    ref_hist_data.append({
                        "bin_min": ref_hist[0][i],
                        "bin_max": ref_hist[0][i + 1],
                        "count": ref_hist[1][i]
                    })
                
                curr_hist_data = []
                for i in range(len(curr_hist[0]) - 1):
                    curr_hist_data.append({
                        "bin_min": curr_hist[0][i],
                        "bin_max": curr_hist[0][i + 1],
                        "count": curr_hist[1][i]
                    })
                
                results[column] = {
                    "ref_histogram": ref_hist_data,
                    "curr_histogram": curr_hist_data
                }
                
            except Exception as e:
                results[column] = {
                    "error": str(e),
                    "status": "failed"
                }
        
        return results
    
    @staticmethod
    def _jensen_shannon_distance(
        dist1: Dict[str, float], 
        dist2: Dict[str, float], 
        all_values: set
    ) -> float:
        """Calculate Jensen-Shannon distance between two distributions
        
        Args:
            dist1: First distribution as dictionary {value: probability}
            dist2: Second distribution as dictionary {value: probability}
            all_values: Set of all possible values
            
        Returns:
            Jensen-Shannon distance [0, 1] where 0 means identical distributions
        """
        # Helper function to calculate entropy
        def entropy(probs):
            return -sum(p * math.log2(p) if p > 0 else 0 for p in probs)
        
        # Get probabilities for all values in both distributions
        p1 = [dist1.get(val, 0) for val in all_values]
        p2 = [dist2.get(val, 0) for val in all_values]
        
        # Calculate midpoint distribution
        m = [(p1[i] + p2[i]) / 2 for i in range(len(all_values))]
        
        # Calculate JS divergence
        js_divergence = (entropy(m) - (entropy(p1) + entropy(p2)) / 2)
        
        # Convert to distance (square root of divergence)
        js_distance = math.sqrt(max(0, js_divergence))  # Ensure non-negative
        
        return js_distance 