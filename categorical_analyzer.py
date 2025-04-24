from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Dict, Any, List, Tuple, Set
import math

class CategoricalAnalyzer:
    """Module for analyzing categorical columns and detecting drift"""
    
    @staticmethod
    def analyze_categorical_columns(
        df_ref: DataFrame,
        df_curr: DataFrame,
        columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Analyze categorical columns for drift
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            columns: List of categorical columns to analyze
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with analysis results by column
        """
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {}).get(profile, {}).get("categorical", {})
        
        # Default thresholds if not specified
        category_threshold = thresholds.get("category_threshold", 0.03)
        chi_square_pvalue = thresholds.get("chi_square_pvalue", 0.05)
        null_threshold = thresholds.get("null_threshold", 0.005)
        
        # Initialize results dictionary
        results = {}
        
        # Analyze each categorical column
        for column in columns:
            # Skip columns not in both dataframes
            if column not in df_ref.columns or column not in df_curr.columns:
                continue
            
            try:
                # Calculate category distributions
                ref_dist = CategoricalAnalyzer._calculate_distribution(df_ref, column)
                curr_dist = CategoricalAnalyzer._calculate_distribution(df_curr, column)
                
                # Calculate distribution changes
                dist_changes = CategoricalAnalyzer._calculate_distribution_changes(
                    ref_dist, curr_dist
                )
                
                # Calculate JS divergence
                js_divergence = CategoricalAnalyzer._calculate_js_divergence(
                    ref_dist["distribution"], curr_dist["distribution"]
                )
                
                # Calculate chi-square test (if possible)
                chi_square_results = CategoricalAnalyzer._calculate_chi_square(
                    df_ref, df_curr, column
                )
                
                # Check if drift is detected
                # Drift is considered detected if any of the following conditions are met:
                # 1. JS divergence is above threshold
                # 2. Chi-square p-value is below threshold
                # 3. Significant change in null proportion
                # 4. Significant change in categories (new/missing categories above threshold)
                
                js_drift = js_divergence > category_threshold
                chi_drift = chi_square_results.get("p_value", 1.0) < chi_square_pvalue
                null_drift = abs(dist_changes.get("null_proportion_diff", 0)) > null_threshold
                category_drift = (
                    dist_changes.get("new_categories_ratio", 0) > category_threshold or
                    dist_changes.get("missing_categories_ratio", 0) > category_threshold
                )
                
                drift_detected = js_drift or chi_drift or null_drift or category_drift
                
                # Determine drift causes
                drift_causes = []
                if js_drift:
                    drift_causes.append("distribution_change")
                if chi_drift:
                    drift_causes.append("statistical_significance")
                if null_drift:
                    drift_causes.append("null_proportion")
                if category_drift:
                    if dist_changes.get("new_categories_ratio", 0) > category_threshold:
                        drift_causes.append("new_categories")
                    if dist_changes.get("missing_categories_ratio", 0) > category_threshold:
                        drift_causes.append("missing_categories")
                
                # Calculate overall drift score
                drift_score = CategoricalAnalyzer._calculate_drift_score(
                    js_divergence, 
                    chi_square_results.get("p_value", 1.0),
                    dist_changes.get("null_proportion_diff", 0),
                    dist_changes.get("new_categories_ratio", 0),
                    dist_changes.get("missing_categories_ratio", 0)
                )
                
                # Store all results for this column
                results[column] = {
                    "ref_stats": ref_dist,
                    "curr_stats": curr_dist,
                    "distribution_changes": dist_changes,
                    "js_divergence": js_divergence,
                    "chi_square_results": chi_square_results,
                    "drift_detected": drift_detected,
                    "drift_causes": drift_causes,
                    "drift_score": drift_score
                }
                
            except Exception as e:
                # Store error for this column
                results[column] = {
                    "error": str(e),
                    "drift_detected": False
                }
        
        return results
    
    @staticmethod
    def _calculate_distribution(df: DataFrame, column: str) -> Dict[str, Any]:
        """Calculate distribution of a categorical column
        
        Args:
            df: DataFrame to analyze
            column: Column name
            
        Returns:
            Dictionary with distribution statistics
        """
        # Count total rows and null values
        total_count = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        
        # Calculate null proportion
        null_proportion = null_count / total_count if total_count > 0 else 0
        
        # Calculate value counts
        value_counts = df.filter(F.col(column).isNotNull()) \
            .groupBy(column) \
            .count() \
            .orderBy(F.desc("count"))
        
        # Get top categories
        top_categories = value_counts.limit(20).collect()
        
        # Calculate distinct count
        distinct_count = value_counts.count()
        
        # Calculate distribution (probability of each value)
        distribution = {}
        for row in top_categories:
            value = row[column]
            count = row["count"]
            distribution[str(value)] = count / (total_count - null_count) if (total_count - null_count) > 0 else 0
        
        # Calculate entropy
        entropy = 0
        for prob in distribution.values():
            if prob > 0:
                entropy -= prob * math.log2(prob)
        
        return {
            "total_count": total_count,
            "null_count": null_count,
            "null_proportion": null_proportion,
            "distinct_count": distinct_count,
            "top_categories": [
                {"value": str(row[column]), "count": row["count"]} 
                for row in top_categories
            ],
            "distribution": distribution,
            "entropy": entropy
        }
    
    @staticmethod
    def _calculate_distribution_changes(
        ref_dist: Dict[str, Any],
        curr_dist: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate changes between two distributions
        
        Args:
            ref_dist: Reference distribution
            curr_dist: Current distribution
            
        Returns:
            Dictionary with distribution changes
        """
        # Get distribution dictionaries
        ref_distribution = ref_dist["distribution"]
        curr_distribution = curr_dist["distribution"]
        
        # Find new, missing, and common categories
        ref_categories = set(ref_distribution.keys())
        curr_categories = set(curr_distribution.keys())
        
        new_categories = curr_categories - ref_categories
        missing_categories = ref_categories - curr_categories
        common_categories = ref_categories & curr_categories
        
        # Calculate category ratios
        new_categories_ratio = len(new_categories) / len(ref_categories) if ref_categories else 0
        missing_categories_ratio = len(missing_categories) / len(ref_categories) if ref_categories else 0
        
        # Calculate changes in top categories
        top_category_changes = {}
        for cat in common_categories:
            ref_prob = ref_distribution.get(cat, 0)
            curr_prob = curr_distribution.get(cat, 0)
            abs_diff = curr_prob - ref_prob
            rel_diff = abs_diff / ref_prob if ref_prob > 0 else (1.0 if curr_prob > 0 else 0.0)
            
            top_category_changes[cat] = {
                "ref_prob": ref_prob,
                "curr_prob": curr_prob,
                "abs_diff": abs_diff,
                "rel_diff": rel_diff
            }
        
        # Sort categories by absolute difference
        categories_by_change = sorted(
            top_category_changes.items(),
            key=lambda x: abs(x[1]["abs_diff"]),
            reverse=True
        )
        
        top_changes = [
            {"category": cat, **changes}
            for cat, changes in categories_by_change[:10]  # Top 10 changes
        ]
        
        # Calculate null proportion difference
        null_proportion_diff = curr_dist["null_proportion"] - ref_dist["null_proportion"]
        
        # Calculate entropy difference
        entropy_diff = curr_dist["entropy"] - ref_dist["entropy"]
        entropy_rel_diff = entropy_diff / ref_dist["entropy"] if ref_dist["entropy"] > 0 else 0
        
        # Calculate distinct count difference
        distinct_count_diff = curr_dist["distinct_count"] - ref_dist["distinct_count"]
        distinct_count_rel_diff = (
            distinct_count_diff / ref_dist["distinct_count"] 
            if ref_dist["distinct_count"] > 0 else 0
        )
        
        return {
            "new_categories": list(new_categories),
            "new_categories_count": len(new_categories),
            "new_categories_ratio": new_categories_ratio,
            "missing_categories": list(missing_categories),
            "missing_categories_count": len(missing_categories),
            "missing_categories_ratio": missing_categories_ratio,
            "common_categories_count": len(common_categories),
            "top_category_changes": top_changes,
            "null_proportion_diff": null_proportion_diff,
            "entropy_diff": entropy_diff,
            "entropy_rel_diff": entropy_rel_diff,
            "distinct_count_diff": distinct_count_diff,
            "distinct_count_rel_diff": distinct_count_rel_diff
        }
    
    @staticmethod
    def _calculate_js_divergence(
        dist1: Dict[str, float],
        dist2: Dict[str, float]
    ) -> float:
        """Calculate Jensen-Shannon divergence between two distributions
        
        Args:
            dist1: First distribution dictionary {category: probability}
            dist2: Second distribution dictionary {category: probability}
            
        Returns:
            JS divergence value [0-1]
        """
        # Get all categories
        all_categories = set(dist1.keys()) | set(dist2.keys())
        
        # Create probability arrays with zeros for missing categories
        p = [dist1.get(cat, 0) for cat in all_categories]
        q = [dist2.get(cat, 0) for cat in all_categories]
        
        # Calculate midpoint distribution
        m = [(p[i] + q[i]) / 2 for i in range(len(all_categories))]
        
        # Calculate KL divergence for P||M and Q||M
        kl_pm = sum(p[i] * math.log2(p[i] / m[i]) if p[i] > 0 and m[i] > 0 else 0 for i in range(len(all_categories)))
        kl_qm = sum(q[i] * math.log2(q[i] / m[i]) if q[i] > 0 and m[i] > 0 else 0 for i in range(len(all_categories)))
        
        # Calculate JS divergence
        js_divergence = (kl_pm + kl_qm) / 2
        
        # Convert to JS distance (square root of divergence)
        js_distance = math.sqrt(js_divergence)
        
        return js_distance
    
    @staticmethod
    def _calculate_chi_square(
        df_ref: DataFrame,
        df_curr: DataFrame,
        column: str
    ) -> Dict[str, Any]:
        """Calculate chi-square test for independence
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            column: Column name
            
        Returns:
            Dictionary with chi-square test results
        """
        try:
            # Calculate value counts for reference data
            ref_counts = df_ref.filter(F.col(column).isNotNull()) \
                .groupBy(column) \
                .count() \
                .collect()
            
            # Calculate value counts for current data
            curr_counts = df_curr.filter(F.col(column).isNotNull()) \
                .groupBy(column) \
                .count() \
                .collect()
            
            # Convert to dictionaries
            ref_dict = {str(row[column]): row["count"] for row in ref_counts}
            curr_dict = {str(row[column]): row["count"] for row in curr_counts}
            
            # Get all categories
            all_categories = set(ref_dict.keys()) | set(curr_dict.keys())
            
            # Ensure minimum count for chi-square validity
            if len(all_categories) < 2:
                return {"error": "Insufficient categories for chi-square test"}
            
            # Create observed frequency arrays with zeros for missing categories
            ref_array = [ref_dict.get(cat, 0) for cat in all_categories]
            curr_array = [curr_dict.get(cat, 0) for cat in all_categories]
            
            # Calculate chi-square statistic
            chi_square = 0
            
            # Calculate row sums and total
            ref_sum = sum(ref_array)
            curr_sum = sum(curr_array)
            total_sum = ref_sum + curr_sum
            
            # Skip if insufficient data
            if total_sum < 10:
                return {"error": "Insufficient data for chi-square test"}
            
            # Calculate expected frequencies and chi-square
            for i in range(len(all_categories)):
                cat_sum = ref_array[i] + curr_array[i]
                
                # Expected count for reference data
                exp_ref = (ref_sum * cat_sum) / total_sum if total_sum > 0 else 0
                # Expected count for current data
                exp_curr = (curr_sum * cat_sum) / total_sum if total_sum > 0 else 0
                
                # Skip categories with low expected counts (chi-square assumption)
                if exp_ref >= 5 and exp_curr >= 5:
                    # Calculate chi-square components
                    if exp_ref > 0:
                        chi_square += ((ref_array[i] - exp_ref) ** 2) / exp_ref
                    if exp_curr > 0:
                        chi_square += ((curr_array[i] - exp_curr) ** 2) / exp_curr
            
            # Calculate degrees of freedom
            df = len(all_categories) - 1
            
            # Calculate p-value (approximation)
            # For simplicity, we'll use a rough approximation based on chi-square distribution
            # In a real implementation, you might want to use a proper statistical function
            p_value = CategoricalAnalyzer._approximate_chi_square_p_value(chi_square, df)
            
            return {
                "chi_square": chi_square,
                "degrees_of_freedom": df,
                "p_value": p_value
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    @staticmethod
    def _approximate_chi_square_p_value(chi_square: float, df: int) -> float:
        """Approximate p-value for chi-square test
        
        This is a simple approximation. For production use, consider using proper statistical functions.
        
        Args:
            chi_square: Chi-square statistic
            df: Degrees of freedom
            
        Returns:
            Approximate p-value
        """
        # Simple approximation for chi-square p-value
        # This is not accurate for all cases but provides a reasonable approximation
        # for common cases in drift detection
        
        # Critical values for p=0.05
        critical_values = {
            1: 3.84, 2: 5.99, 3: 7.81, 4: 9.49, 5: 11.07,
            6: 12.59, 7: 14.07, 8: 15.51, 9: 16.92, 10: 18.31
        }
        
        # Handle degrees of freedom beyond our table
        if df > 10:
            # Approximate critical value for p=0.05
            # Using formula: critical_value ≈ df + sqrt(2*df)
            critical_value = df + math.sqrt(2 * df)
        else:
            critical_value = critical_values.get(df, 3.84)  # Default to df=1 if unknown
        
        # Calculate approximate p-value
        if chi_square < 0.001:
            return 1.0
        elif chi_square > 3 * critical_value:
            return 0.001  # Very significant
        elif chi_square > 2 * critical_value:
            return 0.01  # Significant
        elif chi_square > critical_value:
            return 0.05  # Moderately significant
        else:
            # Approximate p-value in the range (0.05, 1.0)
            return min(1.0, max(0.05, 1.0 - (chi_square / critical_value) * 0.95))
    
    @staticmethod
    def _calculate_drift_score(
        js_divergence: float,
        chi_p_value: float,
        null_diff: float,
        new_cat_ratio: float,
        missing_cat_ratio: float
    ) -> float:
        """Calculate overall drift score for a categorical column
        
        Args:
            js_divergence: Jensen-Shannon divergence
            chi_p_value: Chi-square p-value
            null_diff: Difference in null proportion
            new_cat_ratio: Ratio of new categories
            missing_cat_ratio: Ratio of missing categories
            
        Returns:
            Drift score in range [0, 1]
        """
        # Convert p-value to significance score (0 = not significant, 1 = very significant)
        # Lower p-values mean higher significance
        if chi_p_value <= 0.001:
            chi_score = 1.0
        elif chi_p_value <= 0.01:
            chi_score = 0.8
        elif chi_p_value <= 0.05:
            chi_score = 0.6
        elif chi_p_value <= 0.1:
            chi_score = 0.3
        else:
            chi_score = 0.0
        
        # Combine all metrics with weights
        weights = {
            "js_divergence": 0.4,  # JS divergence is a primary indicator
            "chi_score": 0.3,      # Statistical significance
            "null_diff": 0.1,      # Changes in null proportion
            "cat_ratio": 0.2       # Changes in category composition
        }
        
        # Calculate category change score
        cat_ratio_score = max(new_cat_ratio, missing_cat_ratio)
        
        # Calculate weighted score
        drift_score = (
            weights["js_divergence"] * min(1.0, js_divergence * 4) +  # Scale JS divergence
            weights["chi_score"] * chi_score +
            weights["null_diff"] * min(1.0, abs(null_diff) * 10) +  # Scale null diff
            weights["cat_ratio"] * min(1.0, cat_ratio_score * 2)    # Scale category ratio
        )
        
        return min(1.0, drift_score)  # Ensure score is in [0, 1]
