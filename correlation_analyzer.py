from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np
from typing import Dict, List, Set, Any, Tuple
import math

class CorrelationAnalyzer:
    """Module to analyze correlation and multi-dimensional relationships between data"""
    
    @staticmethod
    def analyze_correlations(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        numerical_columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze correlations between numerical columns and detect significant changes
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            numerical_columns: List of numerical columns to analyze
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with correlation analysis results
        """
        results = {
            "correlation_changes": {},
            "significant_correlation_shifts": [],
            "new_correlations": [],
            "disappeared_correlations": []
        }
        
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {}).get(profile, {})
        correlation_threshold = thresholds.get("correlation_threshold", 0.7)
        correlation_change_threshold = thresholds.get("correlation_change_threshold", 0.2)
        
        # Skip if not enough numerical columns
        if len(numerical_columns) < 2:
            results["status"] = "skipped_insufficient_columns"
            return results
        
        try:
            # Compute correlation matrices
            ref_corr_matrix = CorrelationAnalyzer._compute_correlation_matrix(
                df_ref, numerical_columns)
            curr_corr_matrix = CorrelationAnalyzer._compute_correlation_matrix(
                df_curr, numerical_columns)
            
            # Find significant correlation changes
            correlation_changes = {}
            significant_shifts = []
            new_correlations = []
            disappeared_correlations = []
            
            # Analyze all column pairs
            for i, col1 in enumerate(numerical_columns):
                for j, col2 in enumerate(numerical_columns):
                    if i < j:  # Only look at unique pairs
                        pair_key = f"{col1}_{col2}"
                        ref_corr = ref_corr_matrix.get(pair_key, 0)
                        curr_corr = curr_corr_matrix.get(pair_key, 0)
                        
                        # Calculate absolute change
                        abs_change = abs(curr_corr - ref_corr)
                        
                        correlation_changes[pair_key] = {
                            "ref_correlation": ref_corr,
                            "curr_correlation": curr_corr,
                            "abs_change": abs_change
                        }
                        
                        # Check for significant shifts
                        if abs_change >= correlation_change_threshold:
                            significant_shifts.append({
                                "column_pair": pair_key,
                                "ref_correlation": ref_corr,
                                "curr_correlation": curr_corr,
                                "abs_change": abs_change,
                                "change_type": "weaker" if abs(curr_corr) < abs(ref_corr) else "stronger"
                            })
                        
                        # Check for new strong correlations
                        if abs(ref_corr) < correlation_threshold and abs(curr_corr) >= correlation_threshold:
                            new_correlations.append({
                                "column_pair": pair_key,
                                "ref_correlation": ref_corr,
                                "curr_correlation": curr_corr,
                                "correlation_type": "negative" if curr_corr < 0 else "positive"
                            })
                        
                        # Check for disappeared strong correlations
                        if abs(ref_corr) >= correlation_threshold and abs(curr_corr) < correlation_threshold:
                            disappeared_correlations.append({
                                "column_pair": pair_key,
                                "ref_correlation": ref_corr,
                                "curr_correlation": curr_corr,
                                "correlation_type": "negative" if ref_corr < 0 else "positive"
                            })
            
            results["correlation_changes"] = correlation_changes
            results["significant_correlation_shifts"] = sorted(
                significant_shifts, key=lambda x: x["abs_change"], reverse=True)
            results["new_correlations"] = sorted(
                new_correlations, key=lambda x: abs(x["curr_correlation"]), reverse=True)
            results["disappeared_correlations"] = sorted(
                disappeared_correlations, key=lambda x: abs(x["ref_correlation"]), reverse=True)
            
            # Find top correlations in each dataset
            results["top_correlations_ref"] = CorrelationAnalyzer._find_top_correlations(
                ref_corr_matrix, numerical_columns, correlation_threshold)
            results["top_correlations_curr"] = CorrelationAnalyzer._find_top_correlations(
                curr_corr_matrix, numerical_columns, correlation_threshold)
            
            results["status"] = "success"
            
        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
        
        return results
    
    @staticmethod
    def analyze_group_correlations(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        numerical_columns: List[str],
        categorical_columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze shifts in correlations within categorical groups
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            numerical_columns: List of numerical columns to analyze
            categorical_columns: List of categorical columns to use as group dimensions
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with group correlation analysis results
        """
        results = {
            "group_correlation_shifts": {}
        }
        
        # Get thresholds from config
        profile = config.get("profile", "standard")
        thresholds = config.get("thresholds", {}).get(profile, {})
        correlation_change_threshold = thresholds.get("correlation_change_threshold", 0.3)
        
        # Skip if not enough columns
        if len(numerical_columns) < 2 or len(categorical_columns) < 1:
            results["status"] = "skipped_insufficient_columns"
            return results
        
        # Limit categorical columns to analyze to prevent explosion
        max_cat_columns = min(3, len(categorical_columns))
        cat_columns_to_analyze = categorical_columns[:max_cat_columns]
        
        # Analyze correlation within groups for each categorical column
        for cat_column in cat_columns_to_analyze:
            try:
                # Get top N categories by count
                top_categories = CorrelationAnalyzer._get_top_categories(
                    df_ref, df_curr, cat_column, top_n=10
                )
                
                group_results = {}
                
                # Analyze correlations within each category
                for category in top_categories:
                    # Filter dataframes to only include this category
                    ref_filter = df_ref.filter(F.col(cat_column) == category)
                    curr_filter = df_curr.filter(F.col(cat_column) == category)
                    
                    # Skip if not enough data in either dataset
                    if ref_filter.count() < 30 or curr_filter.count() < 30:
                        continue
                    
                    # Compute correlation matrices for this category
                    ref_corr = CorrelationAnalyzer._compute_correlation_matrix(
                        ref_filter, numerical_columns)
                    curr_corr = CorrelationAnalyzer._compute_correlation_matrix(
                        curr_filter, numerical_columns)
                    
                    # Find significant correlation changes
                    significant_shifts = []
                    
                    # Analyze all column pairs
                    for i, col1 in enumerate(numerical_columns):
                        for j, col2 in enumerate(numerical_columns):
                            if i < j:  # Only look at unique pairs
                                pair_key = f"{col1}_{col2}"
                                ref_val = ref_corr.get(pair_key, 0)
                                curr_val = curr_corr.get(pair_key, 0)
                                
                                # Calculate absolute change
                                abs_change = abs(curr_val - ref_val)
                                
                                # Check for significant shifts
                                if abs_change >= correlation_change_threshold:
                                    significant_shifts.append({
                                        "column_pair": pair_key,
                                        "ref_correlation": ref_val,
                                        "curr_correlation": curr_val,
                                        "abs_change": abs_change,
                                        "change_type": "weaker" if abs(curr_val) < abs(ref_val) else "stronger"
                                    })
                    
                    if significant_shifts:
                        group_results[str(category)] = {
                            "significant_shifts": sorted(
                                significant_shifts, key=lambda x: x["abs_change"], reverse=True),
                            "ref_count": ref_filter.count(),
                            "curr_count": curr_filter.count()
                        }
                
                if group_results:
                    results["group_correlation_shifts"][cat_column] = group_results
            
            except Exception as e:
                if "group_correlation_errors" not in results:
                    results["group_correlation_errors"] = {}
                results["group_correlation_errors"][cat_column] = str(e)
        
        results["status"] = "success" if "group_correlation_shifts" in results else "no_significant_shifts"
        
        return results
    
    @staticmethod
    def analyze_feature_importance_drift(
        df_ref: DataFrame, 
        df_curr: DataFrame, 
        target_column: str,
        predictor_columns: List[str],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze shifts in feature importance relative to a target variable
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            target_column: Target column to analyze feature importance against
            predictor_columns: List of columns to analyze as predictors
            config: Configuration dictionary with analysis parameters
        
        Returns:
            Dictionary with feature importance drift analysis results
        """
        results = {
            "feature_importance_shifts": {}
        }
        
        # Skip if target column not provided or not enough predictor columns
        if not target_column or len(predictor_columns) < 1:
            results["status"] = "skipped_insufficient_columns"
            return results
        
        try:
            # Compute correlation of each predictor with target in reference data
            ref_importance = {}
            for col in predictor_columns:
                ref_corr = df_ref.stat.corr(col, target_column)
                ref_importance[col] = abs(ref_corr)  # Use absolute correlation as importance
            
            # Compute correlation of each predictor with target in current data
            curr_importance = {}
            for col in predictor_columns:
                curr_corr = df_curr.stat.corr(col, target_column)
                curr_importance[col] = abs(curr_corr)
            
            # Calculate importance shifts
            importance_shifts = []
            for col in predictor_columns:
                ref_imp = ref_importance.get(col, 0)
                curr_imp = curr_importance.get(col, 0)
                abs_change = abs(curr_imp - ref_imp)
                rel_change = abs_change / max(ref_imp, 0.01)  # Avoid division by zero
                
                importance_shifts.append({
                    "column": col,
                    "ref_importance": ref_imp,
                    "curr_importance": curr_imp,
                    "abs_change": abs_change,
                    "rel_change": rel_change,
                    "change_type": "increased" if curr_imp > ref_imp else "decreased"
                })
            
            # Sort by absolute change
            importance_shifts.sort(key=lambda x: x["abs_change"], reverse=True)
            
            # Calculate rank shifts
            ref_ranked = sorted(predictor_columns, key=lambda x: ref_importance.get(x, 0), reverse=True)
            curr_ranked = sorted(predictor_columns, key=lambda x: curr_importance.get(x, 0), reverse=True)
            
            rank_shifts = []
            for col in predictor_columns:
                ref_rank = ref_ranked.index(col) + 1  # 1-based ranking
                curr_rank = curr_ranked.index(col) + 1
                rank_shift = ref_rank - curr_rank  # positive means improved rank
                
                if abs(rank_shift) >= 2:  # Only include significant rank shifts
                    rank_shifts.append({
                        "column": col,
                        "ref_rank": ref_rank,
                        "curr_rank": curr_rank,
                        "rank_shift": rank_shift,
                        "shift_type": "improved" if rank_shift > 0 else "declined"
                    })
            
            # Sort by absolute rank shift
            rank_shifts.sort(key=lambda x: abs(x["rank_shift"]), reverse=True)
            
            results["feature_importance_shifts"] = {
                "target_column": target_column,
                "importance_shifts": importance_shifts,
                "rank_shifts": rank_shifts,
                "ref_top_features": ref_ranked[:5],
                "curr_top_features": curr_ranked[:5]
            }
            
            results["status"] = "success"
            
        except Exception as e:
            results["status"] = "failed"
            results["error"] = str(e)
        
        return results
    
    @staticmethod
    def _compute_correlation_matrix(
        df: DataFrame, 
        columns: List[str]
    ) -> Dict[str, float]:
        """Compute correlation matrix between numerical columns
        
        Args:
            df: DataFrame to analyze
            columns: List of numerical columns
            
        Returns:
            Dictionary with column pairs as keys and correlation coefficients as values
        """
        if len(columns) < 2:
            return {}
        
        # Create feature vector
        vector_col = "features"
        assembler = VectorAssembler(inputCols=columns, outputCol=vector_col)
        df_vector = assembler.transform(df.select(columns))
        
        # Compute correlation matrix
        matrix = Correlation.corr(df_vector, vector_col, "pearson").collect()[0][0]
        
        # Convert to dictionary with column pair keys
        result = {}
        matrix_array = matrix.toArray()
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i != j:  # Skip self-correlations
                    pair_key = f"{col1}_{col2}"
                    result[pair_key] = float(matrix_array[i][j])
        
        return result
    
    @staticmethod
    def _find_top_correlations(
        corr_matrix: Dict[str, float],
        columns: List[str],
        threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Find top correlations in correlation matrix
        
        Args:
            corr_matrix: Dictionary with column pairs as keys and correlation coefficients as values
            columns: List of columns that were analyzed
            threshold: Minimum absolute correlation threshold
            
        Returns:
            List of dictionaries with top correlations
        """
        # Get unique column pairs
        unique_pairs = set()
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:
                    unique_pairs.add(f"{col1}_{col2}")
        
        # Find strong correlations
        strong_correlations = []
        for pair in unique_pairs:
            corr = corr_matrix.get(pair, 0)
            if abs(corr) >= threshold:
                col1, col2 = pair.split('_')
                strong_correlations.append({
                    "column_pair": pair,
                    "column1": col1,
                    "column2": col2,
                    "correlation": corr,
                    "correlation_type": "negative" if corr < 0 else "positive"
                })
        
        # Sort by absolute correlation
        return sorted(strong_correlations, key=lambda x: abs(x["correlation"]), reverse=True)
    
    @staticmethod
    def _get_top_categories(
        df_ref: DataFrame,
        df_curr: DataFrame,
        column: str,
        top_n: int = 10
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
    def analyze_correlation_groups(df_ref: DataFrame, df_curr: DataFrame, numerical_columns: List[str],
                                 threshold: float = 0.7) -> Dict[str, Any]:
        """Find groups of correlated features and how they change
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            numerical_columns: List of numerical columns to analyze
            threshold: Correlation threshold to consider features as grouped
            
        Returns:
            Dictionary with correlation group information
        """
        if len(numerical_columns) < 3:  # Need at least 3 columns for meaningful groups
            return {"ref_groups": [], "curr_groups": [], "changes": []}
            
        # Filter out columns that don't exist in both DataFrames
        ref_cols = set(df_ref.columns)
        curr_cols = set(df_curr.columns)
        common_numerical = [col for col in numerical_columns if col in ref_cols and col in curr_cols]
        
        if len(common_numerical) < 3:
            return {"ref_groups": [], "curr_groups": [], "changes": []}
            
        # Get correlation matrices
        ref_corr = CorrelationAnalyzer._get_correlation_matrix(df_ref, common_numerical)
        curr_corr = CorrelationAnalyzer._get_correlation_matrix(df_curr, common_numerical)
        
        # Find groups in reference data
        ref_groups = CorrelationAnalyzer._find_correlation_groups(ref_corr, common_numerical, threshold)
        
        # Find groups in current data
        curr_groups = CorrelationAnalyzer._find_correlation_groups(curr_corr, common_numerical, threshold)
        
        # Analyze changes in groups
        changes = CorrelationAnalyzer._analyze_group_changes(ref_groups, curr_groups)
        
        return {
            "ref_groups": ref_groups,
            "curr_groups": curr_groups,
            "changes": changes
        }
        
    @staticmethod
    def _find_correlation_groups(corr_matrix: np.ndarray, columns: List[str], threshold: float) -> List[List[str]]:
        """Find groups of correlated features
        
        Args:
            corr_matrix: Correlation matrix
            columns: Column names corresponding to matrix indices
            threshold: Correlation threshold
            
        Returns:
            List of feature groups
        """
        n = len(columns)
        
        # Create adjacency matrix for highly correlated features
        adjacency = np.zeros((n, n), dtype=bool)
        for i in range(n):
            for j in range(n):
                if i != j and abs(corr_matrix[i, j]) >= threshold:
                    adjacency[i, j] = True
                    
        # Use connected components to find correlation groups
        visited = [False] * n
        groups = []
        
        for i in range(n):
            if not visited[i]:
                # Start a new component
                component = []
                queue = [i]
                visited[i] = True
                
                while queue:
                    node = queue.pop(0)
                    component.append(node)
                    
                    # Find all adjacent nodes
                    for j in range(n):
                        if adjacency[node, j] and not visited[j]:
                            visited[j] = True
                            queue.append(j)
                            
                # Convert indices to column names
                if len(component) > 1:  # Only include groups with at least 2 columns
                    groups.append([columns[idx] for idx in component])
                    
        return groups
        
    @staticmethod
    def _analyze_group_changes(ref_groups: List[List[str]], curr_groups: List[List[str]]) -> List[Dict[str, Any]]:
        """Analyze changes in correlation groups
        
        Args:
            ref_groups: Reference correlation groups
            curr_groups: Current correlation groups
            
        Returns:
            List of group changes
        """
        changes = []
        
        # Prepare for easy lookup
        ref_group_map = {}
        for i, group in enumerate(ref_groups):
            for col in group:
                ref_group_map[col] = i
                
        curr_group_map = {}
        for i, group in enumerate(curr_groups):
            for col in group:
                curr_group_map[col] = i
                
        # Find columns that changed groups
        all_columns = set(ref_group_map.keys()) | set(curr_group_map.keys())
        
        for col in all_columns:
            # Handle column in reference but not in current
            if col in ref_group_map and col not in curr_group_map:
                ref_group_idx = ref_group_map[col]
                changes.append({
                    "column": col,
                    "change_type": "removed_from_group",
                    "ref_group": ref_groups[ref_group_idx],
                    "severity": "medium"
                })
                
            # Handle column in current but not in reference
            elif col in curr_group_map and col not in ref_group_map:
                curr_group_idx = curr_group_map[col]
                changes.append({
                    "column": col,
                    "change_type": "added_to_group",
                    "curr_group": curr_groups[curr_group_idx],
                    "severity": "medium"
                })
                
            # Handle column in both but changed groups
            elif col in ref_group_map and col in curr_group_map:
                ref_group_idx = ref_group_map[col]
                curr_group_idx = curr_group_map[col]
                
                ref_group = set(ref_groups[ref_group_idx])
                curr_group = set(curr_groups[curr_group_idx])
                
                # Only report if actually changed
                if ref_group != curr_group:
                    # Calculate how much the group changed
                    intersection = ref_group.intersection(curr_group)
                    union = ref_group.union(curr_group)
                    change_ratio = 1 - (len(intersection) / len(union))
                    
                    severity = "high" if change_ratio > 0.5 else "medium"
                    
                    changes.append({
                        "column": col,
                        "change_type": "changed_group",
                        "ref_group": list(ref_group),
                        "curr_group": list(curr_group),
                        "change_ratio": change_ratio,
                        "severity": severity
                    })
                    
        return changes

    @staticmethod
    def analyze_pairwise_correlations(df_ref: DataFrame, 
                                    df_curr: DataFrame,
                                    num_columns: List[str], 
                                    config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze pairwise correlations between numerical columns
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            num_columns: List of numerical columns
            config: Configuration dictionary
            
        Returns:
            List of correlation change results
        """
        if len(num_columns) < 2:
            return []
            
        # Get threshold from config
        threshold = config.get("correlation_change_threshold", 0.3)
        
        # Filter out columns with too many nulls
        min_non_null_ratio = config.get("min_non_null_ratio", 0.7)
        ref_counts = df_ref.select([F.count(F.when(F.col(c).isNotNull(), c)).alias(c) for c in num_columns])
        curr_counts = df_curr.select([F.count(F.when(F.col(c).isNotNull(), c)).alias(c) for c in num_columns])
        
        ref_row_count = df_ref.count()
        curr_row_count = df_curr.count()
        
        if ref_row_count == 0 or curr_row_count == 0:
            return []
            
        # Get columns with sufficient non-null values in both datasets
        valid_columns = []
        
        for col in num_columns:
            ref_count = ref_counts.select(col).collect()[0][0]
            curr_count = curr_counts.select(col).collect()[0][0]
            
            ref_ratio = ref_count / ref_row_count if ref_row_count > 0 else 0
            curr_ratio = curr_count / curr_row_count if curr_row_count > 0 else 0
            
            if ref_ratio >= min_non_null_ratio and curr_ratio >= min_non_null_ratio:
                valid_columns.append(col)
                
        if len(valid_columns) < 2:
            return []
            
        # Calculate correlations in both datasets
        ref_corr_matrix = CorrelationAnalyzer._calculate_correlation_matrix(df_ref, valid_columns)
        curr_corr_matrix = CorrelationAnalyzer._calculate_correlation_matrix(df_curr, valid_columns)
        
        if not ref_corr_matrix or not curr_corr_matrix:
            return []
            
        # Find significant correlation changes
        results = []
        
        for i in range(len(valid_columns)):
            for j in range(i+1, len(valid_columns)):
                col1 = valid_columns[i]
                col2 = valid_columns[j]
                
                ref_corr = ref_corr_matrix.get((col1, col2))
                curr_corr = curr_corr_matrix.get((col1, col2))
                
                if ref_corr is None or curr_corr is None:
                    continue
                    
                # Calculate absolute change in correlation
                corr_change = abs(curr_corr - ref_corr)
                
                if corr_change >= threshold:
                    results.append({
                        "column1": col1,
                        "column2": col2,
                        "reference_correlation": ref_corr,
                        "current_correlation": curr_corr,
                        "correlation_change": corr_change,
                        "is_significant": True,
                        "threshold": threshold
                    })
                    
        return results
    
    @staticmethod
    def _calculate_correlation_matrix(df: DataFrame, columns: List[str]) -> Dict[Tuple[str, str], float]:
        """Calculate correlation matrix for specified columns
        
        Args:
            df: DataFrame to analyze
            columns: List of columns to include
            
        Returns:
            Dictionary mapping column pairs to correlation values
        """
        try:
            # Create vector column
            assembler = VectorAssembler(
                inputCols=columns,
                outputCol="features",
                handleInvalid="skip"
            )
            
            df_vector = assembler.transform(df).select("features")
            
            # Calculate correlation matrix
            matrix = Correlation.corr(df_vector, "features", "pearson")
            
            # Extract correlation values
            corr_matrix = {}
            
            # Convert to Row format
            matrix_row = matrix.collect()[0][0]
            values = matrix_row.toArray().tolist()
            
            # Populate correlation matrix dictionary
            for i in range(len(columns)):
                for j in range(len(columns)):
                    if i != j:
                        corr_matrix[(columns[i], columns[j])] = values[i][j]
                        
            return corr_matrix
            
        except Exception:
            return {}
    
    @staticmethod
    def detect_correlation_structure_changes(df_ref: DataFrame, 
                                           df_curr: DataFrame,
                                           num_columns: List[str], 
                                           config: Dict[str, Any]) -> Dict[str, Any]:
        """Detect changes in correlation structure using eigenvalue analysis
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            num_columns: List of numerical columns
            config: Configuration dictionary
            
        Returns:
            Dictionary with correlation structure change results
        """
        # Skip if too few columns
        if len(num_columns) < 3:
            return {"structure_change_detected": False, "reason": "Too few columns for structure analysis"}
        
        # Get thresholds from config
        eigenvalue_ratio_threshold = config.get("eigenvalue_ratio_threshold", 0.2)
        
        # Calculate correlation matrices
        ref_corr_matrix = CorrelationAnalyzer._calculate_correlation_matrix(df_ref, num_columns)
        curr_corr_matrix = CorrelationAnalyzer._calculate_correlation_matrix(df_curr, num_columns)
        
        if not ref_corr_matrix or not curr_corr_matrix:
            return {"structure_change_detected": False, "reason": "Could not calculate correlation matrices"}
        
        # Compare eigenvalues of correlation matrices
        # This is a simplified approach - in practice, you would compute eigenvalues 
        # using numpy or a PySpark UDF with numpy
        
        # Here we'll use a simplified metric - the average absolute difference in correlations
        total_diff = 0.0
        count = 0
        
        for i in range(len(num_columns)):
            for j in range(i+1, len(num_columns)):
                col1 = num_columns[i]
                col2 = num_columns[j]
                
                ref_corr = ref_corr_matrix.get((col1, col2), 0)
                curr_corr = curr_corr_matrix.get((col1, col2), 0)
                
                total_diff += abs(ref_corr - curr_corr)
                count += 1
                
        if count == 0:
            return {"structure_change_detected": False, "reason": "No valid correlation pairs"}
            
        avg_diff = total_diff / count
        
        return {
            "structure_change_detected": avg_diff > eigenvalue_ratio_threshold,
            "average_correlation_change": avg_diff,
            "threshold": eigenvalue_ratio_threshold
        }
        
    @staticmethod
    def detect_key_column_relationship_changes(df_ref: DataFrame, 
                                             df_curr: DataFrame,
                                             key_column: str,
                                             target_columns: List[str],
                                             config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect changes in relationships between a key column and target columns
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            key_column: Key column to analyze relationships for
            target_columns: Target columns to check relationship with key
            config: Configuration dictionary
            
        Returns:
            List of relationship change results
        """
        results = []
        
        # Get threshold from config
        threshold = config.get("relationship_change_threshold", 0.3)
        
        # Check if key column is in DataFrames
        if key_column not in df_ref.columns or key_column not in df_curr.columns:
            return []
            
        for target_col in target_columns:
            if target_col not in df_ref.columns or target_col not in df_curr.columns:
                continue
                
            # Check relationship via correlation for numerical columns
            if df_ref.select(target_col).dtypes[0][1] in ('int', 'bigint', 'double', 'float'):
                # Calculate correlation between key and target in both datasets
                ref_corr = CorrelationAnalyzer._calculate_single_correlation(df_ref, key_column, target_col)
                curr_corr = CorrelationAnalyzer._calculate_single_correlation(df_curr, key_column, target_col)
                
                if ref_corr is None or curr_corr is None:
                    continue
                    
                # Calculate absolute change in correlation
                corr_change = abs(curr_corr - ref_corr)
                
                if corr_change >= threshold:
                    results.append({
                        "key_column": key_column,
                        "target_column": target_col,
                        "reference_correlation": ref_corr,
                        "current_correlation": curr_corr,
                        "relationship_change": corr_change,
                        "is_significant": True,
                        "threshold": threshold
                    })
                    
        return results
        
    @staticmethod
    def _calculate_single_correlation(df: DataFrame, col1: str, col2: str) -> float:
        """Calculate correlation between two columns
        
        Args:
            df: DataFrame to analyze
            col1: First column name
            col2: Second column name
            
        Returns:
            Correlation value or None if calculation fails
        """
        try:
            # Convert columns to double if needed
            df_prep = df
            for col in [col1, col2]:
                if df.select(col).dtypes[0][1] not in ('double', 'float'):
                    df_prep = df_prep.withColumn(f"{col}_double", F.col(col).cast("double"))
                else:
                    df_prep = df_prep.withColumn(f"{col}_double", F.col(col))
                    
            # Calculate correlation
            corr = df_prep.stat.corr(f"{col1}_double", f"{col2}_double")
            return corr
            
        except Exception:
            return None 