from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import PCA
from typing import Dict, List, Any, Tuple

class RareEventAnalyzer:
    """Analyzer for detecting changes in rare events and anomalies"""
    
    @staticmethod
    def analyze_rare_categories(df_ref: DataFrame, df_curr: DataFrame, cat_columns: List[str], 
                               min_count: int = 10, max_frequency: float = 0.01) -> List[Dict[str, Any]]:
        """Analyze changes in rare categories
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            cat_columns: List of categorical columns to analyze
            min_count: Minimum count to be considered (avoid ultra-rare categories)
            max_frequency: Maximum frequency to be considered a rare category
            
        Returns:
            List of rare category change results
        """
        results = []
        
        # Get total counts for calculating frequencies
        ref_count = df_ref.count()
        curr_count = df_curr.count()
        
        if ref_count == 0 or curr_count == 0:
            return results
            
        for col in cat_columns:
            if col not in df_ref.columns or col not in df_curr.columns:
                continue
                
            # Get value counts in reference data
            ref_counts = df_ref.groupBy(col).count().withColumnRenamed("count", "ref_count")
            ref_counts = ref_counts.withColumn("ref_freq", F.col("ref_count") / ref_count)
            
            # Get value counts in current data
            curr_counts = df_curr.groupBy(col).count().withColumnRenamed("count", "curr_count")
            curr_counts = curr_counts.withColumn("curr_freq", F.col("curr_count") / curr_count)
            
            # Join the counts
            joined = ref_counts.join(curr_counts, on=col, how="full_outer") \
                .fillna(0, subset=["ref_count", "curr_count"]) \
                .fillna(0, subset=["ref_freq", "curr_freq"])
                
            # Find rare categories that have significant changes
            rare_changes = joined.filter(
                # Was rare in reference data
                ((F.col("ref_freq") <= max_frequency) & (F.col("ref_count") >= min_count)) |
                # Is rare in current data
                ((F.col("curr_freq") <= max_frequency) & (F.col("curr_count") >= min_count))
            )
            
            # Calculate relative change
            rare_changes = rare_changes.withColumn(
                "rel_change", 
                F.when(F.col("ref_count") > 0, 
                      (F.col("curr_count") - F.col("ref_count")) / F.col("ref_count")
                ).otherwise(
                    F.when(F.col("curr_count") > 0, 1.0).otherwise(0.0)
                )
            )
            
            # Filter for significant changes
            significant_changes = rare_changes.filter(
                (F.abs(F.col("rel_change")) >= 0.3) |  # 30% change
                (F.col("ref_count") == 0) |            # New categories
                (F.col("curr_count") == 0)             # Disappeared categories
            )
            
            # Convert to result format
            for row in significant_changes.collect():
                category = row[col]
                
                # Skip nulls
                if category is None:
                    continue
                    
                severity = "high"
                if row["ref_count"] == 0:
                    change_type = "new_rare_category"
                elif row["curr_count"] == 0:
                    change_type = "disappeared_rare_category"
                elif row["rel_change"] > 0:
                    change_type = "increased_rare_category"
                    severity = "high" if row["rel_change"] > 1.0 else "medium"
                else:
                    change_type = "decreased_rare_category"
                    severity = "high" if abs(row["rel_change"]) > 0.5 else "medium"
                    
                results.append({
                    "column": col,
                    "category": str(category),
                    "change_type": change_type,
                    "ref_count": int(row["ref_count"]),
                    "curr_count": int(row["curr_count"]),
                    "ref_freq": float(row["ref_freq"]),
                    "curr_freq": float(row["curr_freq"]),
                    "rel_change": float(row["rel_change"]),
                    "severity": severity
                })
                
        # Sort by severity and relative change
        results.sort(key=lambda x: (0 if x["severity"] == "high" else 1, -abs(x["rel_change"])))
        
        return results
        
    @staticmethod
    def detect_outlier_changes(df_ref: DataFrame, df_curr: DataFrame, num_columns: List[str], 
                             z_threshold: float = 3.0) -> List[Dict[str, Any]]:
        """Detect changes in outlier patterns
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            num_columns: List of numerical columns to analyze
            z_threshold: Z-score threshold for outliers
            
        Returns:
            List of outlier change results
        """
        results = []
        
        for col in num_columns:
            if col not in df_ref.columns or col not in df_curr.columns:
                continue
                
            # Calculate z-scores for reference data
            ref_mean = df_ref.select(F.mean(col).alias("mean")).collect()[0]["mean"]
            ref_stddev = df_ref.select(F.stddev(col).alias("stddev")).collect()[0]["stddev"]
            
            # Handle zero std dev
            if ref_stddev is None or ref_stddev == 0:
                continue
                
            ref_outliers = df_ref.filter(
                F.abs((F.col(col) - ref_mean) / ref_stddev) > z_threshold
            ).count()
            
            ref_count = df_ref.count()
            ref_outlier_ratio = ref_outliers / ref_count if ref_count > 0 else 0
            
            # Calculate z-scores for current data (using reference statistics)
            curr_outliers = df_curr.filter(
                F.abs((F.col(col) - ref_mean) / ref_stddev) > z_threshold
            ).count()
            
            curr_count = df_curr.count()
            curr_outlier_ratio = curr_outliers / curr_count if curr_count > 0 else 0
            
            # Calculate relative change in outlier ratio
            if ref_outlier_ratio > 0:
                rel_change = (curr_outlier_ratio - ref_outlier_ratio) / ref_outlier_ratio
            elif curr_outlier_ratio > 0:
                rel_change = 1.0  # New outliers
            else:
                rel_change = 0.0  # No outliers in either dataset
                
            # Check if change is significant
            if abs(rel_change) >= 0.3 or abs(curr_outlier_ratio - ref_outlier_ratio) >= 0.01:
                # Determine severity
                severity = "high" if abs(rel_change) >= 1.0 or abs(curr_outlier_ratio - ref_outlier_ratio) >= 0.05 else "medium"
                
                results.append({
                    "column": col,
                    "ref_outlier_count": int(ref_outliers),
                    "curr_outlier_count": int(curr_outliers),
                    "ref_outlier_ratio": float(ref_outlier_ratio),
                    "curr_outlier_ratio": float(curr_outlier_ratio),
                    "rel_change": float(rel_change),
                    "z_threshold": z_threshold,
                    "severity": severity
                })
                
        # Sort by severity and relative change
        results.sort(key=lambda x: (0 if x["severity"] == "high" else 1, -abs(x["rel_change"])))
        
        return results
        
    @staticmethod
    def detect_multivariate_anomalies(df_ref: DataFrame, df_curr: DataFrame, num_columns: List[str], 
                                   threshold: float = 3.0, 
                                   max_components: int = 10) -> Dict[str, Any]:
        """Detect multivariate anomalies using PCA
        
        Args:
            df_ref: Reference DataFrame
            df_curr: Current DataFrame
            num_columns: List of numerical columns to analyze
            threshold: Threshold for anomaly scores
            max_components: Maximum number of PCA components to use
            
        Returns:
            Dictionary with anomaly detection results
        """
        # Check if we have enough columns
        if len(num_columns) < 3:
            return {
                "status": "skipped",
                "reason": "Not enough numerical columns for multivariate analysis"
            }
            
        # Filter columns that exist in both DataFrames
        cols = [col for col in num_columns if col in df_ref.columns and col in df_curr.columns]
        
        if len(cols) < 3:
            return {
                "status": "skipped",
                "reason": "Not enough common numerical columns for multivariate analysis"
            }
            
        try:
            # Create feature vector
            assembler = VectorAssembler(
                inputCols=cols,
                outputCol="features",
                handleInvalid="skip"
            )
            
            # Standardize features
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
            
            # Apply to reference data
            ref_assembled = assembler.transform(df_ref.select(cols))
            scaler_model = scaler.fit(ref_assembled)
            ref_scaled = scaler_model.transform(ref_assembled)
            
            # Determine number of components (use smaller of max_components or number of columns)
            k = min(max_components, len(cols))
            
            # Apply PCA to reference data
            pca = PCA(
                k=k,
                inputCol="scaled_features",
                outputCol="pca_features"
            )
            
            pca_model = pca.fit(ref_scaled)
            
            # Get explained variance
            explained_variance = pca_model.explainedVariance.toArray()
            
            # Apply PCA model to both datasets
            ref_pca = pca_model.transform(ref_scaled)
            
            # Apply to current data (using same scaler and PCA model)
            curr_assembled = assembler.transform(df_curr.select(cols))
            curr_scaled = scaler_model.transform(curr_assembled)
            curr_pca = pca_model.transform(curr_scaled)
            
            # Reconstruct the data from PCA
            # For reconstruction, we need to do matrix multiplication of pca_features with principalComponents
            principal_components = pca_model.pc.toArray()
            
            # Function to calculate reconstruction error
            def calculate_reconstruction_error(df):
                return df.select(
                    F.udf(
                        lambda original, pca: float(
                            sum((o - sum(p * c for p, c in zip(pca, comp))) ** 2 
                                for o, comp in zip(original, principal_components))
                        ),
                        "double"
                    )(F.col("scaled_features"), F.col("pca_features")).alias("reconstruction_error")
                )
            
            # Calculate reconstruction error for both datasets
            ref_errors = calculate_reconstruction_error(ref_pca)
            curr_errors = calculate_reconstruction_error(curr_pca)
            
            # Calculate anomaly statistics
            ref_mean_error = ref_errors.select(F.mean("reconstruction_error")).collect()[0][0]
            ref_stddev_error = ref_errors.select(F.stddev("reconstruction_error")).collect()[0][0]
            
            if ref_stddev_error is None or ref_stddev_error == 0:
                return {
                    "status": "skipped",
                    "reason": "Reference data has zero standard deviation in reconstruction error"
                }
                
            # Count anomalies in reference data
            ref_anomalies = ref_errors.filter(
                F.col("reconstruction_error") > ref_mean_error + threshold * ref_stddev_error
            ).count()
            
            ref_count = ref_errors.count()
            ref_anomaly_ratio = ref_anomalies / ref_count if ref_count > 0 else 0
            
            # Count anomalies in current data (using reference statistics)
            curr_anomalies = curr_errors.filter(
                F.col("reconstruction_error") > ref_mean_error + threshold * ref_stddev_error
            ).count()
            
            curr_count = curr_errors.count()
            curr_anomaly_ratio = curr_anomalies / curr_count if curr_count > 0 else 0
            
            # Calculate mean error for current data
            curr_mean_error = curr_errors.select(F.mean("reconstruction_error")).collect()[0][0]
            
            # Calculate relative change
            error_change = (curr_mean_error - ref_mean_error) / ref_mean_error if ref_mean_error > 0 else 0
            
            if ref_anomaly_ratio > 0:
                anomaly_change = (curr_anomaly_ratio - ref_anomaly_ratio) / ref_anomaly_ratio
            elif curr_anomaly_ratio > 0:
                anomaly_change = 1.0
            else:
                anomaly_change = 0.0
                
            # Determine if change is significant
            is_significant = abs(error_change) >= 0.3 or abs(anomaly_change) >= 0.5
            
            return {
                "status": "success",
                "is_significant": is_significant,
                "columns_analyzed": cols,
                "ref_mean_error": float(ref_mean_error),
                "curr_mean_error": float(curr_mean_error),
                "error_change": float(error_change),
                "ref_anomaly_count": int(ref_anomalies),
                "curr_anomaly_count": int(curr_anomalies),
                "ref_anomaly_ratio": float(ref_anomaly_ratio),
                "curr_anomaly_ratio": float(curr_anomaly_ratio),
                "anomaly_change": float(anomaly_change),
                "explained_variance": [float(v) for v in explained_variance],
                "severity": "high" if is_significant and (abs(error_change) >= 0.5 or abs(anomaly_change) >= 1.0) else "medium"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            } 