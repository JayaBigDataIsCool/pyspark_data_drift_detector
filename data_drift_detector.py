from pyspark.sql import SparkSession, DataFrame
import json
import datetime
from typing import Dict, Any, List, Optional, Tuple

# Import all analyzer modules
from config_manager import ConfigManager
from data_loader import DataLoader
from column_analyzer import ColumnAnalyzer
from schema_analyzer import SchemaAnalyzer
from numerical_analyzer import NumericalAnalyzer
from categorical_analyzer import CategoricalAnalyzer
from distribution_analyzer import DistributionAnalyzer
from correlation_analyzer import CorrelationAnalyzer
from group_analyzer import GroupAnalyzer

class DataDriftDetector:
    """Main class for detecting data drift between Delta table versions"""
    
    def __init__(self, config_path: str):
        """Initialize the data drift detector with config
        
        Args:
            config_path: Path to the configuration file in DBFS
        """
        self.config = ConfigManager.load_config_and_defaults(config_path)
        self.spark = SparkSession.builder.getOrCreate()
    
    def detect_drift(self) -> Dict[str, Any]:
        """Run data drift detection between table versions
        
        Returns:
            Dictionary with complete drift analysis results
        """
        start_time = datetime.datetime.now()
        
        # Initialize results object
        results = {
            "analysis_timestamp": start_time.isoformat(),
            "table_path": self.config["table_path"],
            "reference_version": self.config["reference_version"],
            "current_version": self.config["current_version"],
            "profile": self.config.get("profile", "standard"),
            "metrics": {},
            "drift_detected": False,
            "drift_summary": {},
            "execution_time_sec": None
        }
        
        try:
            # Load data from reference and current versions
            print(f"Loading table versions: ref={self.config['reference_version']}, curr={self.config['current_version']}")
            df_ref = DataLoader.load_data(self.config["table_path"], self.config["reference_version"])
            df_curr = DataLoader.load_data(self.config["table_path"], self.config["current_version"])
            
            # Schema analysis
            print("Analyzing schema changes...")
            schema_results = SchemaAnalyzer.analyze_schema(df_ref, df_curr)
            results["schema_analysis"] = schema_results
            
            # Determine column types and which columns to analyze
            print("Determining column types...")
            column_info = ColumnAnalyzer.infer_column_types(df_curr, self.config)
            numerical_columns = column_info["numerical_columns"]
            categorical_columns = column_info["categorical_columns"]
            temporal_columns = column_info["temporal_columns"]
            included_columns = column_info["included_columns"]
            
            # Store column information in results
            results["column_info"] = {
                "numerical_count": len(numerical_columns),
                "categorical_count": len(categorical_columns),
                "temporal_count": len(temporal_columns),
                "total_analyzed": len(included_columns),
                "column_types": column_info["column_types"]
            }
            
            # Analyze numerical columns
            if numerical_columns:
                print(f"Analyzing {len(numerical_columns)} numerical columns...")
                num_results = NumericalAnalyzer.analyze_numerical_columns(
                    df_ref, df_curr, numerical_columns, self.config
                )
                results["metrics"]["numerical"] = num_results
                
                # Check for drift in numerical columns
                drift_columns = [
                    col for col, metrics in num_results.items() 
                    if metrics.get("drift_detected", False)
                ]
                
                if drift_columns:
                    results["drift_detected"] = True
                    results["drift_summary"]["numerical_drift_columns"] = drift_columns
                    results["drift_summary"]["numerical_drift_count"] = len(drift_columns)
            
            # Analyze categorical columns
            if categorical_columns:
                print(f"Analyzing {len(categorical_columns)} categorical columns...")
                cat_results = CategoricalAnalyzer.analyze_categorical_columns(
                    df_ref, df_curr, categorical_columns, self.config
                )
                results["metrics"]["categorical"] = cat_results
                
                # Check for drift in categorical columns
                drift_columns = [
                    col for col, metrics in cat_results.items() 
                    if metrics.get("drift_detected", False)
                ]
                
                if drift_columns:
                    results["drift_detected"] = True
                    results["drift_summary"]["categorical_drift_columns"] = drift_columns
                    results["drift_summary"]["categorical_drift_count"] = len(drift_columns)
            
            # Distribution analysis
            if self.config.get("analyze_distributions", True):
                print("Analyzing distributions...")
                dist_results = DistributionAnalyzer.analyze_distributions(
                    df_ref, df_curr, numerical_columns, categorical_columns, self.config
                )
                results["distribution_analysis"] = dist_results
                
                # Check for distribution drift
                if dist_results.get("status") == "success":
                    # Count numerical columns with significant distribution changes
                    num_dist_drift = sum(
                        1 for col, metrics in dist_results.get("numerical_distribution_changes", {}).items()
                        if metrics.get("skew_change") != "none" or metrics.get("kurt_change") != "none"
                    )
                    
                    # Count categorical columns with significant distribution changes
                    cat_dist_drift = sum(
                        1 for col, metrics in dist_results.get("categorical_distribution_changes", {}).items()
                        if metrics.get("significant_change", False)
                    )
                    
                    if num_dist_drift > 0 or cat_dist_drift > 0:
                        results["drift_detected"] = True
                        results["drift_summary"]["distribution_drift"] = {
                            "numerical_distribution_drift_count": num_dist_drift,
                            "categorical_distribution_drift_count": cat_dist_drift
                        }
            
            # Correlation analysis
            if self.config.get("analyze_correlations", True) and len(numerical_columns) >= 2:
                print("Analyzing correlations...")
                corr_results = CorrelationAnalyzer.analyze_correlations(
                    df_ref, df_curr, numerical_columns, self.config
                )
                results["correlation_analysis"] = corr_results
                
                # Check for correlation drift
                if corr_results.get("status") == "success":
                    significant_shifts = len(corr_results.get("significant_correlation_shifts", []))
                    new_correlations = len(corr_results.get("new_correlations", []))
                    disappeared_correlations = len(corr_results.get("disappeared_correlations", []))
                    
                    if significant_shifts > 0 or new_correlations > 0 or disappeared_correlations > 0:
                        results["drift_detected"] = True
                        results["drift_summary"]["correlation_drift"] = {
                            "significant_correlation_shifts": significant_shifts,
                            "new_correlations": new_correlations,
                            "disappeared_correlations": disappeared_correlations
                        }
            
            # Group analysis
            if self.config.get("analyze_groups", True) and len(included_columns) >= 2:
                group_columns = self.config.get("group_columns", categorical_columns[:3])
                if group_columns:
                    print(f"Analyzing groups by {len(group_columns)} dimensions...")
                    group_results = GroupAnalyzer.analyze_grouped_data(
                        df_ref, df_curr, group_columns, numerical_columns, self.config
                    )
                    results["group_analysis"] = group_results
                    
                    # Check for group drift
                    if group_results.get("status") == "success":
                        drift_dimensions = []
                        for dim, metrics in group_results.get("group_metrics", {}).items():
                            if metrics.get("drift_detected", False):
                                drift_dimensions.append(dim)
                        
                        if drift_dimensions:
                            results["drift_detected"] = True
                            results["drift_summary"]["group_drift"] = {
                                "drift_dimensions": drift_dimensions,
                                "drift_dimension_count": len(drift_dimensions)
                            }
            
            # Multi-dimensional correlation analysis if specified
            target_column = self.config.get("target_column")
            if target_column and self.config.get("analyze_feature_importance", False):
                if target_column in numerical_columns:
                    predictor_columns = [c for c in numerical_columns if c != target_column]
                    if predictor_columns:
                        print(f"Analyzing feature importance against target: {target_column}")
                        importance_results = CorrelationAnalyzer.analyze_feature_importance_drift(
                            df_ref, df_curr, target_column, predictor_columns, self.config
                        )
                        results["feature_importance_analysis"] = importance_results
                        
                        # Check for feature importance drift
                        if importance_results.get("status") == "success":
                            importance_shifts = importance_results.get("feature_importance_shifts", {}).get("importance_shifts", [])
                            if importance_shifts:
                                significant_shifts = [
                                    shift for shift in importance_shifts 
                                    if shift.get("abs_change", 0) >= 0.1  # Threshold for significant change
                                ]
                                if significant_shifts:
                                    results["drift_detected"] = True
                                    results["drift_summary"]["feature_importance_drift"] = {
                                        "significant_shifts_count": len(significant_shifts)
                                    }
            
            # Generate drift summary
            if results["drift_detected"]:
                results["drift_summary"]["overall_assessment"] = self._generate_drift_assessment(results)
                
                # Generate recommended actions
                results["recommended_actions"] = self._generate_recommendations(results)
            
            # Calculate execution time
            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            results["execution_time_sec"] = execution_time
            
            print(f"Data drift analysis completed in {execution_time:.2f} seconds")
            print(f"Drift detected: {results['drift_detected']}")
            
            return results
            
        except Exception as e:
            # Handle errors
            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            results["status"] = "failed"
            results["error"] = str(e)
            results["execution_time_sec"] = execution_time
            
            print(f"Error in data drift detection: {str(e)}")
            return results
    
    def save_results(self, results: Dict[str, Any]) -> None:
        """Save drift detection results to Delta table
        
        Args:
            results: Dictionary with drift analysis results
        """
        if "output_table" not in self.config:
            print("No output table specified. Results will not be saved.")
            return
        
        try:
            # Convert results to JSON for storage
            results_json = json.dumps(results)
            
            # Create a DataFrame with results
            results_df = self.spark.createDataFrame([
                {
                    "analysis_timestamp": results["analysis_timestamp"],
                    "table_path": results["table_path"],
                    "reference_version": results["reference_version"],
                    "current_version": results["current_version"],
                    "profile": results["profile"],
                    "drift_detected": results["drift_detected"],
                    "execution_time_sec": results["execution_time_sec"],
                    "results_json": results_json
                }
            ])
            
            # Write to Delta table
            results_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(self.config["output_table"])
            
            print(f"Results saved to {self.config['output_table']}")
            
        except Exception as e:
            print(f"Error saving results: {str(e)}")
    
    def _generate_drift_assessment(self, results: Dict[str, Any]) -> str:
        """Generate an overall assessment of detected drift
        
        Args:
            results: Dictionary with drift analysis results
            
        Returns:
            String with overall drift assessment
        """
        # Count total drift instances
        drift_count = 0
        drift_summary = results.get("drift_summary", {})
        
        # Count numerical column drift
        num_drift = drift_summary.get("numerical_drift_count", 0)
        drift_count += num_drift
        
        # Count categorical column drift
        cat_drift = drift_summary.get("categorical_drift_count", 0)
        drift_count += cat_drift
        
        # Count distribution drift
        dist_drift = drift_summary.get("distribution_drift", {})
        num_dist_drift = dist_drift.get("numerical_distribution_drift_count", 0)
        cat_dist_drift = dist_drift.get("categorical_distribution_drift_count", 0)
        drift_count += num_dist_drift + cat_dist_drift
        
        # Count correlation drift
        corr_drift = drift_summary.get("correlation_drift", {})
        corr_shifts = corr_drift.get("significant_correlation_shifts", 0)
        drift_count += corr_shifts
        
        # Count group drift
        group_drift = drift_summary.get("group_drift", {})
        group_dim_count = group_drift.get("drift_dimension_count", 0)
        drift_count += group_dim_count
        
        # Count feature importance drift
        feat_drift = drift_summary.get("feature_importance_drift", {})
        feat_shifts = feat_drift.get("significant_shifts_count", 0)
        drift_count += feat_shifts
        
        # Determine severity
        severity = "low"
        if drift_count > 10:
            severity = "high"
        elif drift_count > 5:
            severity = "medium"
        
        # Generate assessment message
        if severity == "high":
            assessment = "Significant data drift detected across multiple dimensions and metrics."
        elif severity == "medium":
            assessment = "Moderate data drift detected in several columns and relationships."
        else:
            assessment = "Minor data drift detected in a few columns or metrics."
        
        return f"{assessment} Severity: {severity.upper()}"
    
    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on detected drift
        
        Args:
            results: Dictionary with drift analysis results
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        drift_summary = results.get("drift_summary", {})
        
        # Check for schema changes
        schema_analysis = results.get("schema_analysis", {})
        if schema_analysis.get("added_columns") or schema_analysis.get("removed_columns"):
            recommendations.append(
                "Review schema changes. Schema evolution may require updates to downstream processes."
            )
        
        # Check for numerical drift
        if "numerical_drift_columns" in drift_summary:
            num_drift_cols = drift_summary["numerical_drift_columns"]
            if len(num_drift_cols) > 0:
                recommendations.append(
                    f"Investigate numerical drift in {len(num_drift_cols)} columns: {', '.join(num_drift_cols[:3])}{'...' if len(num_drift_cols) > 3 else ''}"
                )
        
        # Check for categorical drift
        if "categorical_drift_columns" in drift_summary:
            cat_drift_cols = drift_summary["categorical_drift_columns"]
            if len(cat_drift_cols) > 0:
                recommendations.append(
                    f"Investigate categorical drift in {len(cat_drift_cols)} columns: {', '.join(cat_drift_cols[:3])}{'...' if len(cat_drift_cols) > 3 else ''}"
                )
        
        # Check for distribution drift
        dist_drift = drift_summary.get("distribution_drift", {})
        if dist_drift:
            recommendations.append(
                "Review distribution changes. Significant shifts may impact statistical models and assumptions."
            )
        
        # Check for correlation drift
        corr_drift = drift_summary.get("correlation_drift", {})
        if corr_drift:
            if corr_drift.get("new_correlations", 0) > 0:
                recommendations.append(
                    "New strong correlations detected. Consider updating feature engineering processes."
                )
            if corr_drift.get("disappeared_correlations", 0) > 0:
                recommendations.append(
                    "Previously strong correlations have weakened. Validate feature importance in models."
                )
        
        # Check for group drift
        group_drift = drift_summary.get("group_drift", {})
        if group_drift:
            dims = group_drift.get("drift_dimensions", [])
            if dims:
                recommendations.append(
                    f"Significant group-level drift detected in dimensions: {', '.join(dims[:3])}{'...' if len(dims) > 3 else ''}"
                )
        
        # Check for feature importance drift
        feat_drift = drift_summary.get("feature_importance_drift", {})
        if feat_drift and feat_drift.get("significant_shifts_count", 0) > 0:
            recommendations.append(
                "Feature importance has changed significantly. Consider retraining models."
            )
        
        # Add generic recommendations if we have drift
        if results.get("drift_detected", False):
            recommendations.append(
                "Consider monitoring affected columns more frequently to establish trends."
            )
        
        return recommendations

def run_data_drift_detection(config_path: str) -> Dict[str, Any]:
    """Run data drift detection with the given configuration
    
    Args:
        config_path: Path to the configuration file in DBFS
    
    Returns:
        Dictionary with drift analysis results
    """
    detector = DataDriftDetector(config_path)
    results = detector.detect_drift()
    detector.save_results(results)
    return results

# Entry point for direct execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python data_drift_detector.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    run_data_drift_detection(config_path) 