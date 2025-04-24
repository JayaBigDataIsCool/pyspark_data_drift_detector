from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, Any, List
from datetime import datetime
import json

class ResultHandler:
    """Handles result consolidation and formatting"""
    
    @staticmethod
    def create_result_schema() -> StructType:
        """Create schema for results DataFrame"""
        return StructType([
            StructField("run_timestamp", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("column_type", StringType(), False),
            StructField("dimension_id", StringType(), False),
            StructField("drift_score", DoubleType(), False),
            StructField("metrics", StringType(), True)  # JSON string of detailed metrics
        ])
    
    @staticmethod
    def create_results_dataframe(results: List[Dict]) -> DataFrame:
        """Create a DataFrame from drift results
        
        Args:
            results: List of result dictionaries
            
        Returns:
            DataFrame with consolidated results
        """
        spark = SparkSession.builder.getOrCreate()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        rows = []
        for result in results:
            row = {
                "run_timestamp": timestamp,
                "column_name": result["column_name"],
                "column_type": result["column_type"],
                "dimension_id": result.get("dimension_id", "all"),
                "drift_score": result["drift_score"],
                "metrics": json.dumps(result.get("metrics", {}))
            }
            rows.append(row)
        
        # Create schema
        schema = ResultHandler.create_result_schema()
        
        # Create DataFrame
        return spark.createDataFrame(rows, schema)
    
    @staticmethod
    def get_drift_severity(score: float) -> str:
        """Get a human-readable drift severity
        
        Args:
            score: Drift score (0-1)
            
        Returns:
            String severity label
        """
        if score < 0.1:
            return "None"
        elif score < 0.25:
            return "Low"
        elif score < 0.5:
            return "Medium"
        elif score < 0.75:
            return "High"
        else:
            return "Critical"
    
    @staticmethod
    def get_drift_summary(results_df: DataFrame, dimension_id: str = "all") -> Dict[str, Any]:
        """Get a summary of drift statistics
        
        Args:
            results_df: DataFrame with drift results
            dimension_id: Optional filter for specific dimension
            
        Returns:
            Dictionary with summary statistics
        """
        # Filter by dimension if specified
        if dimension_id != "all":
            filtered_df = results_df.filter(F.col("dimension_id") == dimension_id)
        else:
            filtered_df = results_df.filter(F.col("dimension_id") == "all")
        
        # Add severity column
        results_with_severity = filtered_df.withColumn(
            "drift_severity", 
            F.when(F.col("drift_score") < 0.1, "None")
            .when(F.col("drift_score") < 0.25, "Low")
            .when(F.col("drift_score") < 0.5, "Medium")
            .when(F.col("drift_score") < 0.75, "High")
            .otherwise("Critical")
        )
        
        # Get counts by severity
        severity_counts = results_with_severity.groupBy("drift_severity").count().collect()
        severity_stats = {row["drift_severity"]: row["count"] for row in severity_counts}
        
        # Get top drifted columns
        top_drifted = results_with_severity.orderBy(F.col("drift_score").desc()).limit(5).collect()
        top_columns = [{"column": row["column_name"], "drift_score": row["drift_score"], "severity": row["drift_severity"]} 
                      for row in top_drifted]
        
        # Calculate average drift score
        avg_drift = filtered_df.select(F.avg("drift_score")).collect()[0][0]
        
        return {
            "dimension_id": dimension_id,
            "total_columns_analyzed": filtered_df.count(),
            "average_drift_score": avg_drift,
            "severity_distribution": severity_stats,
            "top_drifted_columns": top_columns
        }
    
    @staticmethod
    def get_dimensional_summaries(results_df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """Get drift summaries for all dimensions
        
        Args:
            results_df: DataFrame with drift results
            
        Returns:
            Dictionary mapping dimension_id to summary stats
        """
        # Get all unique dimension IDs
        dimensions = results_df.select("dimension_id").distinct().collect()
        dimension_ids = [row["dimension_id"] for row in dimensions]
        
        # Generate summary for each dimension
        summaries = {}
        for dim_id in dimension_ids:
            summaries[dim_id] = ResultHandler.get_drift_summary(results_df, dim_id)
        
        return summaries
    
    @staticmethod
    def get_top_dimensions_by_drift(results_df: DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
        """Get dimensions with highest average drift
        
        Args:
            results_df: DataFrame with drift results
            top_n: Number of top dimensions to return
            
        Returns:
            List of top dimensions by average drift
        """
        # Skip the global "all" dimension
        dim_avgs = results_df.filter(F.col("dimension_id") != "all") \
                             .groupBy("dimension_id") \
                             .agg(F.avg("drift_score").alias("avg_drift"), 
                                  F.count("*").alias("column_count")) \
                             .orderBy("avg_drift", ascending=False) \
                             .limit(top_n) \
                             .collect()
        
        return [{"dimension_id": row["dimension_id"], 
                 "avg_drift_score": row["avg_drift"],
                 "column_count": row["column_count"],
                 "severity": ResultHandler.get_drift_severity(row["avg_drift"])}
                for row in dim_avgs]
    
    @staticmethod
    def consolidate_results(all_results: List[Dict], config: Dict[str, Any]) -> DataFrame:
        """Consolidate results into a DataFrame
        
        Args:
            all_results: List of result dictionaries
            config: Configuration dictionary with output path
            
        Returns:
            DataFrame with consolidated results
        """
        spark = SparkSession.builder.getOrCreate()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        results_rows = []
        for result in all_results:
            row = {
                "run_timestamp": timestamp,
                "column_name": result["column_name"],
                "analysis_type": result["analysis_type"],
                "dimension_slice": result["dimension_slice"],
                "metric_checked": result["metric_checked"],
                "reference_value": str(result["reference_value"]),
                "current_value": str(result["current_value"]),
                "diff_value": float(result["diff_value"]),
                "threshold_value": float(result["threshold_value"]),
                "drift_detected": bool(result["drift_detected"])
            }
            results_rows.append(row)
        
        # Create schema
        schema = ResultHandler.create_result_schema()
        
        # Create DataFrame from results
        return spark.createDataFrame(results_rows, schema)
    
    @staticmethod
    def write_results(results_df: DataFrame, output_path: str) -> None:
        """Write results to Delta table
        
        Args:
            results_df: DataFrame with results
            output_path: Path to write results to
        """
        try:
            results_df.write.format("delta").mode("append").save(output_path)
            print(f"Results written to {output_path}")
            
            # Print summary stats
            drift_count = results_df.filter(F.col("drift_detected") == True).count()
            total_metrics = results_df.count()
            distinct_columns = results_df.select("column_name").distinct().count()
            
            print(f"Summary: {drift_count} drifts detected out of {total_metrics} metrics checked across {distinct_columns} columns")
        except Exception as e:
            raise Exception(f"Failed to write results to {output_path}: {str(e)}") 