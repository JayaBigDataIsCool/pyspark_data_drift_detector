# Databricks notebook source
# MAGIC %md
# MAGIC # Data Drift Detector Demo
# MAGIC 
# MAGIC This notebook demonstrates how to use the Data Drift Detector to automatically detect data drift between versions of a Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC First, let's make sure our code is available on the cluster

# COMMAND ----------

# DBTITLE 1,Import the Data Drift Detector
# Import the Data Drift Detector
from main import DataDriftDetector

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Basic Usage
# MAGIC 
# MAGIC Simply point the detector at a Delta table path or Unity Catalog table name

# COMMAND ----------

# DBTITLE 1,Run drift detection
# Run the drift detector on a table
table_path = "your_catalog.your_schema.your_table"  # Replace with your table

# Run detection 
results = DataDriftDetector.detect_drift(table_path)

# COMMAND ----------

# DBTITLE 1,View top drifted columns
# Show columns with highest drift
display(results.orderBy("drift_score", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Dimensional Analysis
# MAGIC 
# MAGIC Slice data by business dimensions to find localized drift

# COMMAND ----------

# DBTITLE 1,Run drift detection with dimensions
# Specify dimensions to analyze (e.g., region, product_category)
dimension_columns = ["region", "product_category"]

# Run with dimensional analysis
dimensional_results = DataDriftDetector.detect_drift(
    table_path,
    dimension_columns=dimension_columns
)

# COMMAND ----------

# DBTITLE 1,View results by dimension
# Filter to see results for a specific dimension slice
region_slice = "region=US/product_category=Electronics"
display(dimensional_results.filter(f"dimension_id = '{region_slice}'"))

# COMMAND ----------

# DBTITLE 1,Find top dimensions by drift
from result_handler import ResultHandler

# Get dimensions with highest drift
top_dimensions = ResultHandler.get_top_dimensions_by_drift(dimensional_results)

# Print top dimensions
for dim in top_dimensions:
    print(f"Dimension: {dim['dimension_id']}")
    print(f"  Average Drift: {dim['avg_drift_score']:.3f} ({dim['severity']})")
    print(f"  Columns Analyzed: {dim['column_count']}")
    print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Large Table Handling with Sampling

# COMMAND ----------

# DBTITLE 1,Run with sampling
# For very large tables, use sampling
large_table_results = DataDriftDetector.detect_drift(
    table_path,
    sampling_ratio=0.1  # Sample 10% of data
)

# Show results
display(large_table_results.orderBy("drift_score", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Outlier Detection
# MAGIC 
# MAGIC Detect rare value patterns and outliers

# COMMAND ----------

# DBTITLE 1,Run with outlier detection
# Enable outlier detection
outlier_results = DataDriftDetector.detect_drift(
    table_path,
    detect_outliers=True
)

# Find columns with outlier issues
from pyspark.sql import functions as F
import json

# Parse JSON metrics and extract outlier-related metrics
outlier_metrics = outlier_results.select(
    "column_name",
    "column_type",
    "drift_score",
    "metrics"
).collect()

print("Columns with outlier changes:")
for row in outlier_metrics:
    metrics = json.loads(row["metrics"])
    
    if row["column_type"] == "numerical" and "outlier_rate_diff" in metrics:
        if metrics["outlier_rate_diff"] > 0.01:  # 1% difference in outlier rate
            print(f"  {row['column_name']} (numerical):")
            print(f"    Outlier rate: {metrics['ref_outlier_rate']:.3f} → {metrics['curr_outlier_rate']:.3f}")
            print(f"    Extreme value rate: {metrics['ref_extreme_rate']:.3f} → {metrics['curr_extreme_rate']:.3f}")
            
    elif row["column_type"] == "categorical" and "rare_rate_diff" in metrics:
        if metrics["rare_rate_diff"] > 0.05:  # 5% difference in rare category rate
            print(f"  {row['column_name']} (categorical):")
            print(f"    Rare category rate: {metrics['ref_rare_rate']:.3f} → {metrics['curr_rare_rate']:.3f}")
            print(f"    Very rare category rate: {metrics['ref_very_rare_rate']:.3f} → {metrics['curr_very_rare_rate']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Comprehensive Analysis
# MAGIC 
# MAGIC Combine all features for a complete analysis

# COMMAND ----------

# DBTITLE 1,Run comprehensive analysis
# Run with all features
comprehensive_results = DataDriftDetector.detect_drift(
    table_path,
    output_path="dbfs:/path/to/drift_results",
    dimension_columns=["region", "product_category", "customer_segment"],
    sampling_ratio=0.2,
    detect_outliers=True
)

# Show summary of results by dimension
dimension_summaries = ResultHandler.get_dimensional_summaries(comprehensive_results)

# Show global summary
global_summary = dimension_summaries["all"]
print(f"Global Analysis Summary:")
print(f"  Total columns analyzed: {global_summary['total_columns_analyzed']}")
print(f"  Average drift score: {global_summary['average_drift_score']:.3f}")
print("\nSeverity distribution:")
for severity, count in global_summary['severity_distribution'].items():
    print(f"  {severity}: {count} columns")

# COMMAND ----------

# DBTITLE 1,Examine detailed metrics
from pyspark.sql import functions as F
import json

# Get the column with highest drift
top_column = comprehensive_results.filter("dimension_id = 'all'").orderBy("drift_score", ascending=False).limit(1).collect()[0]

# Parse the metrics JSON
metrics = json.loads(top_column["metrics"])

# Print detailed information
print(f"Column: {top_column['column_name']} ({top_column['column_type']})")
print(f"Drift score: {top_column['drift_score']:.3f}")
print(f"Drift severity: {ResultHandler.get_drift_severity(top_column['drift_score'])}")
print("\nDetailed metrics:")

if top_column['column_type'] == 'numerical':
    print(f"  Mean: {metrics['mean_ref']} → {metrics['mean_curr']}")
    print(f"  Std dev: {metrics['stddev_ref']} → {metrics['stddev_curr']}")
    print(f"  Range: [{metrics['min_ref']}, {metrics['max_ref']}] → [{metrics['min_curr']}, {metrics['max_curr']}]")
    print(f"  Percentiles (25/75/95/99):")
    print(f"    p25: {metrics.get('p25_ref')} → {metrics.get('p25_curr')}")
    print(f"    p75: {metrics.get('p75_ref')} → {metrics.get('p75_curr')}")
    print(f"    p95: {metrics.get('p95_ref')} → {metrics.get('p95_curr')}")
    print(f"    p99: {metrics.get('p99_ref')} → {metrics.get('p99_curr')}")
    
    if "ref_outlier_rate" in metrics:
        print(f"  Outlier metrics:")
        print(f"    Outlier rate: {metrics['ref_outlier_rate']:.3f} → {metrics['curr_outlier_rate']:.3f}")
        print(f"    Extreme value rate: {metrics['ref_extreme_rate']:.3f} → {metrics['curr_extreme_rate']:.3f}")
else:
    print(f"  Distinct values: {metrics['distinct_count_ref']} → {metrics['distinct_count_curr']}")
    print(f"  Null fraction: {metrics['null_fraction_ref']:.3f} → {metrics['null_fraction_curr']:.3f}")
    print(f"  JS divergence: {metrics['js_divergence']:.3f}")
    print(f"  Missing categories: {metrics['missing_category_count']} (examples: {metrics['missing_categories']})")
    print(f"  New categories: {metrics['new_category_count']} (examples: {metrics['new_categories']})")
    
    if "ref_rare_rate" in metrics:
        print(f"  Rare category metrics:")
        print(f"    Rare category rate: {metrics['ref_rare_rate']:.3f} → {metrics['curr_rare_rate']:.3f}")
        print(f"    Very rare category rate: {metrics['ref_very_rare_rate']:.3f} → {metrics['curr_very_rare_rate']:.3f}")
        print(f"    Categories changing rarity: {metrics['rare_to_common_count']} became common, {metrics['common_to_rare_count']} became rare")
        
    print(f"  Top categories (ref):")
    for item in metrics['top_categories_ref']:
        for cat, value in item.items():
            print(f"    {cat}: {value:.3f}")
    print(f"  Top categories (curr):")
    for item in metrics['top_categories_curr']:
        for cat, value in item.items():
            print(f"    {cat}: {value:.3f}")
            
# COMMAND ---------- 