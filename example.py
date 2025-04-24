from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import os
from datetime import datetime, timedelta
import random

# Import our modules
from data_drift_detector import run_data_drift_detection
from config_generator import generate_sample_config

def create_sample_delta_table():
    """Create a sample Delta table with multiple versions for testing the drift detector"""
    
    spark = SparkSession.builder.getOrCreate()
    print("Creating sample Delta table for testing...")
    
    # Create base data (version 1)
    data = []
    for i in range(10000):
        data.append({
            "id": i,
            "numeric_normal": random.normalvariate(100, 15),
            "numeric_uniform": random.uniform(0, 100),
            "category_balanced": random.choice(["A", "B", "C", "D"]),
            "category_imbalanced": random.choice(["X"] * 70 + ["Y"] * 20 + ["Z"] * 10),
            "binary_feature": random.choice([0, 1]),
            "date_col": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
            "null_col": None if random.random() < 0.1 else random.randint(1, 100)
        })
    
    df1 = spark.createDataFrame(data)
    
    # Write version 1
    table_path = "/tmp/sample_drift_data"
    
    if os.path.exists(table_path):
        import shutil
        shutil.rmtree(table_path)
    
    df1.write.format("delta").save(table_path)
    print("Created version 1 (baseline)")
    
    # Create version 2 with mild drift
    data2 = []
    for i in range(10000):
        data2.append({
            "id": i + 5000,  # Slight overlap with version 1
            "numeric_normal": random.normalvariate(105, 16),  # Slight mean shift
            "numeric_uniform": random.uniform(0, 100),
            "category_balanced": random.choice(["A", "B", "C", "D"]),
            "category_imbalanced": random.choice(["X"] * 65 + ["Y"] * 25 + ["Z"] * 10),  # Slight distribution shift
            "binary_feature": random.choice([0, 1]),
            "date_col": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
            "null_col": None if random.random() < 0.12 else random.randint(1, 100)  # Increased nulls
        })
    
    df2 = spark.createDataFrame(data2)
    
    # Write version 2
    df2.write.format("delta").mode("overwrite").save(table_path)
    print("Created version 2 (mild drift)")
    
    # Create version 3 with significant drift
    data3 = []
    for i in range(10000):
        data3.append({
            "id": i + 10000,  # No overlap with version 1
            "numeric_normal": random.normalvariate(120, 25),  # Significant mean and std shift
            "numeric_uniform": random.uniform(0, 150),  # Range increased
            "category_balanced": random.choice(["A", "B", "C", "E"]),  # E instead of D
            "category_imbalanced": random.choice(["X"] * 40 + ["Y"] * 40 + ["Z"] * 20),  # Major distribution shift
            "binary_feature": random.choice([0, 1, 0, 0]),  # Imbalanced now
            "date_col": (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d"),  # More recent dates
            "null_col": None if random.random() < 0.25 else random.randint(1, 100)  # Significantly increased nulls
        })
    
    df3 = spark.createDataFrame(data3)
    
    # Add a new column to version 3
    df3 = df3.withColumn("new_feature", F.round(F.rand() * 100, 0))
    
    # Write version 3
    df3.write.format("delta").mode("overwrite").save(table_path)
    print("Created version 3 (significant drift)")
    
    return table_path

def run_example():
    """Run a complete example of the data drift detector"""
    
    # Create sample Delta table with multiple versions
    table_path = create_sample_delta_table()
    
    # Generate configuration file for comparing version 1 to 2 (mild drift)
    config_path_mild = "/tmp/config_mild_drift.json"
    generate_sample_config(
        table_path=table_path,
        reference_version=1,
        current_version=2,
        output_path=config_path_mild,
        profile="standard"
    )
    print(f"Generated configuration at {config_path_mild}")
    
    # Run drift detection for mild drift
    print("\nRunning drift detection for mild drift (v1 vs v2):")
    results_mild = run_data_drift_detection(config_path_mild)
    
    # Print summary of mild drift results
    print(f"\nDrift detected: {results_mild['drift_detected']}")
    if results_mild['drift_detected']:
        print("Drift summary:")
        for key, value in results_mild.get('drift_summary', {}).items():
            if key != 'overall_assessment':
                print(f"  - {key}: {value}")
        
        print(f"\nOverall assessment: {results_mild['drift_summary'].get('overall_assessment', 'N/A')}")
        
        print("\nRecommended actions:")
        for action in results_mild.get('recommended_actions', []):
            print(f"  - {action}")
    
    # Generate configuration file for comparing version 1 to 3 (significant drift)
    config_path_significant = "/tmp/config_significant_drift.json"
    generate_sample_config(
        table_path=table_path,
        reference_version=1,
        current_version=3,
        output_path=config_path_significant,
        profile="standard"
    )
    print(f"\nGenerated configuration at {config_path_significant}")
    
    # Run drift detection for significant drift
    print("\nRunning drift detection for significant drift (v1 vs v3):")
    results_significant = run_data_drift_detection(config_path_significant)
    
    # Print summary of significant drift results
    print(f"\nDrift detected: {results_significant['drift_detected']}")
    if results_significant['drift_detected']:
        print("Drift summary:")
        for key, value in results_significant.get('drift_summary', {}).items():
            if key != 'overall_assessment':
                print(f"  - {key}: {value}")
        
        print(f"\nOverall assessment: {results_significant['drift_summary'].get('overall_assessment', 'N/A')}")
        
        print("\nRecommended actions:")
        for action in results_significant.get('recommended_actions', []):
            print(f"  - {action}")
    
    print("\nExample completed successfully!")

if __name__ == "__main__":
    run_example() 