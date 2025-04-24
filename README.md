# PySpark Data Drift Detector

A powerful, configurable, and scalable data drift detection tool for Delta Lake tables. It helps identify statistical and distributional changes between different versions of your data.

## Features

- **Pure PySpark Implementation**: Built entirely on PySpark, designed to work efficiently with large datasets
- **Delta Lake Integration**: Native support for Delta Lake tables with time travel capabilities
- **Comprehensive Analysis**: Detects multiple types of data drift:
  - Schema changes (added/removed columns, type changes)
  - Statistical drift in numerical columns
  - Distribution changes in categorical columns
  - Changes in correlations between columns
  - Multi-dimensional analysis across categorical groups
  - Distribution shape analysis and quantile shifts
  - Feature importance drift
- **Configuration Driven**: Fully configurable via JSON configuration files
- **Modular Design**: Well-structured, modular codebase that's easy to extend
- **Sensitivity Controls**: Multiple analysis profiles with configurable thresholds
- **Scalability**: Designed to handle large tables with billions of rows
- **Results Storage**: Save analysis results to Delta tables for historical tracking

## Installation

1. Clone this repository to your Databricks workspace or local environment:

```bash
git clone https://github.com/your-username/pyspark-data-drift-detector.git
```

2. Upload the code to your Databricks workspace or install in your PySpark environment.

## Quick Start

### 1. Generate a Configuration File

First, create a configuration file using the config generator:

```python
from config_generator import generate_sample_config

generate_sample_config(
    table_path="dbfs:/path/to/your/delta/table",
    reference_version=1,  # baseline version
    current_version=2,    # version to compare against
    output_path="config.json",
    output_table="dbfs:/path/to/results/table",  # optional
    profile="standard"    # analysis profile (summary, standard, deep_dive)
)
```

Alternatively, infer column types from your data:

```python
from config_generator import infer_and_generate_config

infer_and_generate_config(
    table_path="dbfs:/path/to/your/delta/table",
    reference_version=1,
    current_version=2,
    output_path="config.json",
    output_table="dbfs:/path/to/results/table",  # optional
    profile="standard"
)
```

### 2. Run the Drift Detector

```python
from data_drift_detector import run_data_drift_detection

results = run_data_drift_detection("config.json")
```

Or from command line:

```bash
python data_drift_detector.py config.json
```

## Configuration Options

The configuration file controls all aspects of the drift detection process:

```json
{
  "table_path": "dbfs:/path/to/your/delta/table",
  "reference_version": 1,
  "current_version": 2,
  "profile": "standard",
  "analyze_distributions": true,
  "analyze_correlations": true,
  "analyze_groups": true,
  "analyze_feature_importance": false,
  "target_column": null,
  "include_columns": [],
  "exclude_columns": [],
  "custom_column_types": {},
  "group_columns": [],
  "sample_size": 100000,
  "thresholds": {
    "standard": {
      "numerical": {
        "mean_threshold": 0.05,
        "median_threshold": 0.05,
        "std_threshold": 0.1,
        "iqr_threshold": 0.1,
        "null_threshold": 0.005
      },
      "categorical": {
        "category_threshold": 0.03,
        "chi_square_pvalue": 0.05,
        "null_threshold": 0.005
      },
      "correlation_threshold": 0.7,
      "correlation_change_threshold": 0.2,
      "js_distance_threshold": 0.1,
      "analyze_distributions": true
    }
  }
}
```

### Key Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `table_path` | Path to the Delta table to analyze | (Required) |
| `reference_version` | Version number for reference/baseline data | (Required) |
| `current_version` | Version number for current data | (Required) |
| `profile` | Analysis profile with predefined thresholds | `"standard"` |
| `analyze_distributions` | Whether to analyze distribution changes | `true` |
| `analyze_correlations` | Whether to analyze correlation changes | `true` |
| `analyze_groups` | Whether to analyze by categorical groups | `true` |
| `include_columns` | List of columns to include (empty = all) | `[]` |
| `exclude_columns` | List of columns to exclude | `[]` |
| `custom_column_types` | Override auto-detected column types | `{}` |
| `sample_size` | Maximum rows to analyze (0 = no sampling) | `100000` |

## Analysis Profiles

The tool comes with three predefined analysis profiles:

1. **Summary**: Fastest profile with higher thresholds, focusing on major changes only
2. **Standard**: Balanced profile with moderate thresholds (default)
3. **Deep Dive**: Comprehensive analysis with lower thresholds to catch subtle changes

## Results Format

The drift detection results include:

- Summary of detected drift
- Detailed metrics by column
- Schema changes
- Statistical metrics for numerical and categorical columns
- Distribution analysis
- Correlation changes
- Group-level analysis
- Recommended actions based on findings

## Example Usage in Databricks

```python
# Import the detector
from data_drift_detector import DataDriftDetector

# Initialize with configuration
detector = DataDriftDetector("dbfs:/path/to/config.json")

# Run detection
results = detector.detect_drift()

# Save results to a Delta table
detector.save_results(results)

# Display drift summary
display(spark.createDataFrame([{"drift_detected": results["drift_detected"]}]))

# Check recommended actions if drift is detected
if results["drift_detected"]:
    for action in results["recommended_actions"]:
        print(f"- {action}")
```

## Extending the Tool

The modular design makes it easy to extend the tool with new analyzers:

1. Create a new analyzer class in a separate file
2. Implement static analysis methods
3. Add the analyzer to the main `data_drift_detector.py` file
4. Update the relevant configuration options

## Performance Considerations

- Use appropriate sampling for very large tables
- Select only the columns you need to analyze
- For tables with billions of rows, start with the "summary" profile
- Consider partitioning your analysis by date or other dimensions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 