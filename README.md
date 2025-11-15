# Spark ETL Pipeline

A robust PySpark-based ETL pipeline for processing CSV data with data quality checks, Apache Iceberg integration, and comprehensive logging.

## Features

- **Data Quality Checks**: Automated validation for missing columns, null values, and duplicate records
- **Apache Iceberg Integration**: Modern table format with ACID transactions
- **Comprehensive Logging**: Process and error logs stored in Iceberg tables
- **Data Partitioning**: Automatic partitioning by report month for optimized queries
- **Error Handling**: Graceful error management with detailed error logging

## Prerequisites

- Python 3.7+
- Apache Spark 3.x
- Java 8 or 11
- Apache Iceberg JAR file

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <project-directory>
```

2. Install required Python packages:
```bash
pip install pyspark findspark pyyaml python-dotenv
```

3. Download the Apache Iceberg Spark runtime JAR and place it in your project directory.

## Configuration

Create a `config.yaml` file in the project root with the following structure:

```yaml
etl:
  raw_data_path: "data/raw/"
  cleansed_data_path: "data/cleansed/"

iceberg:
  jar: "path/to/iceberg-spark-runtime.jar"
  warehouse_location: "warehouse/"
  catalog_name: "spark_catalog"
  database_name: "db"
  logs_table: "process_logs"
  error_table: "error_logs"

expected_columns:
  - "Order Date"
  - "Column1"
  - "Column2"
  # Add your expected columns here
```

## Project Structure

```
project/
├── main.py                 # Main ETL script
├── config.yaml            # Configuration file
├── .env                   # Environment variables (optional)
├── data/
│   ├── raw/              # Source CSV files
│   └── cleansed/         # Output parquet files
├── warehouse/            # Iceberg warehouse
└── spark_job.log         # Application logs
```

## Usage

Run the ETL pipeline:

```bash
python main.py
```

The pipeline will:
1. Read CSV files from the raw data path
2. Perform data quality checks
3. Transform date columns
4. Partition data by month
5. Write to parquet format
6. Log all operations to Iceberg tables

## Data Quality Checks

The pipeline performs the following validations:

### Missing Columns Check
Validates that all expected columns from `config.yaml` are present in the source data.

### Null Values Check
Identifies and logs columns containing null values.

### Duplicate Rows Check
Detects and removes duplicate records using SHA-256 hashing.

## Logging

### Process Logs
Stored in `{catalog}.{database}.process_logs` with fields:
- `uuid`: Unique process identifier
- `ts_start`: Process start timestamp
- `ts_end`: Process end timestamp
- `duration_seconds`: Process duration
- `process_name`: Name of the ETL process
- `sub_process_name`: Name of the sub-process
- `table_destination`: Target table/dataframe
- `record_insert`: Number of records processed
- `column_length`: Number of columns

### Error Logs
Stored in `{catalog}.{database}.error_logs` with fields:
- `timestamp`: Error occurrence time
- `uuid`: Process identifier
- `process_name`: Process where error occurred
- `sub_process_name`: Sub-process where error occurred
- `table_destination`: Target destination
- `error_message`: Detailed error message

## Data Transformations

1. **Date Parsing**: Converts "Order Date" from string to timestamp format (`MM/dd/yy HH:mm`)
2. **Month Extraction**: Creates `report_month` column in `yyyy-MM` format
3. **Partitioning**: Data is partitioned by `report_month` for efficient querying

## Output

Processed data is written to the cleansed data path in Parquet format with the following structure:
```
data/cleansed/
├── report_month=2024-01/
├── report_month=2024-02/
└── report_month=2024-03/
```

## Monitoring

Check the following for pipeline status:
- Console output for real-time progress
- `spark_job.log` for detailed application logs
- Process logs table for execution metrics
- Error logs table for failures and warnings

## Error Handling

The pipeline continues processing even when non-critical errors occur. All errors are:
- Logged to the error logs table
- Written to the application log file
- Displayed in the console

## Troubleshooting

### Common Issues

**Issue**: `java.lang.ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`
- Solution: Verify the Iceberg JAR path in `config.yaml`

**Issue**: `FileNotFoundError` for CSV files
- Solution: Check the `raw_data_path` configuration and ensure CSV files exist

**Issue**: Memory errors with large datasets
- Solution: Adjust Spark configuration for executor and driver memory

## Performance Tuning

Consider these configurations for better performance:
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here]

## Contact

[Add contact information here]