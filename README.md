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

1. make sure java, spark, pyspark, iceberg are perfectly configured.

2. Clone the repository:
```bash
https://github.com/adamaulia/Spark-ETL.git
cd Spark-ETL
```

3. Install required Python packages:
```bash
pip install pyspark findspark pyyaml python-dotenv
```

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
├── etl_spark.py        # Main ETL script
├── config.yaml         # Configuration file
├── cleansed /          # output clean files
├── iceberg /           # Iceberg warehouse
├── Sales_Data_1 /      # Input csv 
│   ├── Sales_Data /    # Source CSV files
└── spark_job.log       # Application logs
```

## Usage

Run the ETL pipeline:

```bash
python etl_spark.py
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
Stored in `{spark_catalog}.{db}.process_logs` with fields:
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
Stored in `{spark_catalog}.{db}.error_logs` with fields:
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
I choose parquet files because of the benefit of size and speed. In addition, partition by month 
improves performance, as queries scan only the relevant partitions instead of the entire dataset. 

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

Every major operation is wrapped in try-catch. Here's my logic:

- **Schema problems or table creation fails**: Kill the whole thing. Can't continue without the basics columns.

    - prosess logs 
    ```
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
        |                uuid|            ts_start|              ts_end|duration_seconds|process_name|    sub_process_name|table_destination|record_insert|column_length|
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
        |e1daebdc-9ac6-408...|2025-11-17 18:59:...|2025-11-17 18:59:...|        1.718924|    ETL ADLS|load csv from source|  spark dataframe|        18383|            5|
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
    ```

    - error logs 
    ```
        +--------------------+--------------------+------------+--------------------+-----------------+--------------------+
        |           timestamp|                uuid|process_name|    sub_process_name|table_destination|       error_message|
        +--------------------+--------------------+------------+--------------------+-----------------+--------------------+
        |2025-11-17T18:59:...|e1daebdc-9ac6-408...|    ETL ADLS|check missing_col...|  spark dataframe|Missing columns: ...|
        +--------------------+--------------------+------------+--------------------+-----------------+--------------------+

    ```

    - spark logs 
    ```
        2025-11-17 18:59:36,890 INFO spark_logger: Logs table ensured
        2025-11-17 18:59:36,962 INFO spark_logger: Error log table ensured
        2025-11-17 18:59:38,681 INFO spark_logger: CSV file read from source
        2025-11-17 18:59:47,973 ERROR spark_logger: Missing Columns: ['Purchase Address']
        2025-11-17 18:59:47,974 ERROR spark_logger: send alert to the team and process stop
        2025-11-17 18:59:55,458 INFO py4j.clientserver: Closing down clientserver connection

    ```

- **Nulls or duplicates found**: Log it but keep going. These aren't always deal-breakers.

    -   an example for null removal and duplicate handling in the spark_catalog.db.process_logs. It can be seen the record_insert drop from 186850 to 185688.


        ```
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
        |                uuid|            ts_start|              ts_end|duration_seconds|process_name|    sub_process_name|table_destination|record_insert|column_length|
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
        |96637efa-8659-47e...|2025-11-16 20:06:...|2025-11-16 20:06:...|        5.281171|    ETL ADLS|load csv from source|  spark dataframe|       186850|            6|
        |96637efa-8659-47e...|2025-11-16 20:07:...|2025-11-16 20:07:...|             0.0|    ETL ADLS|check missing_col...|  spark dataframe|       186850|            6|
        |96637efa-8659-47e...|2025-11-16 20:08:...|2025-11-16 20:08:...|        0.025135|    ETL ADLS|     cast order date|  spark dataframe|       185688|            7|
        |96637efa-8659-47e...|2025-11-16 20:09:...|2025-11-16 20:09:...|        0.021247|    ETL ADLS|convert to date only|  spark dataframe|       185688|            8|
        |96637efa-8659-47e...|2025-11-16 20:10:...|2025-11-16 20:10:...|       16.388351|    ETL ADLS|       write parquet|    spark parquet|       185688|            8|
        +--------------------+--------------------+--------------------+----------------+------------+--------------------+-----------------+-------------+-------------+
        ```
    - this error logs shows the check null column and duplicate   
    
        - spark_catalog.db.error_logs
        ```
            +--------------------+--------------------+------------+------------------+-----------------+--------------------+
            |           timestamp|                uuid|process_name|  sub_process_name|table_destination|       error_message|
            +--------------------+--------------------+------------+------------------+-----------------+--------------------+
            |2025-11-16T20:07:...|96637efa-8659-47e...|    ETL ADLS|check null_columns|  spark dataframe|Null columns: Ord...|
            |2025-11-16T20:08:...|96637efa-8659-47e...|    ETL ADLS|  check duplicates|  spark dataframe|Duplicate rows fo...|
            +--------------------+--------------------+------------+------------------+-----------------+--------------------+

        ```

    - this spark logs shows the detail null and duplicate

        -   spark_job.log 
        ```
            2025-11-16 20:07:54,164 ERROR spark_logger: Null Check: {'Order ID': 545, 'Product': 545, 'Quantity Ordered': 545, 'Price Each': 545, 'Order Date': 545, 'Purchase Address': 545}
            2025-11-16 20:08:31,882 ERROR spark_logger: Duplicate Rows Found: 266
        ``` 

## Troubleshooting

### Common Issues

**Issue**: `java.lang.ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`
- Solution: Verify the Iceberg JAR path in `config.yaml`

**Issue**: `FileNotFoundError` for CSV files
- Solution: Check the `raw_data_path` configuration and ensure CSV files exist

**Issue**: Memory errors with large datasets
- Solution: Adjust Spark configuration for executor and driver memory
