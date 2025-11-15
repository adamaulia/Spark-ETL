import yaml
from dotenv import load_dotenv
import os
from datetime import date

# Step 0: Activate findspark
import findspark
findspark.init()

# Step 1: Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, col, from_unixtime, date_format, hour
from pyspark.sql.functions import isnan, when, count, col, sum
from pyspark.sql import functions as F
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.functions import input_file_name
from datetime import datetime
from pyspark.sql import Row
import logging
import uuid
import glob
import sys 

def check_missing_columns(df, expected_columns):
    
    """ 
    Check for missing columns in the DataFrame.
    Args:
        df (DataFrame): The Spark DataFrame to check.
        expected_columns (list): List of expected column names.
        
        returns: list: List of missing column names.
    """
    
    # missing columns check
    missing_columns = [col for col in expected_columns if col not in df.columns]

    return missing_columns

def check_null_columns(df):
    """
    Check for null values in each column of the DataFrame.
    Args:
        df (DataFrame): The Spark DataFrame to check.
    Returns:
        dict: A dictionary with column names as keys and counts of null values as values.
    """
    
    # Step 1: Count nulls per column
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
    ])

    # Step 2: Convert to dictionary
    null_dict = null_counts.collect()[0].asDict()

    # # Step 3: Loop and print only columns with missing values
    # for column, count in null_dict.items():
    #     if count > 0:
    #         print(f"Column '{column}' has {count} missing values.")
    return null_dict

def check_duplicates(df):
    """
    Check for duplicate rows in the DataFrame.
    Args:
        df (DataFrame): The Spark DataFrame to check.
    Returns:
        int: The count of duplicate rows.
    """
   
    # Step 1: Concatenate all columns into one string
    df_combined = df.withColumn("row_concat", concat_ws("||", *df.columns))

    # Step 2: Apply SHA-256 hash to the concatenated string
    df_hashed = df_combined.withColumn("row_sha256", sha2(col("row_concat"), 256))

    # step 3 : show duplicates based on hash
    result = df_hashed.groupBy("row_sha256").count().filter(col("count") > 1).count()
    
    return result

# Define log schema
def create_log(spark, uuid, ts_start, ts_end, process_name, sub_process_name ,table_destination, record_insert, column_length=None, log_table="spark_catalog.db.process_logs"):
    """ Create a log entry as a Row object.
    Args:
        spark (SparkSession): The Spark session object.
        uuid (str): Unique identifier for the process.
        ts_start (datetime): Start timestamp of the process.
        ts_end (datetime): End timestamp of the process.
        process_name (str): Name of the process.
        sub_process_name (str): Name of the sub-process.
        table_destination (str): Destination table name.
        record_insert (int): Number of records inserted.
        column_length (int): Number of columns processed.
    Returns:
        Row: A Row object containing the log entry.
    """
    
    msg = Row(
        # timestamp=datetime.now().isoformat(),
        uuid=uuid,
        ts_start=ts_start,
        ts_end=ts_end,
        duration_seconds=(ts_end - ts_start).total_seconds(),
        process_name=process_name,
        sub_process_name=sub_process_name,
        table_destination=table_destination,
        record_insert=record_insert,
        column_length=column_length
    )
    
    log_entry = [msg]
    log_df = spark.createDataFrame(log_entry)
    log_df.writeTo(f"{log_table}").append()
    return msg

def create_error_log(spark, uuid, process_name, sub_process_name ,table_destination, error_message, log_table="spark_catalog.db.error_logs"):
    """ Create an error log entry as a Row object.
    Args:
        spark (SparkSession): The Spark session object.
        process_name (str): Name of the process.
        sub_process_name (str): Name of the sub-process.
        table_destination (str): Destination table name.
        error_message (str): Error message.
    Returns:
        Row: A Row object containing the error log entry.
    """
    msg = Row(
        timestamp=datetime.now().isoformat(),
        uuid=uuid,
        process_name=process_name,
        sub_process_name=sub_process_name,
        table_destination=table_destination,
        error_message=error_message
    )

    error_log_entry = [msg]
    error_log_df = spark.createDataFrame(error_log_entry)
    error_log_df.writeTo(f"{log_table}").append()
    return msg

# Configure logging to file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("spark_job.log"),  # Log file path
        logging.StreamHandler()               # Also log to console
    ]
)
logger = logging.getLogger("spark_logger")

# set uuid     
uuid_ = str(uuid.uuid4())
logger.info(f"Process UUID: {uuid_}")


# Load YAML from a file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
    logger.info("load yaml file")


project_path = os.getcwd()

raw_data_path = os.path.join(project_path, config['etl']['raw_data_path']) 
clean_data_path = os.path.join(project_path, config['etl']['cleansed_data_path'])
iceberg_jar = config['iceberg']['jar']
warehouse_path = os.path.join(project_path, config['iceberg']['warehouse_location'])
iceberg_schema = config.get('iceberg').get('catalog_name') + "." + config.get('iceberg').get('database_name') 
logs_table = iceberg_schema + "." + config.get('iceberg').get('logs_table')
error_table = iceberg_schema + "." + config.get('iceberg').get('error_table') 

logger.info(f"raw_data_path: {raw_data_path}")
logger.info(f"clean_data_path: {clean_data_path}")
logger.info(f"iceberg_jar: {iceberg_jar}")
logger.info(f"warehouse_path: {warehouse_path}")
logger.info(f"iceberg_schema: {iceberg_schema}")
logger.info(f"logs_table: {logs_table}")
logger.info(f"error_table: {error_table}")

# create spark session with iceberg and s3a configurations
    
# spark = SparkSession.builder \
#     .appName("spark_etl") \
#     .master("local[*]") \
#     .config("spark.hadoop.io.nativeio.NativeIO.disable", "true")\
#     .config("spark.hadoop.fs.use.nativeio", "false")\
#     .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
#     .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
#     .getOrCreate()
    
spark = SparkSession.builder \
    .appName("spark_etl") \
    .config("spark.jars", iceberg_jar) \
    .config("spark.hadoop.hadoop.native.io", "false") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", warehouse_path) \
    .getOrCreate()
    
# Reduce Spark internal logs
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session created")


try :
    # create logs table and gold table if not exists
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {logs_table} (
      uuid STRING,
      ts_start STRING,
      ts_end STRING,
      duration_seconds DOUBLE,
      process_name STRING,
      sub_process_name STRING,
      table_destination STRING,
      record_insert INT,
      column_length INT
    )
    USING iceberg
    """)

    logger.info("Logs table ensured")
except Exception as e:
    logger.error(f"Error creating logs table: {str(e)}")
    # sys.exit()

try : 
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {error_table} (
        timestamp STRING,
        uuid STRING,
        process_name STRING,
        sub_process_name STRING,
        table_destination STRING,
        error_message STRING
    )
    USING iceberg
    """)

    logger.info("Error log table ensured")
except Exception as e:
    logger.error(f"Error creating error log table: {str(e)}")
    # sys.exit()

# Read CSV files from a folder or pattern
try :
    ts_start = datetime.now()
    df = spark.read.option("header", True).csv(raw_data_path + "*.csv")
    ts_end = datetime.now()
    logger.info("CSV file read from source")
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "load csv from source", "spark dataframe", df.count(), len(df.columns))
except Exception as e:  
    logger.error(f"Error during reading CSV from source: {str(e)}")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "load csv from source", raw_data_path, str(e))
    # sys.exit()

# Add filename column
# df = df.withColumn("source_file", input_file_name())

# data quality checks
# check missing columns
ts_start = datetime.now()
missing_columns = check_missing_columns(df,config['expected_columns'])
ts_end = datetime.now()

if missing_columns:
    # print(f"Missing columns: {missing_columns}")
    logger.error(f"Missing Columns: {missing_columns}")
    logger.error("send alert to the team and process stop")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "check missing_columns", "spark dataframe", f"Missing columns: {'|'.join(missing_columns)}")
    # sys.exit()
else:
    # print("No missing columns.")
    logger.info("No missing columns found.")
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "check missing_columns", "spark dataframe", df.count(), len(df.columns))

# check null values
ts_start = datetime.now()
null_check = check_null_columns(df)
ts_end = datetime.now()

if [(k,v) for k, v in null_check.items() if v > 0] : 
    # logger.error(f"Null Check: {null_check}")
    logger.error(f"Null Check: {null_check}")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "check null_columns", "spark dataframe", f"Null columns: {'|'.join([k for k, v in null_check.items() if v > 0])}")

else :
    # print("No missing values found.")
    logger.info("No missing values found.")
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "check null_columns", "spark dataframe", df.count(), len(df.columns))

# check duplicate rows
ts_start = datetime.now()
duplicate_count = check_duplicates(df)
ts_end = datetime.now()
if duplicate_count > 0:
    logger.error(f"Duplicate Rows Found: {duplicate_count}")
    df = df.dropDuplicates()
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "check duplicates", "spark dataframe", f"Duplicate rows found: {duplicate_count}")
else:
    logger.info("No duplicate rows found.")
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "remove duplicate", "spark dataframe", df.count()-duplicate_count, len(df.columns))

# data transformation 
try :
    # casting order date data types to from string to datetype  
    ts_start = datetime.now()
    df = df.withColumn(
        "Order Date New",
        F.to_timestamp("Order Date", "MM/dd/yy HH:mm")
    )
    ts_end = datetime.now()
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "cast order date", "spark dataframe", df.count(), len(df.columns))
except Exception as e:
    logger.error(f"Error during casting order date: {str(e)}")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "cast order date", "spark dataframe", str(e))
    # sys.exit()

# convert to date only (without time component)
# df = df.withColumn(
#     "order_date_only",
#     F.to_date("Order Date New", "MM/dd/yy HH:mm")
# )
try :
    ts_start = datetime.now()
    df = df.withColumn("report_month", F.date_format(F.col("Order Date New"), "yyyy-MM"))
    ts_end = datetime.now()
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "convert to date only", "spark dataframe", df.count(), len(df.columns))
except Exception as e:
    logger.error(f"Error during converting to date only: {str(e)}")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "convert to date only", "spark dataframe", str(e))
    # sys.exit()


try :
    # write to parquet partitioned by report_month
    ts_start = datetime.now()
    df.write \
      .mode("overwrite") \
      .partitionBy("report_month") \
      .parquet(clean_data_path)
    ts_end = datetime.now()
    log_entry = create_log(spark,uuid_, ts_start, ts_end, "ETL ADLS", "write parquet", "spark parquet", df.count(), len(df.columns))
except Exception as e:
    logger.error(f"Error during writing parquet: {str(e)}")
    error_entry = create_error_log(spark,uuid_,"ETL ADLS", "write parquet", "spark parquet", str(e))
    # sys.exit()

spark.stop()