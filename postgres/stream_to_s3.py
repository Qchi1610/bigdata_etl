import sys
import os
import warnings
import traceback
import logging
import dotenv
import json
from time import sleep
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampNTZType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, udf


utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
            
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]

MEMORY = spark_cfg['executor_memory']
###############################################


###############################################
# PySpark
###############################################
def create_spark_session():
    try: 
        spark = (SparkSession.builder.config("spark.executor.memory", MEMORY) \
                        .config(
                            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:2.8.2"
                        )
                        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                        .config("spark.hadoop.fs.s3a.path.style.access", "true")
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Streaming Processing Application")
                        .getOrCreate()
        )
        
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def create_initial_dataframe(spark_session):
    df = (spark_session
          .readStream
          .)


def create_final_dataframe(df, spark_session):
    


if __name__ == '__main__':
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)