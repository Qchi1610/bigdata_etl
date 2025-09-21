import os
from dotenv import load_dotenv
load_dotenv()
from pyspark.sql import SparkSession

MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")


spark = SparkSession.builder \