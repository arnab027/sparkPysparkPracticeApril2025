import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator
import numpy as np
import pandas as pd
import pyarrow
import logging
import os

# Configure logging with timestamp and filename
log_format = '%(asctime)s - %(levelname)s - %(filename)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(os.path.basename(__file__))

try:
    # Initialize Spark session with Delta Lake support
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.execution.arrow.pyspark.enabled", True) \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000") \
        .config('spark.sql.debug.maxToStringFields', 2000) \
        .config('spark.debug.maxToStringFields', 2000)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info("Spark session initialized successfully")

    df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))
    logger.info("Creating test DataFrame...")
    def filter_func(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            yield pyarrow.RecordBatch.from_pandas(pdf[pdf.id == 1])

    df.mapInArrow(filter_func, df.schema).show()
    logger.info("MapInArrow operation completed successfully")
except Exception as e:
    logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)