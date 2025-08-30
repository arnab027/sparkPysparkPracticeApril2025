import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator
import numpy as np
import pandas as pd
import pyarrow

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


df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

def filter_func(iterator):
    for batch in iterator:
        pdf = batch.to_pandas()
        yield pyarrow.RecordBatch.from_pandas(pdf[pdf.id == 1])

df.mapInArrow(filter_func, df.schema).show()