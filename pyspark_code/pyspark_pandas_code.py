import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pyspark
from delta import *
import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
start_date_range = '2025-01-01'
end_date_range = '2025-04-30'
print(ps.date_range(start_date_range,end_date_range))