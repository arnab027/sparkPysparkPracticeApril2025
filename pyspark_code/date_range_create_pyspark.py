import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import numpy as np
import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import Row
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
start_date_range = '2025-01-01'
end_date_range = '2025-04-30'
# Updated code
# NaN_value = np.nan # Correct usage in NumPy 2.0

date_df = spark.createDataFrame([(start_date_range,end_date_range)], schema=['start_date','end_date'])
date_df.show()
date_transformed_df = date_df.withColumn("start_date_range",to_date("start_date"))\
                             .withColumn("end_date_range",to_date("end_date"))

date_transformed_df.show()

# date_transformed_df.select("start_date_range", "end_date_range")\
#                    .withColumn('date_seq', expr('sequence(start_date_range, end_date_range, interval 1 day)'))\
#                    .select(explode("date_seq").alias("date_seq"))\
#                    .show()


date_transformed_df.select("start_date_range", "end_date_range")\
                   .withColumn('date_seq', explode(expr("""sequence(start_date_range, end_date_range,interval 1 week) """)))\
                   .select("date_seq")\
                   .show()


ps
# spark.sparkContext.parallelize([(start_date_range, end_date_range)]). \
#     toDF(['start', 'end']). \
#     withColumn('start', func.to_date('start')). \
#     withColumn('end', func.to_date('end')). \
#     withColumn('date_seq', func.expr('sequence(start, end, interval 1 day)')). \
#     select(func.explode('date_seq').alias('date')). \
#     show(10)
#

 # select(explode(sequence(to_date("start_date"), to_date("end_date"))).alias("date")).show())
# date_range_df = spark.range(1).select(explode(sequence(to_date(start_date_range),
#                                                        to_date(end_date_range))).alias("date"))
# # yyyy-MM-dd HH:mm:ss'
# date_range_df.show(truncate=False)
