import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pyspark
from delta import *
import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)\
    .config('spark.sql.ansi.enabled', "False")\
    .config('compute.fail_on_ansi_mode', "False")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
start_date_range = '2025-01-01'
end_date_range = '2025-04-30'
# print(ps.date_range(start_date_range,end_date_range))
# print(np.array(ps.date_range(start_date_range,end_date_range)))
array_numpy= np.array(pd.date_range(start_date_range,end_date_range,freq='D'))
for elem in array_numpy:
    print(f"elem- {elem}, {type(elem)}")
    # for items in elem:
    #     print(f"items - {items},{type(items)}")

pandas_df=pd.DataFrame(array_numpy,columns=['date'])
print(pandas_df.head(120))
#
#
date_df=spark.createDataFrame(pandas_df,schema=['date'])
date_df.show()