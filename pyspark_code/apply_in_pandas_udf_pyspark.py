import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator
import numpy as np
import pandas as pd

# Initialize Spark session with Delta Lake support
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

test_data = [(1,"Arnab","Dutta",2000,"Dev"),
             (1,"Arnab","Dutta",3000,"Sr. Dev"),
             (2,"James","Bond",1000,"HR"),
             (2,"Ryan","Gosling",3000,"Sr. HR"),
             (3,"Peter","Parker",1500,"Analyst"),
             (3,"Peter","Parker",2500,"Sr. Analyst")]


data_schema = StructType([StructField("id", IntegerType(), True),
                          StructField("FirstName", StringType(), True),
                          StructField("LastName", StringType(), True),
                          StructField("Salary", IntegerType(), True),
                          StructField("Designation", StringType(), True)])
df = spark.createDataFrame(test_data,schema=data_schema)
df.show()
print(df.printSchema())
salary_list_df = df.groupBy("id").agg(collect_list("Salary").alias("SalaryList"))
salary_list_df.show()
def test_apply_in_pandas_udf_pyspark(key,value_passed: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"id": [value_passed["id"].iloc[0]],
                         "FirstName":[(value_passed["FirstName"].iloc[0])],
                         "LastName": [(value_passed["LastName"].iloc[0])],
                         "Max_Salary": [(value_passed["Salary"].max())],
                         "Min_Salary": [(value_passed["Salary"].min())],
                         "Sum_Salary": [(value_passed["Salary"].sum())]
                         })


outputSchema = StructType([StructField("id", IntegerType(), True),
                           StructField("FirstName", StringType(), True),
                           StructField("LastName", StringType(), True),
                           StructField("Max_Salary", IntegerType(), True),
                           StructField("Min_Salary", IntegerType(), True),
                           StructField("Sum_Salary", IntegerType(), True)
                          ])

pandas_udf_df = df.groupBy("id").applyInPandas(test_apply_in_pandas_udf_pyspark,schema = outputSchema)

pandas_udf_df.join(salary_list_df,["id"],how="inner").show()
