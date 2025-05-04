import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)\
    .config("spark.jars", "D:\\pysparkProject\\pysparkPracticeApril2025\\mysql-connector-j-9.3.0.jar")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data =['103', 'tester_1']
df = spark.createDataFrame([('Alice', 1)], "name: string, age: int")
# df = spark.sparkContext.parallelize(data).toDF(['id', 'name'])

df.write\
    .format("jdbc") \
    .mode("overwrite")\
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/PySparkTables") \
    .option("dbtable", "DestinationTable1") \
    .option("user", "root") \
    .option("password", "root") \
    .save()