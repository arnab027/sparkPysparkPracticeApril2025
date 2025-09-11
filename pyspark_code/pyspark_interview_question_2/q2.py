import pyspark
from delta import *
from pyspark.sql.functions import col,concat_ws,expr
import os


# Set environment variables for better local execution
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'



builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", True) \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000") \
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
company_data = [("East",2024,1000,200),("East",2025,1200,300),("West",2024,900,260),("West",2025,1050,500)]
"""Input
+------+----+-----+------+
|Region|Year|Sales|Profit|
+------+----+-----+------+
|  East|2024| 1000|   200|
|  East|2025| 1200|   300|
|  West|2024|  900|   260|
|  West|2025| 1050|   500|
+------+----+-----+------+
"""


company_df = spark.createDataFrame(company_data, ["Region","Year","Sales","Profit"])
company_df.show()

# company_df2 = company_df.select("Region", "Year", expr("stack(2, 'Sales', Sales, 'Profit', Profit) as (Metric, Value)"))

company_df2 = company_df.selectExpr("Region", "Year", "stack(2, 'Sales', Sales, 'Profit', Profit) as (Metric,Value)")

company_df2.show()

company_df3= company_df2.withColumn("Metric_Year", concat_ws("_", "Metric", "Year"))
company_df3.show()

final_df = company_df3.groupBy("Region").pivot("Metric_Year").sum("Value")
final_df.show()

"""
+------+-----------+-----------+----------+----------+
|Region|Profit_2024|Profit_2025|Sales_2024|Sales_2025|
+------+-----------+-----------+----------+----------+
|  West|        260|        500|       900|      1050|
|  East|        200|        300|      1000|      1200|
+------+-----------+-----------+----------+----------+

"""