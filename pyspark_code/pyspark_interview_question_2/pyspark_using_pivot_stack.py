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
pivot_sales = company_df.groupBy("Region").pivot("Year").sum("Sales")
for c in [c for c in pivot_sales.columns if c != "Region"]:
    pivot_sales = pivot_sales.withColumnRenamed(c, f"Sales_{c}")

# pivot Profit by Year, rename numeric year columns to Profit_<year>
pivot_profit = company_df.groupBy("Region").pivot("Year").sum("Profit")
for c in [c for c in pivot_profit.columns if c != "Region"]:
    pivot_profit = pivot_profit.withColumnRenamed(c, f"Profit_{c}")

# join on Region and replace nulls with 0 if desired
result = pivot_sales.join(pivot_profit, "Region").na.fill(0)
result.show()

# """ Output
# +------+----------+----------+-----------+-----------+
# |Region|Sales_2024|Sales_2025|Profit_2024|Profit_2025|
# +------+----------+----------+-----------+-----------+
# |  West|       900|      1050|        260|        500|
# |  East|      1000|      1200|        200|        300|
# +------+----------+----------+-----------+-----------+
#
# """