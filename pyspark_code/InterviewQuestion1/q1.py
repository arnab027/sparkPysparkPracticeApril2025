import pyspark
from delta import *
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions  import lag,lead,datediff
from pyspark.sql.functions  import sum as fsum

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

"""
Scenario:
You’re working as a Data Engineer for a power distribution company in India. 
Customers are billed daily, but due to technical issues, some records are missing in the billing_logs table. 
Management wants to find out the missing billing dates for each customer.

Identify continuous date gaps in billing logs for each customer between their first and last billing date.

data = [
 ("C001", "2024-01-01"),
 ("C001", "2024-01-02"),
 ("C001", "2024-01-04"),
 ("C001", "2024-01-06"),
 ("C002", "2024-01-03"),
 ("C002", "2024-01-05"),
]

df = spark.createDataFrame(data, ["customer_id", "billing_date"])


Output : 
+------------+------------+------------+
|customer_id |missing_from|missing_to |
+------------+------------+------------+
|C001    |2024-01-03 |2024-01-03 |
|C001    |2024-01-05 |2024-01-05 |
|C002    |2024-01-04 |2024-01-04 |
+------------+------------+------------



"""
spark = configure_spark_with_delta_pip(builder).getOrCreate()

dataset = [("C001",1200, "2021-01-01"),
           ("C001",1205, "2021-02-01"),
           ("C001",1215, "2021-05-01"),
           ("C001",1225, "2021-07-01"),
           ("C002",1100, "2021-01-01"),
           ("C002",1110, "2021-02-01"),
           ("C002",1001, "2021-04-01"),
           ("C002",1101, "2021-06-01")]

df = spark.createDataFrame(dataset,schema= "Cust_Id: string, Amount: int, Bill_Date: string")
# df.show()
# print(df.printSchema())
df1= df.withColumn("Bill_Date",col("Bill_Date").cast("date"))\
       .withColumn("Bill_date_v1",to_date("Bill_Date","yyyy-MM-dd"))
# df1.show()
# print(df1.printSchema())
group_df = df1.groupBy(["Cust_Id"]).agg(min("Bill_Date").alias("start_date"),
                                        max("Bill_Date").alias("end_date"))\
             .withColumn("date",explode(sequence("start_date","end_date")))\
             .drop("start_date","end_date")

group_df.show(truncate=False)

missing_df =group_df.join(df1,( (group_df["Cust_Id"] == df1["Cust_Id"] )  & (group_df["date"] == df1["Bill_Date"] )), how= "left_anti")\
                    .withColumnRenamed("date","missing_date")\
                    .orderBy(col("missing_date").desc())

missing_df.show(1000,truncate=False)

w= Window.partitionBy("Cust_Id").orderBy("missing_date")
missing_range_df = missing_df.withColumn("prev_date",lag("missing_date").over(w))
missing_range_1_df = missing_range_df.withColumn("grp", when(col("prev_date").isNull(),1)\
                                                              .when(datediff(col("missing_date"), col("prev_date")) >1,1).otherwise(0))

missing_range_1_df.show(1000,truncate=False)

missing_range_2_df = missing_range_1_df.withColumn("grp_id",fsum("grp").over(w.rowsBetween(Window.unboundedPreceding,0)))

result = missing_range_2_df.groupBy("Cust_Id","grp_id").agg(min("missing_date").alias("missing_from"),
                                                            max("missing_date").alias("missing_to"))\
                            .select("Cust_Id","missing_from","missing_to")

result.show(truncate=False)