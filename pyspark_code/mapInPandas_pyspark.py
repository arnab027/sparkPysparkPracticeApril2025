import requests
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
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


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

    ## Fetch data from API
    response = requests.get("https://api.restful-api.dev/objects",timeout=10,verify=False)
    data = response.json()
    status_code = response.status_code
    print(status_code)
    list_of_values_to_append = []
    if status_code == 200:
        # print(data)
        for values in data:
            print(values)
            list_of_values_to_append.append(values)
    else:
        print("Failed to fetch data")

    if list_of_values_to_append:
        print(list_of_values_to_append)

        schema_value =StructType([
            StructField("id",StringType(),True),
            StructField("name",StringType(),True),
            StructField("data",StructType([
                StructField("color",StringType(),True),
                StructField("capacity", StringType(), True),
                StructField("price", StringType(), True),
                StructField("generation", StringType(), True),
                StructField("year", StringType(), True),
                StructField("CPU model", StringType(), True),
                StructField("Hard disk size", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("Capacity", StringType(), True),
                StructField("Screen size", StringType(), True)]
            ),True),
        ])
        df = spark.createDataFrame(list_of_values_to_append,schema=schema_value)
        print(df.show(truncate=False))


        def add_values_in_api_call(iterator):
            headers = {"content-type": "application/json"}
            value1= ""
            for pdf in iterator:
                for _, row in pdf.iterrows():
                    id_value = row["id"]
                    name_value= row["name"]

                    # Handle null data field
                    data_row = row["data"]

                    if pd.isna(data_row):
                        data_value = {
                            "color": value1,
                            "capacity": value1,
                            "price": 200,
                            "generation": value1,
                            "year": value1,
                            "CPU model": value1,
                            "Hard disk size": value1,
                            "Description": value1,
                            "Capacity": value1,
                            "Screen size": value1
                        }
                    else:
                        # data_row is a dictionary, access its keys safely
                        data_value = {
                            "color": str(data_row.get("color", value1)),
                            "capacity": str(data_row.get("capacity", value1)),
                            "price": 200,
                            "generation": str(data_row.get("generation", value1)),
                            "year": str(data_row.get("year", value1)),
                            "CPU model": str(data_row.get("CPU model", value1)),
                            "Hard disk size": str(data_row.get("Hard disk size", value1)),
                            "Description": str(data_row.get("Description", value1)),
                            "Capacity": str(data_row.get("Capacity", value1)),
                            "Screen size": str(data_row.get("Screen size", value1))
                        }


                    payload = json.dumps(
                        {"id": id_value, "name":name_value ,"data":data_value}
                    )

                    request_url = "https://api.restful-api.dev/objects"
                    response_value = requests.post(request_url, data=payload, headers=headers)
                    response_json_str  = json.dumps(response_value.json())

                    result_df = pd.DataFrame([{
                        "id": id_value,
                        "name": name_value,
                        "response_json": str(response_json_str)
                    }])
                    yield result_df


        new_schema_value = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("response_json", StringType(), True),
        ])

        #=======applying mapInPandas===========
        print(df.mapInPandas(add_values_in_api_call,new_schema_value).show(truncate=False))

    # if list_of_values_to_append:
    #     df = spark.createDataFrame(list_of_values_to_append)
    #     print(df.show())
    # df = spark.createDataFrame(data)
    # print(df.show())

except Exception as e:
    logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)