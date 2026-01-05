import pandas as pd
import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import DoubleType
import time
from functools import wraps

spark = SparkSession.builder.appName("UDFComparison").getOrCreate()

# Create test data with multiple numeric columns
data = [(float(i), float(i*2), float(i*3)) for i in range(100000)]
df = spark.createDataFrame(data, ["val1", "val2", "val3"])
#Create a timing decorator to measure the execution time of the functions:


# Timing decorator
def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"{func.__name__}: {elapsed:.2f}s")
        wrapper.elapsed_time = elapsed
        return result

    return wrapper

#Use the timing decorator to measure the execution time of the pandas_udf function:
@pandas_udf(DoubleType())
def weighted_sum_pandas(v1: pd.Series, v2: pd.Series, v3: pd.Series) -> pd.Series:
    return v1 * 0.5 + v2 * 0.3 + v3 * 0.2

@timer
def run_pandas_udf():
    result = df.select(
        weighted_sum_pandas(df.val1, df.val2, df.val3).alias("weighted")
    )
    result.count()  # Trigger computation
    return result

result_pandas = run_pandas_udf()
pandas_time = run_pandas_udf.elapsed_time
print(f"{pandas_time}")

#Use the timing decorator to measure the execution time of the Arrow-optimized UDF using useArrow:

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(DoubleType(), useArrow=True)
def weighted_sum_arrow(v1, v2, v3):
    term1 = pc.multiply(v1, 0.5)
    term2 = pc.multiply(v2, 0.3)
    term3 = pc.multiply(v3, 0.2)
    return pc.add(pc.add(term1, term2), term3)

@timer
def run_arrow_udf():
    result = df.select(
        weighted_sum_arrow(df.val1, df.val2, df.val3).alias("weighted")
    )
    result.count()  # Trigger computation
    return result

result_arrow = run_arrow_udf()
arrow_time = run_arrow_udf.elapsed_time
print(f"arrow_time:{arrow_time}")
speedup = pandas_time / arrow_time
print(f"Speedup: {speedup:.2f}x faster")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Visualization").getOrCreate()

# Create sample sales data
sales_data = [
    ("Electronics", 5000, 1200),
    ("Electronics", 7000, 1800),
    ("Clothing", 3000, 800),
    ("Clothing", 4500, 1100),
    ("Furniture", 6000, 1500),
    ("Furniture", 8000, 2000),
]

sales_df = spark.createDataFrame(sales_data, ["category", "sales", "profit"])
print(sales_df.show())
# Direct plotting without conversion
sales_df.plot(kind="scatter", x="sales", y="profit", color="category")

#Let’s extract data from a 3-level nested JSON structure using the traditional approach:
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("json_schema").getOrCreate()

# 3 levels of nested StructType - verbose and hard to maintain
schema = StructType([
    StructField("user", StructType([
        StructField("name", StringType()),
        StructField("profile", StructType([
            StructField("settings", StructType([
                StructField("theme", StringType())
            ]))
        ]))
    ]))
])

json_data = [
    '{"user": {"name": "Alice", "profile": {"settings": {"theme": "dark"}}}}',
    '{"user": {"name": "Bob", "profile": {"settings": {"theme": "light"}}}}'
]

rdd = spark.sparkContext.parallelize(json_data)
json_df = spark.read.schema(schema).json(rdd)
print(json_df.select("user.name", "user.profile.settings.theme").show())

#
# PySpark 4.0 introduces the Variant type, which lets you skip schema definitions entirely. To work with the Variant type:
#
# Use parse_json() to load JSON data
# Use variant_get() to extract fields with JSONPath syntax

from pyspark.sql import SparkSession
from pyspark.sql.functions import parse_json, variant_get

spark = SparkSession.builder.appName("json_variant").getOrCreate()

json_data = [
    ('{"user": {"name": "Alice", "profile": {"settings": {"theme": "dark"}}}}',),
    ('{"user": {"name": "Bob", "profile": {"settings": {"theme": "light"}}}}',)
]

json_data_df = spark.createDataFrame(json_data, ["json_str"])
df_variant = json_data_df.select(parse_json("json_str").alias("data"))

# No schema needed - just use JSONPath
result = df_variant.select(
    variant_get("data", "$.user.name", "string").alias("name"),
    variant_get("data", "$.user.profile.settings.theme", "string").alias("theme")
)
print(result.show())


# Dynamic Schema Generation with UDTF analyze() (PySpark 4.0+)
# Python UDTFs (User-Defined Table Functions) generate multiple rows from a single input row, but they come with a critical limitation: you must define the output schema upfront. When your output columns depend on the input data itself (like creating pivot tables or dynamic aggregations where column names come from data values), this rigid schema requirement becomes a problem.
#
# For example, a word-counting UDTF requires you to specify all output columns upfront, even though the words themselves are unknown until runtime.

from pyspark.sql.functions import udtf, lit
from pyspark.sql.types import StructType, StructField, IntegerType

# Schema must be defined upfront with fixed column names
@udtf(returnType=StructType([
    StructField("hello", IntegerType()),
    StructField("world", IntegerType()),
    StructField("spark", IntegerType())
]))
class StaticWordCountUDTF:
    def eval(self, text: str):
        words = text.split(" ")
        yield tuple(words.count(word) for word in ["hello", "world", "spark"])

# Only works for exactly these three words
result = StaticWordCountUDTF(lit("hello world hello spark"))
print(result.show())
# PySpark 4.0 introduces the analyze() method for UDTFs, enabling dynamic schema determination based on input data.
# Instead of hardcoding your output schema, analyze() inspects the input and generates the appropriate columns at
# runtime.

from pyspark.sql.functions import udtf, lit
from pyspark.sql.types import StructType, IntegerType
from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult

@udtf
class DynamicWordCountUDTF:
    @staticmethod
    def analyze(text: AnalyzeArgument) -> AnalyzeResult:
        """Dynamically create schema based on input text"""
        schema = StructType()
        # Create one column per unique word in the input
        for word in sorted(set(text.value.split(" "))):
            schema = schema.add(word, IntegerType())
        return AnalyzeResult(schema=schema)

    def eval(self, text: str):
        """Generate counts for each word"""
        words = text.split(" ")
        # Use same logic as analyze() to determine column order
        unique_words = sorted(set(words))
        yield tuple(words.count(word) for word in unique_words)

# Schema adapts to any input text
result = DynamicWordCountUDTF(lit("hello world hello spark"))
result.show()

# Now try with completely different words:
# Different words - schema adapts automatically
result2 = DynamicWordCountUDTF(lit("python data science"))
result2.show()