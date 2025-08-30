# python
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
from typing import Iterator
import numpy as np
import json

spark = SparkSession.builder.appName("vectorized-udf-examples").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# sample DF
df = spark.createDataFrame(
    [(1, "apple", 2.0), (2, "banana", 3.0), (3, None, 4.0), (1, "pear", 5.0)],
    ["id", "fruit", "val"]
)

# 1) Scalar vectorized UDF (series -> series)
@pandas_udf(T.StringType())
def add_prefix(s: pd.Series) -> pd.Series:
    return "p_" + s.fillna("")

df.withColumn("prefixed", add_prefix(F.col("fruit"))).show()

# 2) Numeric vectorized UDF (fast elementwise numeric ops)
@pandas_udf(T.DoubleType())
def square(s: pd.Series) -> pd.Series:
    return s * s

df.withColumn("val_sq", square(F.col("val"))).show()

# 3) Grouped aggregation (GROUPED_AGG): compute group mean
######################
df = spark.createDataFrame(
    [
        (1, '{"a": 10, "b": "x"}', 2.0, "g1"),
        (2, '{"a": 20, "b": "y"}', 3.5, "g1"),
        (3, None, 5.0, "g2"),
        (4, '{"a": 5, "b": "z"}', 1.2, "g2")
    ],
    ["id", "payload", "val", "grp"]
)

# 1) Scalar vectorized UDF returning a StructType (parse JSON payload)
schema_struct = T.StructType([
    T.StructField("a", T.IntegerType()),
    T.StructField("b", T.StringType())
])

@pandas_udf(schema_struct)
def parse_payload(s: pd.Series) -> pd.DataFrame:
    parsed = s.fillna("{}").apply(json.loads)
    return pd.DataFrame({
        "a": [p.get("a") for p in parsed],
        "b": [p.get("b") for p in parsed]
    })

df.select("id", "payload").withColumn("parsed", parse_payload(F.col("payload"))).select("id", "parsed.*").show()


# # 2) mapInPandas example: enrich batches by joining with an external Pandas lookup table
# lookup_df = pd.DataFrame({"id": [1, 2, 3, 4], "meta": ["one", "two", "three", "four"]})
#
# def enrich_batches(iterator):
#     for pdf in iterator:
#         out = pdf.merge(lookup_df, on="id", how="left")
#         yield out
#
# out_schema = T.StructType(df.schema.fields + [T.StructField("meta", T.StringType())])
# df.mapInPandas(enrich_batches, schema=out_schema).show()
#
# # 3) GROUPED_MAP example: keep top-1 row per group by 'val'
# group_schema = T.StructType(df.schema.fields)  # keep same columns in output
#
# @pandas_udf(group_schema, functionType=PandasUDFType.GROUPED_MAP)
# def top1_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
#     return pdf.nlargest(1, "val")
#
# df.groupby("grp").apply(top1_per_group).show()

# 4) NumPy-vectorized scalar UDF for fast numeric transforms
@pandas_udf(T.DoubleType())
def fast_log_scale(s: pd.Series) -> pd.Series:
    arr = s.to_numpy(dtype=np.float64)
    return pd.Series(np.log1p(arr) * 2.0)

df.withColumn("fast_transformed", fast_log_scale(F.col("val"))).show()