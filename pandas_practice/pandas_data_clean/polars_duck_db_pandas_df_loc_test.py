import pandas as pd
import polars as ps
import duckdb as db
from io import BytesIO

print("==================pandas df ====================================")
pandas_df = pd.read_csv("imdbratings.csv",header=0)
print(pandas_df.head(3))

print("==================polars df ====================================")
polars_df = ps.read_csv("imdbratings.csv", has_header=True)
polars_df = polars_df.lazy()
polars_df1 = ps.scan_csv("imdbratings.csv",has_header=True)
print(polars_df.head(3))
print(polars_df1.select("title","actors_list").show(3))
print("==================duck db df ====================================")
duck_db_df = db.read_csv("imdbratings.csv",header=True)
print(duck_db_df.show())

## pandas df filter movies with star rating greater 9
print("================================pandas filter ===============================")
pandas_filter_start_rating_df = pandas_df['star_rating'] > 9.0
print(pandas_df.loc[pandas_filter_start_rating_df,["title","actors_list"]].head(100))

print("================================polars filter ===============================")
## polars filter dataframe movies with star rating greater 8 and genre not crime
polars_filter_df = polars_df.filter(ps.col("star_rating") > 8.0 , ps.col("genre") != "Crime")
print(polars_filter_df.show(3))
