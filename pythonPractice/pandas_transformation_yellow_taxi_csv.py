import pandas as pd
import requests
import datetime

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_rows", 1000)

yellow_taxi_df = pd.read_csv("D:\\pysparkProject\\pysparkPracticeApril2025\\pythonPractice\\2019_Yellow_Taxi_Trip_Data.csv")
print(yellow_taxi_df.head())
print(yellow_taxi_df.columns)
print(yellow_taxi_df.shape)
print(yellow_taxi_df.describe())

"""Drop All Columns that has either id as a part of string or store_and_fwd_flag"""

drop_col = yellow_taxi_df.columns.str.contains("id$|store_and_fwd_flag",regex=True)
##getting the actual list of columns
yellow_taxi_df_drop_cols = yellow_taxi_df.columns[drop_col]
print(yellow_taxi_df_drop_cols)
#passing the list of columns to be dropped#
yellow_taxi_drp_col_transform_df = yellow_taxi_df.drop(columns = yellow_taxi_df_drop_cols)
print(yellow_taxi_drp_col_transform_df.head())
##renaming columns
yellow_taxi_rename_cols_transform_df = yellow_taxi_drp_col_transform_df.rename(columns={"passenger_count":"no_of_passengers",
                                                                                         "tip_amount":"tip",
                                                                                         "tolls_amount":"toll"})
print(f"Earlier columns= {yellow_taxi_drp_col_transform_df.columns} \n updated columns names = {yellow_taxi_rename_cols_transform_df.columns}")
print(yellow_taxi_rename_cols_transform_df.head())
print(yellow_taxi_rename_cols_transform_df.dtypes)
# 2019-10-23T16:39:42.00

yellow_taxi_rename_cols_transform_df[["tpep_pickup_datetime","tpep_dropoff_datetime"]] = yellow_taxi_rename_cols_transform_df[["tpep_pickup_datetime","tpep_dropoff_datetime"]].apply(pd.to_datetime)
print(yellow_taxi_rename_cols_transform_df.head())
print(yellow_taxi_rename_cols_transform_df.dtypes)