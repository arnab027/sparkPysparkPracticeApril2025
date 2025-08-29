import pandas as pd
import numpy as np
df =pd.read_excel("D:\\pysparkProject\\pysparkPracticeApril2025\\pandas_practice\\read_file.xlsx",sheet_name="Sheet1", engine="openpyxl")
# print(df.head())
# {values[1]}
for i,values in df.iterrows():
    print(values['Col1'])