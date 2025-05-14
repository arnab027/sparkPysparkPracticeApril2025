import pandas as pd
import requests
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_rows", 1000)
meteorites = pd.read_csv('D:\\pysparkProject\\pysparkPracticeApril2025\\pythonPractice\\Meteorite_Landings.csv', engine="pyarrow",header=0)
print(meteorites.head())

#requests

api_url = "https://api.redtube.com/?data=redtube.Videos.searchVideos&output=json&search=hard&tags[]=Teen&thumbsize=medium"
headers = {"Content-Type": "application/json; charset=utf-8"}
response = requests.request("GET", api_url, headers=headers)
print(response.json())

redTubeDf = pd.DataFrame(response.json())
print(redTubeDf.head())
print(redTubeDf.columns)
redTubeDf1 = pd.DataFrame(response.json(),columns=["videos"])
print(redTubeDf1.head())
"""meteorites"""

print(meteorites.info())

"""select rows 100-104 and columns- name and id"""
df2 = meteorites.iloc[100:114,[0,1]]
print(df2)

"""filter meteorites df"""
filter_meteorites_df =meteorites[(meteorites['mass (g)'] > 1e6) & (meteorites.fall== "Fell")]
print(filter_meteorites_df)

"""filter using query method"""
filter_meteorites_query_df =meteorites.query("`mass (g)` > 1e6 and fall == 'Fell'")
print(filter_meteorites_query_df)

"""column level values"""
print(meteorites.fall.value_counts())
print(meteorites.year.value_counts())
"""normalize=True shows %age of values, e.g yes is of 0.60, No is 0.4 , totals upto 100% """
print(meteorites.value_counts(subset=['fall','year'],normalize=True))
print(meteorites.value_counts(subset=['fall'],normalize=True))
