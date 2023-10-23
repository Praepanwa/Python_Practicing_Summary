# import all the required libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# define the url, database name, table name and csv path
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
# relative path from the current directory
csv_path = 'top_50_films.csv'
# define the dataframe with the required columns
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
# index for table rows usage (for using 50 rows only)
count = 0

# get the html page and parse it to beautifulsoup
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# find the html table elements (then it returns as a collection AKA. arrays) and rows
tables = data.find_all('tbody')
# pick up the first table in the html page
rows = tables[0].find_all('tr')
# loop through the rows and get the data from the columns
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            # create dataframe from the dictionary
            df1 = pd.DataFrame(data_dict, index=[0])
            # append the dataframe to the main dataframe
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break

print(df)
# save the dataframe as csv file
df.to_csv(csv_path)

# save the dataframe to the database
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()