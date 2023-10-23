import requests
import sqlite3
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=['Name','MC_USD_Billion']

table_attribs_final=['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
output_csv ='./Largest_banks_data.csv'
db_name='Banks.db'
table_name='Largest_banks'
Log_file='./code_log.txt'
csv_path='./exchange_rate.csv'
# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    log_file = Log_file

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

# log_progress('hello')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    # print(tables[0])
    # print(rows)
    for row in rows:
        col = row.find_all('td')
        if len(col) !=0 :
            if col[0].find('a') is not None :
                data_dict = {"Name": col[0].a.contents[0],
                            "MC_USD_Billion": (col[1].contents[0]).strip('\n')}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    # print(df)

    return df

# extract(url,table_attribs)


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    print(dict)
    USD_val = df['MC_USD_Billion'].tolist()
    GBP_val_list = []
    EUR_val_list = []
    INR_val_list = []
    for i in range(len(USD_val)):
        USD_val[i] = float(USD_val[i].strip('$').replace(',',''))
        for j in dict:
            if j == 'GBP':
               GBP_val = round((USD_val[i]*dict[j]),2)
               GBP_val_list.append(GBP_val)
            elif j=='EUR':
                EUR_val = round(USD_val[i]*dict[j],2)
                EUR_val_list.append(EUR_val)
            elif j=='INR':
                INR_val = round(USD_val[i]*dict[j],2)
                INR_val_list.append(INR_val)
                
    df['MC_GBP_Billion'] = GBP_val_list
    df['MC_EUR_Billion'] = EUR_val_list
    df['MC_INR_Billion'] = INR_val_list

    print(df)
    return df

# transform(extract(url,table_attribs),csv_path)

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)
    return

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    return

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''

# transform(extract(url,table_attribs),csv_path)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} ORDER BY MC_EUR_Billion DESC LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()