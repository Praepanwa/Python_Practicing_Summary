import sqlite3
import pandas as pd

# data source
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv

# Connect to the SQLite3 service
conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
# define the table parameters
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
# Reading CSV file
file_path = 'C:/Users/User/Documents/python for dataen/sqllite3/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Loading csv data to table
# using pandas(df; object) through to_sql() method to load directly to DB
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
# parameter table_name, conn = db connection
# if_exists has 3 options : 'fail' = doesn't work if table using the same name exists in DB
#                           'replace' = replace existing table with the same name
#                           'append' = appends new data to the existing table with the same name

print('Table is ready')

# Running basic quires (get statement)
# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# trying to create new data and input to the same table
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# close the databse connection
conn.close()