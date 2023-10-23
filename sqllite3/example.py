import sqlite3
import pandas as pd

conn = sqlite3.connect('Departmets.db')
table_name = 'Departments'
attribute_list = ['DEPT_ID','DEPT_NAME','MANAGER_ID','LOC_ID']

file_path = 'C:/Users/User/Documents/python for dataen/sqllite3/Departments.csv'
df=pd.read_csv(file_path,names=attribute_list)
# Loading data to table
# using pandas(df; object) through to_sql() method to load directly to DB
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

data_dict={'DEPT_ID':[9],
           'DEPT_NAME':['Quality Assurance'],
           'MANAGER_ID':['30010'],
           'LOC_ID':['L0010']}

# Running basic quires (get statement)
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT DEPT_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT count(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# close the databse connection
conn.close()