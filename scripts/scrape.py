import requests
import pandas as pd 
import sys
import bs4 
from database import database


fbref = database(
        dbname = 'fbref', 
        user = 'postgres', 
        port = 5432, 
        host = 'localhost', 
        connection_timeout = 10)

sql_query = "SELECT * FROM competitions"

# Load data directly into a Pandas DataFrame
df = pd.read_sql(sql_query, fbref.conn)

# Print the first few rows of the DataFrame
print(df.head())