import psycopg2
import time
from getpass import getpass
import sys


class database():

    def __init__(self, dbname, user, host, port, connection_timeout):

        self.dbname = dbname
        self.user = user
        self.host = host
        self.port = port 
        self.ct = connection_timeout

        password = getpass("Enter password:")

        self.conn = psycopg2.connect(
           dbname= self.dbname,
           user= self.user,
           password = password,
           host = self.host,
           port=self.port,
           connect_timeout=self.ct)


        self.cursor = self.conn.cursor()

    def create_table(self, table_name, table_info):

        self.table_name = table_name
        self.table_info = table_info

        self.query = []

        for i in range(0,len(self.table_info)):
            self.query.append(" ".join(self.table_info[i]))

        self.query = ",".join(self.query)
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.table_name} ({self.query})''')

    def insert_data(self, table_name, columns_order, data):

        self.table_name = table_name
        self.columns_order = ",".join(columns_order)
        self.data = data
        self.values = ",".join(['%s' for i in columns_order])

        try:
            self.cursor.executemany(f'''INSERT INTO {self.table_name} (
            {self.columns_order})
            VALUES ({self.values})''', 
            self.data)

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while inserting data into PostgreSQL table:", error)

    def commit_changes(self):

        self.conn.commit()

    def close_database(self):

        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':

    competition_table_info = [
    ['competition_id', 'INTEGER', 'PRIMARY KEY'],
    ['competition_name', 'VARCHAR(200)', 'UNIQUE', 'NOT NULL'],
    ['competition_country', 'VARCHAR(3)', 'NOT NULL'],
    ['competition_level', 'INTEGER', 'NOT NULL']]

    competitions_data = [
    (9,'Premier League', 'ENG', 1),
    (12,'La Liga', 'ESP', 1),
    (13,'Ligue 1', 'FRA', 1),
    (20,'Bundesliga', 'GER', 1),
    (11,'Seria A', 'ITA', 1),
    (32,'Primeira Liga', 'POR', 1),
    (23,'Eredivisie', 'NED', 1),
    (10,'Championship', 'ENG', 2),
    (17,'Segunda Division', 'ESP', 2),
    (33,'2 Bundesliga', 'GER', 2),
    (18,'Seria B', 'ITA', 2),
    (8,'Champions League', 'EU', 1000),
    (19,'Europa League', 'EU', 1001),
    (882,'Europa Conference League', 'EU', 1002)]

    fbref = database(
        dbname = 'fbref', 
        user = 'postgres', 
        port = 5432, 
        host = 'localhost', 
        connection_timeout = 10)

    fbref.create_table(
        table_name='competitions', 
        table_info = competition_table_info)

    fbref.commit_changes()

    fbref.insert_data(
        table_name = 'competitions',
        columns_order = ['competition_id', 
                         'competition_name', 
                         'competition_country', 
                         'competition_level'],
        data = competitions_data)

    fbref.commit_changes()
    fbref.close_database()



