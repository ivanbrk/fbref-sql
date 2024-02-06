import psycopg2
import time
from getpass import getpass

password = getpass("Enter password:")
st1 = time.time()

conn = psycopg2.connect(
	dbname="fbref",
	user="postgres",
	password = password,
	host = "localhost",
    port='5432',
    connect_timeout=10)

st2 = time.time()

print(st2-st1)

cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS competitions (
    competition_id INTEGER PRIMARY KEY NOT NULL,
    competition_name VARCHAR(30) NOT NULL UNIQUE,
    competition_country VARCHAR(3) NOT NULL,
    competition_level INTEGER NOT NULL)''')

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
    (18,'Seria B', 'ITA', 2)
]

cursor.executemany('''INSERT INTO competitions (
    competition_id, 
    competition_name, 
    competition_country, 
    competition_level)
    VALUES (%s, %s, %s, %s)''', 
    competitions_data)

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()