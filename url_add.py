import psycopg2
from time import sleep
import pika
import json
import random
import settings
import sys

#Postgresql
psql_name = settings.psql_name
psql_user = settings.psql_user
psql_host = settings.psql_host
psql_port = settings.psql_port
psql_password = settings.psql_password

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " url")
    sys.exit(1)

try:
    conn = psycopg2.connect("dbname=" + psql_name +
                            " user=" + psql_user +
                            " host=" + psql_host +
                            " port=" + psql_port +
                            " password=" + psql_password)
except Exception as e:
    print(e)
    print("PostgreSQL Error")   #TODO change by pgpool
    sys.exit(1)

cur = conn.cursor()


url = sys.argv[1]

print("Inserting " + url)
cur.execute("INSERT INTO tabla2 VALUES (DEFAULT , %s)", [url])
conn.commit()
cur.close()
conn.close()
