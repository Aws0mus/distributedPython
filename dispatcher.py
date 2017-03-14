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

#RabbitMQ
rabbit_host = settings.rabbit_host

#Queues
targetsQueue = settings.targetsQueue

class Dispatcher:

    def __init__(self):
        self.rabbit_conn = self.check_rabbit()
        if self.rabbit_conn is None:
            print("RabbitMQ Error")
            return

        self.channel = self.rabbit_conn.channel()
        args = {"x-max-priority": 10}
        self.channel.queue_declare(queue=targetsQueue, durable=True, arguments=args)

        self.conn = self.check_pool()
        if self.conn is None:
            print("PostgreSQL Error")
            return
        self.cur = self.conn.cursor()

    def check_pool(self):
        try:
            conn = psycopg2.connect("dbname=" + psql_name +
                                " user=" + psql_user +
                                " host=" + psql_host +
                                " port=" + psql_port +
                                " password=" + psql_password)
        except Exception as e:
            print(e)
            conn = None
        return conn

    def check_rabbit(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbit_host))
        except Exception:
            connection = None
        return connection

    def main(self):

        initial = 1
        step = 100

        while True:
            last = initial + step
            sleep(2)
            try:
                print("Query to postgreSQL")

                self.cur.execute("SELECT * FROM tabla2 WHERE id >= %s AND id < %s", [initial, last])
                rows = self.cur.fetchall()
                sleep(6)
                initial = last

                for row in rows:
                    print("Sending url to spider: " + row[1])
                    data = {'url': row[1], 'from': row[1]}
                    self.channel.basic_publish(exchange='',
                                          routing_key=targetsQueue,
                                          body=json.dumps(data),
                                          properties=pika.BasicProperties(
                                              delivery_mode=2,  # make message persistent
                                              priority=random.randint(1, 10),
                                          ))

                self.cur.execute("SELECT last_value FROM tabla2_id_seq")
                last_value = self.cur.fetchone()[0]
                if last > last_value:
                    initial = 0

            except Exception as e:
                print(e)
                self.conn.rollback()

    def cleanup(self):
        self.rabbit_conn.close()
        self.cur.close()
        self.conn.close()
        sys.exit(1)

    def execute(self):
        try:
            self.main()
        except KeyboardInterrupt:
            self.cleanup()


if __name__ == '__main__':
    Dispatcher().execute()
