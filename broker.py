import pika
from time import sleep
import psycopg2


#Postgresql
psql_name = "distributed"
psql_user = "test"
psql_host = "localhost"
psql_port = "9999"
psql_password = "password"


class Broker:

    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='postgresqlQueue', durable=True)
        channel.queue_declare(queue='elasticQueue', durable=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')

        def callback_postgresql(ch, method, properties, body):
            links = body.split()
            print(" [x] Inserting %r" % links)

            self.save_postgres(links)

            print(" [x] Done")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def callback_elastic(ch, method, properties, body):
            print(" [x] Inserting %r" % body)
            print(" [x] Done")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback_postgresql,
                              queue='postgresqlQueue')
        channel.basic_consume(callback_elastic,
                              queue='elasticQueue')

        channel.start_consuming()

    def check_postgres(self):
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

    def save_postgres(self, links):
        conn = self.check_postgres()
        if conn is None:
            return
        try:
            cur = conn.cursor()
            for link in links:
                cur.execute("INSERT INTO tabla2 VALUES (DEFAULT , %s)", [link])
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()

        cur.close()
        conn.close()

Broker()