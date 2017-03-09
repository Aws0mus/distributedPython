import pika
import psycopg2
import settings
import sys

#Postgresql
psql_name = settings.psql_name
psql_user = settings.psql_user
psql_host = settings.psql_host
psql_port = settings.pg_pool_port
psql_password = settings.psql_password

#RabbitMQ
rabbit_host = settings.rabbit_host

#Queues
postgresQueue = settings.postgresQueue
elasticQueue = settings.elasticQueue


class Broker:

    def __init__(self):
        self.connection = self.check_rabbit()
        if self.connection is None:
            print("RabbitMQ Error")
            return

        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=postgresQueue, durable=True)
        self.channel.queue_declare(queue=elasticQueue, durable=True)

        self.conn = self.check_pool()
        if self.conn is None:
            print("Pgpool Error")
            return

    def check_rabbit(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbit_host))
        except Exception:
            connection = None
        return connection

    def main(self):

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

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(callback_postgresql,
                              queue=postgresQueue)
        self.channel.basic_consume(callback_elastic,
                              queue=elasticQueue)

        self.channel.start_consuming()

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

    def save_postgres(self, links):
        try:
            cur = self.conn.cursor()
            for link in links:
                cur.execute("INSERT INTO tabla2 VALUES (DEFAULT , %s)", [link])
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

        cur.close()

    def cleanup(self):
        self.connection.close()
        self.conn.close()
        sys.exit(1)

    def execute(self):
        try:
            self.main()
        except KeyboardInterrupt:
            self.cleanup()


if __name__ == '__main__':
    Broker().execute()