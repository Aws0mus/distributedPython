from time import sleep
import random
import pika

#TODO put parameters in other file

#RabbitMQ
rabbit_host = 'localhost'

class Spider:

    def __init__(self):
        rabbit_conn = self.check_rabbit()
        if rabbit_conn is None:
            return

        channel = rabbit_conn.channel()
        channel.queue_declare(queue='postgresqlQueue', durable=True)
        channel.queue_declare(queue='elasticQueue', durable=True)

    def check_rabbit(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbit_host))
        except Exception:
            connection = None
        return connection

    def visit(self):
        soup = "A"*100
        link = "B"*10
        links = []
        for i in range(0, 100):
            links.append(link+str(i))
        print("Duermo")
        sleep(random.randint(5, 8))
        print("Despierto e inserto")
        self.save_rabbit(links, soup)

    def save_rabbit(self, postgresql, elastic):
        rabbit_conn = self.check_rabbit()
        if rabbit_conn is None:
            return
        channel = rabbit_conn.channel()

        channel.basic_publish(exchange='',
                              routing_key='postgresqlQueue',
                              body=', '.join(postgresql),
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        channel.basic_publish(exchange='',
                              routing_key='elasticQueue',
                              body=elastic,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))

        print(" [x] Sent %r" % postgresql)
        rabbit_conn.close()

spider = Spider()
while True:
    spider.visit()
