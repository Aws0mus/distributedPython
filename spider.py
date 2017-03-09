from time import sleep
import random
import pika
import sys
import json
import settings

#RabbitMQ
rabbit_host = settings.rabbit_host

#Queues
targetsQueue = settings.targetsQueue
postgresQueue = settings.postgresQueue
elasticQueue = settings.elasticQueue


class Spider:

    def __init__(self):
        self.rabbit_conn = self.check_rabbit()
        if self.rabbit_conn is None:
            return

        self.channel = self.rabbit_conn.channel()
        self.channel.queue_declare(queue=postgresQueue, durable=True)
        self.channel.queue_declare(queue=elasticQueue, durable=True)
        args = {"x-max-priority": 10}
        self.channel.queue_declare(queue=targetsQueue, durable=True, arguments=args)

    def check_rabbit(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbit_host))
        except Exception:
            connection = None
        return connection

    def visit(self, url):
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

        self.channel.basic_publish(exchange='',
                              routing_key=postgresQueue,
                              body=', '.join(postgresql),
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        self.channel.basic_publish(exchange='',
                              routing_key=elasticQueue,
                              body=elastic,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))

        print(" [x] Sent %r" % postgresql)

    def cleanup(self):
        self.rabbit_conn.close()
        sys.exit(1)

    def main(self):
        print("Waiting for dispatcher")

        def callback_spider(ch, method, properties, body):
            url = json.loads(body.decode("utf-8"))['url']
            priority = properties.priority
            print("Get " + url + " with priority " + str(priority))
            self.visit(url)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(callback_spider,
                              queue=targetsQueue)
        self.channel.start_consuming()

    def execute(self):
        try:
            self.main()
        except KeyboardInterrupt:
            self.cleanup()


if __name__ == '__main__':
    Spider().execute()