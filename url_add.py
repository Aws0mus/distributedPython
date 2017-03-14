import psycopg2
import settings
import sys
import argparse
import pika
import json

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


def add_broker(url, priority):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbit_host))
    except Exception as e:
        print(e)
        print("RabbitMQ Error")
        sys.exit(1)

    channel = connection.channel()
    args = {"x-max-priority": 10}
    channel.queue_declare(queue=targetsQueue, durable=True, arguments=args)

    data = {'url': url, 'from': url}
    channel.basic_publish(exchange='',
                          routing_key=targetsQueue,
                          body=json.dumps(data),
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                              priority=priority,
                          ))
    connection.close()


def add_bd(url, priority):
    try:
        conn = psycopg2.connect("dbname=" + psql_name +
                                " user=" + psql_user +
                                " host=" + psql_host +
                                " port=" + psql_port +
                                " password=" + psql_password)
    except Exception as e:
        print(e)
        print("PostgreSQL Error")
        sys.exit(1)

    cur = conn.cursor()

    print("Inserting " + url)
    cur.execute("INSERT INTO tabla2 VALUES (DEFAULT , %s)", [url])
    conn.commit()
    cur.close()
    conn.close()


def main(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', required=True, help="Url to insert")
    parser.add_argument('-p', '--priority', metavar='N', default=5, type=int, required=False,
                        help="Value between 0 and 10 for url's priority.")
    parser.add_argument('-b', '--broker', action='store_true',
                        help="Adds url to the broker queue too. Use when postgresql has a lot of content.")
    args = parser.parse_args(arguments)

    priority = args.priority
    url = args.url
    if args.broker:
        print("Inserting on bd and broker: " + url + " with priority " + str(priority) )
        add_bd(url, priority)
        add_broker(url, priority)
    else:
        print("Inserting on bd: " + url + " with priority " + str(priority))
        add_bd(url, priority)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
