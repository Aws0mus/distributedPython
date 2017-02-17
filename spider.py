import psycopg2
from time import sleep
import random

class Spider:

    def check_broker(self):
        conn = psycopg2.connect("dbname=" + "distributed" +
                                " user=" + "test" +
                                " host=" + "localhost" +
                                " port=" + "9999" +
                                " password=" + "password")
        return conn

    def visit(self):
        soup = "A"*10000
        link = "B"*64
        links = []
        for i in range(0, 100):
            links.append(link+str(i))
        print("Duermo")
        sleep(random.randint(5, 15))
        print("Despierto e inserto")
        self.save_postgres(links)
        #self.save_elastic(soup)

    def save_postgres(self, links):
        try:
            conn = self.check_broker()
        except Exception as e:
            print(e)
            sleep(2)
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


    def save_elastic(self, text):
        pass


spider = Spider()
while True:
    spider.visit()
