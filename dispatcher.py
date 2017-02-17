import psycopg2
from time import sleep


class Dispatcher:

    def check_pool(self):
        conn = psycopg2.connect("dbname=" + "distributed" +
                                " user=" + "test" +
                                " host=" + "localhost" +
                                " port=" + "5432" +
                                " password=" + "password")
        return conn

    def run(self):

        initial = 6100
        step = 50
        while True:
            try:
                conn = self.check_pool()
                break
            except Exception as e:
                print(e)
                sleep(2)

        while True:
            last = initial + step
            sleep(2)
            try:
                cur = conn.cursor()

                cur.execute("SELECT * FROM tabla2 WHERE id >= %s AND id < %s", [initial, last])
                rows = cur.fetchall()

                initial = last
                for row in rows:
                    print(row)

                cur.execute("SELECT last_value FROM tabla2_id_seq")
                last_value = cur.fetchone()[0]
                if last > last_value:
                    initial = 0

            except Exception as e:
                print(e)
                conn.rollback()

            cur.close()

        conn.close()


dispatcher = Dispatcher()
dispatcher.run()
