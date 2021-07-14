import mariadb
import sys


class Mariadb_DS(object):

    def __init__(self, username, password, host, db):
        self._username = username
        self._password = password
        self._host = host
        self._db = db

        try:
            self._conn = mariadb.connect(
                user=self._username,
                password=self._password,
                host=self._host,
                # port=port
                database=self._db)

            self._cur = self._conn.cursor(buffered=True, dictionary=True)

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

    def retrieve_all_treasury_records(self):
        query = f"SELECT * FROM test_rates"

        self._cur.execute(query)

        rows = self._cur.fetchall()
        self._conn.close()

        result = []

        for x in range(len(rows)):
            single_query = []

            for key in rows[x].keys():
                rec = str(rows[x].get(key))
                single_query.append(rec)

            result.append(single_query)

        return result

    def insert_treasury_records(self, var_string, tyr_list):
        for records in tyr_list:
            print(records)
            query_string = 'INSERT INTO test_rates VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()

    def insert_us_gdp_records(self, var_string, us_gdp_list):
        for records in us_gdp_list:
            query_string = 'INSERT INTO test_gdp VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()

    def insert_stock_market_records(self, var_string, stock_market_list):
        for records in stock_market_list:
            query_string = 'INSERT INTO us_stock_market VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()
