import sys
import sqlalchemy

try:
    import mariadb
except:
    print(sys.exc_info())

class Mariadb_DS(object):

    def __init__(self, username, password, host, db, app_os):
        self._username = username
        self._password = password
        self._host = host
        self._db = db
        self._app_os = app_os

        if self._app_os == 'Darwin':
            db_url = 'mariadb+pymysql://' + self._username + ":" + self._password + "@" + self._host + "/" + self._db + "?charset=utf8mb4"
            engine = sqlalchemy.create_engine(db_url)
            self._conn = engine.connect()
            self._cur = None

        else:
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
        query = f"SELECT * FROM yield_rates"

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
            query_string = 'INSERT INTO yield_rates VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()

    def insert_us_gdp_records(self, var_string, us_gdp_list):
        for records in us_gdp_list:
            query_string = 'INSERT INTO gdp VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()

    def insert_stock_market_records(self, var_string, stock_market_list):
        for records in stock_market_list:
            query_string = 'INSERT INTO us_stock_market VALUES (%s);' % var_string
            self._cur.execute(query_string, records)

        self._conn.commit()


    def insert_treasury_records_sqlal(self, tyr_list):

        metadata = sqlalchemy.MetaData()

        yield_table = sqlalchemy.Table('yield_rates', metadata, autoload=True, autoload_with=self._conn)

        for records in tyr_list:
            query = sqlalchemy.insert(yield_table)

            values_list = [{'date': records[0],
                            '1_Mo': records[1],
                            '2_Mo': records[2],
                            '3_Mo': records[3],
                            '6_Mo': records[4],
                            '1_Yr': records[5],
                            '2_Yr': records[6],
                            '3_Yr': records[7],
                            '5_Yr': records[8],
                            '7_Yr': records[9],
                            '10_Yr': records[10],
                            '20_Yr': records[11],
                            '30_Yr': records[12],
                            'desc_rates': records[13],
                            'pct_change': records[14]}]

            self._conn.connect().execute(query, values_list)
