import mariadb
import sys


class Mariadb_DS(object):

    def __init__(self, username, password, host, db):

        try:
            self._conn = mariadb.connect(
                user=username,
                password=password,
                host=host,
                # port=port
                database=db)

            self._cur = self._conn.cursor(buffered=True, dictionary=True)

        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

    def retrieve_all_treasury_recs(self):
        query = f"SELECT * FROM test_rates"

        self._cur.execute(query)

        rows = self._cur.fetchall()
        self._conn.close()

        result = []

        for x in range(len(rows)):
            # print('now it is: ' + str(x))
            single_query = []

            for key in rows[x].keys():
                rec = str(rows[x].get(key))
                single_query.append(rec)

            result.append(single_query)

        return result
