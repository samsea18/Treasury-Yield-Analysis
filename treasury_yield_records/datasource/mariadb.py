# import sqlalchemy as db
#
# class Mariadb_DS(object):
#
#     def __init__(self, host, username, password):
#         host_url = host
#
#         conn_str = f"mysql://{username}:{password}@{host_url}"
#         self._engine = db.create_engine(conn_str)
#
#     def retrieve_all_rates(self, table):
#         with self._engine.connect() as connection:
#             query = f" select * from '{table}'"
#             result = connection.execute(db.text(query))
#
#             for d in result:
#                 yield dict(d)

import mariadb
import sys

# # Instantiate Connection
# try:
#     conn = mariadb.connect(
#         user="root",
#         password="root",
#         host="localhost",
#         port=3306)
# except mariadb.Error as e:
#     print(f"Error connecting to MariaDB Platform: {e}")
#     sys.exit(1)


class Mariadb_DS(object):

    def __init__(self, username, password, host, db):

        try:
            self._conn = mariadb.connect(
                user=username,
                password=password,
                host=host,
                #port=port
                database=db)

            self._cur = self._conn.cursor(buffered=True , dictionary=True)

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
            #print('now it is: ' + str(x))
            single_query = []

            for key in rows[x].keys():
                rec = str(rows[x].get(key))
                single_query.append(rec)

            result.append(single_query)

        return result

