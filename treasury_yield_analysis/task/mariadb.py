from ..datasource.mariadb import Mariadb_DS


class Mariadb_Task(Mariadb_DS):
    def __init__(self, Mariadb_DS):
        super().__init__(Mariadb_DS._username, Mariadb_DS._password, Mariadb_DS._host, Mariadb_DS._db)
        self._mariadb_DS = Mariadb_DS

    def insert_treasury_yields(self, tyr_list):
        var_string = ', '.join('?' * len(tyr_list[0]))

        self.insert_treasury_records(var_string, tyr_list)

    def insert_us_q_gdp(self, us_gdp_list):
        var_string = ', '.join('?' * len(us_gdp_list[0]))

        self.insert_us_gdp_records(var_string, us_gdp_list)