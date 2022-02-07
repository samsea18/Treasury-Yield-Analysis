from ..datasource.mariadb import Mariadb_DS


class Mariadb_Task(Mariadb_DS):
    def __init__(self, Mariadb_DS):
        super().__init__(Mariadb_DS._username, Mariadb_DS._password, Mariadb_DS._host, Mariadb_DS._db, Mariadb_DS._app_os)
        self._mariadb_DS = Mariadb_DS

    def insert_treasury_yields(self, tyr_list):

        if self._app_os == 'Darwin':
            self.insert_treasury_records_sqlal(tyr_list)

        else:
            var_string = ', '.join('?' * len(tyr_list[0]))
            self.insert_treasury_records(var_string, tyr_list)

    def insert_us_q_gdp(self, us_gdp_list):

        if self._app_os == 'Darwin':
            self.insert_us_gdp_records_sqlal(us_gdp_list)

        else:
            var_string = ', '.join('?' * len(us_gdp_list[0]))

            self.insert_us_gdp_records(var_string, us_gdp_list)

    def insert_stock_market_performance(self, stock_market_list):
        var_string = ', '.join('?' * len(stock_market_list[0]))

        self.insert_stock_market_records(var_string, stock_market_list)

