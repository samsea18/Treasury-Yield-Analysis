import locale
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

from ..datasource.bea import Bea_DS


class BEA_Task(Bea_DS):
    def __init__(self, beaDS):
        self._beaDS = beaDS
        super().__init__(beaDS._url)

    def _execute(self, year_from, year_to):
        raw_gdp = self._beaDS.fetch_us_q_gdp(year_from, year_to)
        extracted_gdp = self.parse_bea_gdp_list(raw_gdp)

        return self.mil_to_tril(self.assign_dates(extracted_gdp))

    def parse_bea_gdp_list(self, raw_gdp_list):
        extraced_gdp_list = []

        for x in raw_gdp_list['BEAAPI']['Results']['Data']:

            if 'Gross domestic product' in x.values():
                y_gdp = []
                y_gdp.append(x['TimePeriod'])
                y_gdp.append(locale.atoi(x['DataValue']))
                # y_gdp.append(locale.atof(x['DataValue']))
                extraced_gdp_list.append(y_gdp)

        return extraced_gdp_list

    def assign_dates(self, extraced_gdp_list):
        vals = {'Q1': '03-30', 'Q2': '06-30', 'Q3': '09-30', 'Q4': '12-30'}

        for x in range(len(extraced_gdp_list)):
            cal_date = extraced_gdp_list[x][0][0:4] + "-" + vals[extraced_gdp_list[x][0][-2:]]
            extraced_gdp_list[x].extend([cal_date])

        return extraced_gdp_list

    def mil_to_tril(self, extraced_gdp_list):

        for x in range(len(extraced_gdp_list)):
            extraced_gdp_list[x].extend([round(extraced_gdp_list[x][1] / (10 ** 6), 2)])

        return extraced_gdp_list