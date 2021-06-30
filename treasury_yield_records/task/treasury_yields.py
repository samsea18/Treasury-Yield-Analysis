import datetime
import pandas as pd

from ..datasource.fed_treasury_yields import Tyr_DS


class Treasury_Yield_Task(Tyr_DS):
    def __init__(self, TyrDS):
        self._tyrDS = TyrDS
        super().__init__(TyrDS._url)

    def _execute(self, year):

        treasury_records = self._tyrDS.fetch_treasury_yields(year)
        return self.combined_scrapped_yields(treasury_records)

    def is_desc(self, rates_list):
        return sorted(rates_list, reverse=True) == rates_list

    def avg_pct_change(self, rec_list):
        x = pd.Series(rec_list)

        return round(x.pct_change().mean(), 2)

    def process_row_info(self, in_list):
        valMap = []
        record_list = []

        counter = 0

        for x in range(len(in_list)):

            record_list.append(in_list[x])
            counter += 1

            if (counter % 13) == 0:
                record_list[0] = datetime.datetime.strptime(record_list[0], "%m/%d/%y").strftime("%Y-%m-%d")
                record_list[1:13] = [float(i) for i in record_list[1:13]]

                record_list.extend((self.is_desc(record_list[1:13]), self.avg_pct_change(record_list[1:13])))
                valMap.append(record_list)
                record_list = []

        return valMap

    def combined_scrapped_yields(self, yield_list):
        if len(yield_list) == 2:

            combined_map = []

            for key in yield_list.keys():
                combined_map.extend(self.process_row_info(yield_list.get(key)))

            return sorted(combined_map, key=lambda x: x[0])
