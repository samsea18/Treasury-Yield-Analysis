import datetime
import pandas as pd
from statistics import mean
import math

from ..datasource.fed_treasury_yields import Tyr_DS


class Treasury_Yield_Task(Tyr_DS):
    def __init__(self, TyrDS):
        self._tyrDS = TyrDS
        super().__init__(TyrDS._url)

    def _execute(self, year):

        treasury_records = self._tyrDS.fetch_treasury_yields(year)
        return self.combined_scrapped_yields(treasury_records)

    def is_desc(self, rates_list):
        rates_list = [i for i in rates_list if i is not None]
        return sorted(rates_list, reverse=True) == rates_list

    def avg_pct_change(self, rec_list):
        rec_list = [i for i in rec_list if i is not None]
        pct_change_list = []

        if len(rec_list) == 0:
            return 0

        elif len(rec_list) > 0:
            for x in range(len(rec_list) - 1):
                if rec_list[x] == 0:
                    pct_change_list.append(rec_list[x + 1])
                else:
                    pct_change_val = (rec_list[x + 1] - rec_list[x]) / rec_list[x]
                    pct_change_list.append(pct_change_val * 100)

            return pct_change_list

    def sum_changes(self, rec_list):
        rec_list = [i for i in rec_list if i is not None]
        pct_diff_list = []

        if len(rec_list) == 0:
            return 0

        elif len(rec_list) > 0:
            for x in range(len(rec_list) - 1):
                val_diff = rec_list[x + 1] - rec_list[x]
                pct_diff_list.append(val_diff)

            return sum(pct_diff_list)

    def cvt_to_float(self, rec_list):
        float_list = []

        for x in rec_list:
            if x is None:
                pass
            else:
                x = float(x)

            float_list.append(x)

        return float_list

    def validate_records(self, rec_list):
        test_list = [i for i in rec_list[1:13] if i is not None]

        if rec_list[0] != '2017-04-14' and len(test_list) > 0:
            val = True
        else:
            val = False

        return val

    def process_row_info(self, in_list):
        valMap = []
        record_list = []

        counter = 0

        for x in range(len(in_list)):

            record_list.append(in_list[x])
            counter += 1

            if (counter % 13) == 0:

                record_list[0] = datetime.datetime.strptime(record_list[0], "%m/%d/%y").strftime("%Y-%m-%d")
                record_list[1:13] = self.cvt_to_float(record_list[1:13])

                if self.validate_records(record_list) is True:
                    is_desc = self.is_desc(record_list[1:13])
                    pct_change_list = self.avg_pct_change(record_list[1:13])
                    avg_pct = round(mean(pct_change_list), 2)

                    cleansed_pct_dd = self.avg_pct_change(pct_change_list)
                    avg_pct_dd = round(mean(cleansed_pct_dd), 2)
                    total_change_movement = round(self.sum_changes(record_list[1:13]), 2)

                    record_list.extend((is_desc, avg_pct, avg_pct_dd, total_change_movement))
                    valMap.append(record_list)

                else:
                    pass

                record_list = []

        return valMap

    def combined_scrapped_yields(self, yield_list):
        if len(yield_list) == 2:

            combined_map = []

            for key in yield_list.keys():
                combined_map.extend(self.process_row_info(yield_list.get(key)))

            return sorted(combined_map, key=lambda x: x[0])
