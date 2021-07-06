import datetime
from statistics import mean


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

            return round(mean(pct_change_list), 2)

    def cvt_to_float(self, rec_list):
        float_list = []

        for x in rec_list:
            if x is None:
                pass
            else:
                x = float(x)

            float_list.append(x)

        return float_list

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

                record_list.extend((self.is_desc(record_list[1:13]), self.avg_pct_change(record_list[1:13])))
                valMap.append(record_list)
                record_list = []

        return valMap

    def combined_scrapped_yields(self, yield_list):
        if len(yield_list) == 2:

            combined_map = []
            print(yield_list)

            for key in yield_list.keys():
                combined_map.extend(self.process_row_info(yield_list.get(key)))

            return sorted(combined_map, key=lambda x: x[0])
