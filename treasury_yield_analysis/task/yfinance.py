from ..datasource import Yfinance_DS


class Yfinance_Task(Yfinance_DS):
    def __init__(self, yfinanceDS, col_list):
        self._yfinanceDS = yfinanceDS
        self._col_list = col_list
        super().__init__(yfinanceDS._ticker)

    def extract_cols(self, data):
        new_list = data[self._col_list]

        return new_list.reset_index(level=0, inplace=True)

    def package_df(self, data):
        extracted_list = self.extract_cols(data['historicals'])

        extracted_list['Date'] = [str(item)[0:10] for item in extracted_list['Date']]
        extracted_list['Open'] = [round(item, 2) for item in extracted_list['Open']]
        extracted_list['Close'] = [round(item, 2) for item in extracted_list['Close']]
        extracted_list['Ticker'] = data['info']['shortName']
        extracted_list['Type'] = data['info']['shortName']['quoteType']

        return extracted_list.values.tolist()
