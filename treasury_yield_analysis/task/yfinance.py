from ..datasource import Yfinance_DS


class Yfinance_Task(Yfinance_DS):
    def __init__(self, yfinanceDS, col_list):
        self._yfinanceDS = yfinanceDS
        self._col_list = col_list
        super().__init__(yfinanceDS._ticker)

    def _execute(self, start_date, end_date):
        raw_df = self._yfinanceDS.fetch_ticker_all(start_date, end_date)
        prep_df = self.package_df(raw_df)

        return prep_df.values.tolist()

    def extract_cols(self, data):
        new_list = data[self._col_list]
        new_list.reset_index(level=0, inplace=True)
        return new_list

    def package_df(self, data):
        extracted_list = self.extract_cols(data['historicals'])

        extracted_list['Date'] = [str(item) for item in extracted_list['Date']]
        extracted_list['Open'] = [round(item, 2) for item in extracted_list['Open']]
        extracted_list['Close'] = [round(item, 2) for item in extracted_list['Close']]
        extracted_list['Ticker'] = data['metadata'].info['shortName']
        extracted_list['Type'] = data['metadata'].info['quoteType']

        return extracted_list
