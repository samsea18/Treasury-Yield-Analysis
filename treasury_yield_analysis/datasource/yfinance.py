import yfinance as yf


class Yfinance_DS(object):
    def __init__(self, ticker):
        self._ticker = ticker

    def fetch_metadata(self):
        return yf.Ticker(self._ticker)

    def fetch_ticker_history(self, start_date, end_date):
        return yf.download(self._ticker, start=start_date, end=end_date)

    def fetch_ticker_all(self, start_date, end_date):
        metadata = self.fetch_metadata(self)
        hist_records = self.fetch_ticker_history(start_date, end_date)

        return {"info": metadata, "historicals": hist_records}

