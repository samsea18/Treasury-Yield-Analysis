from bs4 import BeautifulSoup
import requests


class Tyr_DS(object):
    def __init__(self, url):
        self._url = url

    def get_row_values(self, row_data):
        return [val.get_text() for val in row_data]

    def fetch_treasury_yields(self, year):
        if year == 'ALL':
            treasury_url = self._url + "data=yieldAll"
        else:
            treasury_url = self._url + "data=yieldYear&year=" + "%s" % year

        page = requests.get(treasury_url)
        soup = BeautifulSoup(page.content, 'html.parser')

        rates_table = soup.find(class_="t-chart")

        oddrow_data = self.get_row_values(rates_table.select(".oddrow .text_view_data"))
        evenrow_data = self.get_row_values(rates_table.select(".evenrow .text_view_data"))

        return {'oddrow': oddrow_data, 'evenrow': evenrow_data}
