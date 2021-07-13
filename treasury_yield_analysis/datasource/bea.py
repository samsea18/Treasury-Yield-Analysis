import requests
import json
from urllib.request import urlopen
import os


class Bea_DS(object):
    def __init__(self, url):
        self._url = url

    def get_year_range(self, year_from, year_to):

        year_range = ""
        for x in range(year_from, year_to):
            year_range += (str(x) + ',')

        return year_range[:-1]

    def fetch_us_q_gdp(self, year_from, year_to):
        BEA_API_KEY = os.environ['BEA_API_KEY']
        year_list = self.get_year_range(year_from, year_to)
        bea_url = self._url + "&UserID=" + BEA_API_KEY + \
                       "&method=GetData&DataSetName=NIPA&TableName=T10105&SeriesCode=A191RC" \
                       "&LineDescription=Gross+domestic+product&Frequency=Q&Year=" \
                       + year_list + "&ResultFormat=json"

        response = urlopen(bea_url)
        data_json = json.loads(response.read())

        return data_json