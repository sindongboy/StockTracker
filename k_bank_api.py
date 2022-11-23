# code: utf-8

#%%
import pandas_datareader as pdr
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from lxml import html

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote
import pprint

# import matplotlib.pyplot as plt
# import seaborn as sns

# %matplotlib inline

#%%

API_KEY='JM1DKCYAKDZ97NFRDYHB'

url_list = {
    'news_sentiment': 'http://ecos.bok.or.kr/api/StatisticSearch/{}/xml/kr/1/30000/{}/{}/{}/{}/'
}

STAT_CD_LIST = {
    'news_sentiment': '521Y001'
}


#%%
def get_product(KEY, STAT_CD, PERIOD, START_DATE, END_DATE):
    news_sentiment_url = url_list['news_sentiment'].format(KEY, STAT_CD, PERIOD, START_DATE, END_DATE)
    response = requests.get(news_sentiment_url).content.decode('utf-8')
    xml_obj = BeautifulSoup(response, 'lxml-xml')
    rows = xml_obj.findAll("row")
    return rows


def get_parsed(KEY, STAT_CD, PERIOD, START_DATE, END_DATE):
    rows = get_product(API_KEY, STAT_CD_LIST['news_sentiment'], 'D', '20221101', '20221111')
    for row in rows:
        # stat cd
        stat_cd = row.find('STAT_CODE').text
        item_nm = row.find('ITEM_NAME1').text
        item_cd = row.find('ITEM_CODE1').text
        val = row.find('DATA_VALUE').text
        dt = row.find('TIME').text

        print(stat_cd, item_nm, item_cd, val, dt)


#%%

item_name_list = [
    'STAT_CODE',
    'STAT_NAME', 
    'ITEM_CODE1',
    'ITEM_NAME1',
    'ITEM_CODE2',
    'ITEM_NAME2',
    'ITEM_CODE3',
    'ITEM_NAME3',
    'UNIT_NAME',
    'TIME',
    'DATA_VALUE'
]


