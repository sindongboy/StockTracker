# code: utf-8

from abc import *
from datetime import datetime, timedelta

import time
import requests
from bs4 import BeautifulSoup
from lxml import html

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote

class Index(metaclass=ABCMeta):
    @abstractmethod
    def get_index_by_date(date: datetime):
        pass

    @abstractmethod
    def get_index_by_range(start_dt: datetime, end_dt: datetime):
        pass

    @abstractmethod
    def get_index_by_period(start_dt: datetime, delta_dt: datetime):
        pass



class NewsSentimentIndex(Index):
    """
    뉴스 심리 지수
    선행 지수이며 주요 경제 지표에 1~2개월 정도 선행한다고 알려져 있음
    매주 화요일 발표
    """
    def __init__(self):
        self.info = {
            'stat_cd': '521Y001',
            'stat_nm': 'news sentiment index',
            'api_key': 'JM1DKCYAKDZ97NFRDYHB',
            'period': 'D'
        }
        self.url = 'http://ecos.bok.or.kr/api/StatisticSearch/JM1DKCYAKDZ97NFRDYHB/xml/kr/1/30000/521Y001/D/{}/{}/'


    def __get_parsed__(self, row):
        stat_cd = row.find('STAT_CODE').text
        item_nm = row.find('ITEM_NAME1').text
        item_cd = row.find('ITEM_CODE1').text
        val = row.find('DATA_VALUE').text
        dt = row.find('TIME').text
        ts = time.time()
        timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return stat_cd, item_nm, item_cd, float(val), dt, timestamp


    def get_index_by_range(self, start_dt: datetime, end_dt: datetime):
        response = requests.get(self.url.format(start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d'))).content.decode('utf-8')
        xml_obj = BeautifulSoup(response, 'lxml-xml')
        rows = xml_obj.findAll("row")

        if len(rows) == 1:
            return [self.__get_parsed__(rows[0])] 
        elif len(rows) > 1:
            result = []
            for row in rows:
                result.append(self.__get_parsed__(row))
            return result
        else:
            return []


    def get_index_by_date(self, date: datetime):
        return self.get_index_by_range(date, date)


    def get_index_by_period(self, start_dt: datetime, delta_dt: datetime):
        today = datetime.today()
        end_dt = start_dt + timedelta(days=delta_dt)

        if (today - end_dt) >= 0:
            end_dt = today

        return self.get_index_by_range(start_dt=start_dt, end_dt=end_dt)
