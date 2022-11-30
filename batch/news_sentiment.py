# code: utf-8

#%%
import sys
import yaml

import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from lxml import html

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote

# from opensearchpy import OpenSearch
# from opensearchpy.helpers import bulk

import pymysql as sql


#%%
start_dt = sys.argv[1]
end_dt = sys.argv[2]
#%%
# with open(sys.argv[1], 'r') as f:
#     config = yaml.load(f, Loader=yaml.FullLoader)

with open('../config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

#%%
def get_parsed(row):
    stat_cd = row.find('STAT_CODE').text
    item_nm = row.find('ITEM_NAME1').text
    item_cd = row.find('ITEM_CODE1').text
    val = row.find('DATA_VALUE').text
    dt = row.find('TIME').text
    ts = time.time()
    timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return stat_cd, item_nm, item_cd, float(val), dt, timestamp


def get_news_sentiment(start_dt, end_dt):
    news_sentiment_url = config['stat_info']['news_sentiment']['url']\
            .format(
                config['API_KEY'],
                config['stat_info']['news_sentiment']['cd'],
                'D',
                start_dt,
                end_dt 
            )

    response = requests.get(news_sentiment_url).content.decode('utf-8')
    xml_obj = BeautifulSoup(response, 'lxml-xml')
    rows = xml_obj.findAll("row")

    if len(rows) == 1:
        return [get_parsed(rows[0])] 
    elif len(rows) > 1:
        result = []
        for row in rows:
            result.append(get_parsed(row))
        return result
    else:
        return []


def insert_single(row):
    dt = row[4]
    new_val = row[3]

    query = "SELECT * FROM `news_sentiment` WHERE dt = %s limit 1;"
    cursor.execute(query, dt)
    fetched = cursor.fetchall()
    if len(fetched):
        # update entry if value changed, otherwise skip the update
        prev_val = fetched[0]['sentiment']
        if prev_val != new_val:
            # delete previous entry
            query = "DELETE FROM `news_sentiment` WHERE dt = %s"
            cursor.execute(query, dt)
            db.commit()

            # insert new entry (update)
            query = "INSERT INTO `news_sentiment`(stat_cd, stat_nm, item_cd, sentiment, dt, ts) \
                VALUES (%s, %s, %s, %s, %s, %s);"
            cursor.execute(query, row)
            db.commit()
    else:
        # new entry
        query = "INSERT INTO `news_sentiment`(stat_cd, stat_nm, item_cd, sentiment, dt, ts) \
            VALUES (%s, %s, %s, %s, %s, %s);"
        cursor.execute(query, row)
        db.commit()


def insert_bulk(rows):
    for row in rows:
        insert_single(row)


# %%
db = sql.connect(
    user='root', 
    passwd='1111', 
    host='127.0.0.1', 
    db='stock_tracker', 
    charset='utf8'
)
cursor = db.cursor(sql.cursors.DictCursor)

rows = get_news_sentiment(start_dt=start_dt, end_dt=end_dt)
insert_bulk(rows)

cursor.close()
db.close()
# %%
