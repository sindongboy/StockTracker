# code: utf-8

#%%
import sys
import yaml

from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from lxml import html

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk


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
    return stat_cd, item_nm, item_cd, val, dt


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

#%% -- OpenSearch: Indexing Result 
host = 'localhost'
port = 9200
auth = ('admin', 'admin')

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    # client_cert = client_cert_path,
    # client_key = client_key_path,
)


#%%
# Create an index with non-default settings.
if client.indices.exists('news_sentiment') == False:
    response = client.indices.create('news_sentiment', body={
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }
    })
else:
    print('index already exists')

#%%
rows = get_news_sentiment(start_dt=start_dt, end_dt=end_dt)

#%%
bulk_data = []
x = 0
for row in rows:
    doc_id = '_'.join([row[4], row[3]])
    if client.exists(index='news_sentiment', id=doc_id):
        print('document exists: {}'.format(doc_id))
        continue
    bulk_data.append(
        {
            "_index": "news_sentiment",
            "_id": '_'.join([row[4], row[3]]),
            "_source": {        
                'stat_cd': row[0],
                'stat_nm': row[1], 
                'item_cd': row[2], 
                'sentiment': float(row[3]),
                'dt': row[4]
            }
        }
    )
    print('document count: {}'.format(x))
    x += 1

bulk(client, bulk_data)