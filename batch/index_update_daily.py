# code: utf-8

#%%
import sys
import logging
from logging import handlers, config

import yaml
import pymysql as sql
from index import *

#%%
"""
Logging (code)
"""
with open(sys.argv[1], 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

logging.config.fileConfig(config['logger_config_path'])
logger = logging.getLogger()


#%%
batch_type = sys.argv[2]
if batch_type == "d":
    target_dt = sys.argv[3]
    logger.info('Daily batch mode - (target_dt: {})'.format(target_dt))
elif batch_type == "b":
    start_dt = sys.argv[4]
    end_dt = sys.argv[5]
    logger.info('Bulk batch mode - (start_dt: {}, end_dt: {})'.format(start_dt, end_dt))
else:
    logger.error("Invalid batch type: should be one of 'd' for daily batch or 'b' for bulk batch")
    sys.exit(1)

# target_dt = '20221101'
# logger.error('target_dt: {}'.format(target_dt))
#%%
db = sql.connect(
    user='root', 
    passwd='1111', 
    host='127.0.0.1', 
    db='stock_tracker', 
    charset='utf8'
)
cursor = db.cursor(sql.cursors.DictCursor)


def insert_single(row):
    # check if row exists in database
    c_query = "select * from news_sentiment where dt = %s"
    is_exists = cursor.execute(c_query, row[4])
    if is_exists:
        pass

    query = "INSERT INTO `news_sentiment`(stat_cd, stat_nm, item_cd, sentiment, dt, ts) \
        VALUES (%s, %s, %s, %s, %s, %s);"
    cursor.execute(query, row[0])
    db.commit()


def insert_bulk(rows):
    for row in rows:
        insert_single(row)


#%%
"""
News Sentiment Index
"""
news_sentiment = NewsSentimentIndex()
try:
    if batch_type == 'd':
        row = news_sentiment.get_index_by_date(datetime.strptime(target_dt, "%Y%m%d"))
        logger.info("News Sentiment Index for {} fetched: {}".format(target_dt, len(row)))
        insert_single(row)
    else:
        rows = news_sentiment.get_index_by_range(
                start_dt=datetime.strptime(start_dt, "%Y%m%d"),
                end_dt=datetime.strptime(end_dt, "%Y%m%d")
                )
        logger.info("News Sentiment Index for ({} ~ {}) fetched: {}".format(start_dt, end_dt, len(rows)))
        insert_bulk(rows)
except Exception as e:
    logger.error(e)
    sys.exit(1)
finally:
    cursor.close()
    db.close()