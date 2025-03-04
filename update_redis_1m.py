from datetime import datetime
import mysql_connector as mc
import pandas as pd

import price_crawler
import redis_connector
from currentPrice import fetch_sector_data
from price_crawler import stock_price_crawler,get_naver_stock_index

redis_client = redis_connector.get_redis_client()
def get_redis_connection():
    """
    Redis 연결 확인 및 재연결 함수.
    """
    global redis_client
    try:
        # Redis 연결 상태 확인
        redis_client.ping()
    except Exception as e:
        print(f"Redis 연결 재설정 중...: {e}")
        redis_client = redis_connector.get_redis_client()  # 재연결 수행
    return redis_client

# 매 1분마다 실행
def update_1m():
    try:
        global redis_client
        kospi_current_price = fetch_sector_data('KOSPI')
        kosdaq_current_price = fetch_sector_data('KOSDAQ')

        # Redis에 저장
        redis_connector.save_stocks_to_redis(redis_client, kospi_current_price)
        redis_connector.save_stocks_to_redis(redis_client, kosdaq_current_price)
    except Exception as e:
        print(e)

update_1m()