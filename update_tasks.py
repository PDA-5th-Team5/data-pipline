from datetime import datetime
import mysql_connector as mc

import redis_connector
from currentPrice import fetch_sector_data
from price_crawler import stock_price_crawler

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

# 월~금 하루에 한번 오후 6시 실행
def update_day():
    current_time = datetime.now()
    print("start_day",current_time)
    stockInfo_df = mc.read_table_to_dataframe("stock")
    days_price_df = stock_price_crawler(stockInfo_df, "days", 1)
    mc.upload_dataframe_to_mysql(days_price_df, "stock_price_day", "append")


# 매 1분마다 실행
def update_1m():
    current_time = datetime.now()
    if current_time.weekday() < 5 and 9 <= current_time.hour < 18:
        print("1분 간격 작업 실행 중...")
        print(current_time)
    else:
        return

    redis_client = get_redis_connection()
    kospi_current_price = fetch_sector_data('KOSPI')
    kosdaq_current_price = fetch_sector_data('KOSDAQ')

    # Redis에 저장
    redis_connector.save_stocks_to_redis(redis_client, kospi_current_price)
    redis_connector.save_stocks_to_redis(redis_client, kosdaq_current_price)

    print("실시간 순위 데이터 저장 완료.")
