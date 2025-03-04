import json
import redis
from dotenv import load_dotenv
import os

load_dotenv()

# Redis 연결 설정 함수
def get_redis_client(host="localhost", port=6379, db=0):
    """
    Redis 클라이언트를 생성하는 함수.
    Parameters:
        host (str): Redis 호스트.
        port (int): Redis 포트.
        db (int): Redis 데이터베이스 번호.

    Returns:
        redis.StrictRedis: Redis 클라이언트 객체.
    """
    host = os.getenv("REDIS_HOST", host)
    port = int(os.getenv("REDIS_PORT", port))

    return redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

# Redis에 데이터 저장 함수
def save_df_to_redis_as_nested_json(redis_client, df, key_name):
    """
    DataFrame을 JSON으로 변환하여 Redis에 저장
    - Redis에 저장할 때 2중 객체 구조로 저장
    - key_name: Redis 키
    """
    nested_data = {
        str(index + 1): {
            "종목코드": str(row["종목코드"]),  # 문자열 변환
            "현재가": row["현재가"],
            "등락률": row["등락률"],
            "거래량": row["거래량"]
        }
        for index, row in df.iterrows()
    }

    redis_client.set(key_name, json.dumps(nested_data, ensure_ascii=False))
    print(f"✅ Redis 저장 완료: {key_name}")

# 주식 데이터를 Redis에 저장하는 함수
def save_stocks_to_redis(redis_client, stocks_data):
    """
    주식 데이터를 Redis에 저장하는 함수.
    - 각 종목 데이터를 해시(Hash) 구조로 저장
    """
    for stock in stocks_data:
        key = stock.get("ticker")  # Redis 키
        if not key:
            continue  # ticker 없는 경우 스킵

        stock_data = stock.copy()
        stock_data.pop("ticker", None)  # ticker 제거 (키로 사용됨)

        # Redis에 저장 (각 필드를 개별적으로 hset 사용)
        for field, value in stock_data.items():
            redis_client.hset(key, field, json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list, str)) else str(value))

    print("✅ Redis에 주식 데이터 저장 완료")

# 주식 ID 데이터를 Redis에 저장하는 함수
def save_id_to_redis(redis_client, stocks_data):
    """
    주식 ID 데이터를 Redis에 저장하는 함수.
    - stock_id를 키로 하여 수익률 정보를 해시(Hash) 구조로 저장
    """
    for stock in stocks_data.itertuples(index=False, name=None):
        key = str(stock[0])  # stock_id를 문자열로 변환
        stock_dict = {
            'stock_id': str(stock[0]),  # 숫자는 문자열 변환
            'week_rate_change': str(stock[1]),  # 숫자 값 문자열 변환
            'year_rate_change': str(stock[2])
        }

        # Redis에 저장
        for field, value in stock_dict.items():
            redis_client.hset(key, field, value)

    print("✅ Redis에 주식 ID 데이터 저장 완료")

