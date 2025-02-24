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

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))


    return redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

# Redis에 데이터 저장 함수
def save_df_to_redis_as_nested_json(redis_client, df, key_name):
    """
    DataFrame을 JSON으로 변환하여 Redis에 저장
    - Redis에 저장할 때 2중 객체 구조로 저장
    - key_name: Redis 키
    """
    # DataFrame을 JSON 형태로 변환
    nested_data = {
        str(index + 1): {
            "종목코드": row["종목코드"],
            "현재가": row["현재가"],
            "등락률": row["등락률"],
            "거래량": row["거래량"]
        }
        for index, row in df.iterrows()
    }

    # JSON 데이터를 Redis에 저장
    redis_client.set(key_name, json.dumps(nested_data, ensure_ascii=False))
    # print(f"Redis 저장 완료: {key_name} -> {nested_data}")

def save_stocks_to_redis(redis_client, stocks_data):
    """주식 데이터를 Redis에 저장하는 함수."""
    for stock in stocks_data:
        key = f"stock:{stock['ticker']}"  # Redis 키 생성
        redis_client.hmset(key, stock)  # 데이터를 해시 형식으로 저장
        # print(f"Redis 저장 완료: {key} -> {stock}")