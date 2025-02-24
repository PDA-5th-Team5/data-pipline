import time

import requests
import pandas as pd
from tqdm import tqdm
import mysql_connector


def fetch_stock_data(interval, stock_id, ticker, limit=1000):
    """
    Fetch stock data for a specific interval (day, week, month) and return it as a list of dictionaries.
    """
    try:
        # URL 설정
        url = f'https://finance.daum.net/api/charts/A{ticker}/{interval}?limit={limit}&adjusted=true'

        # 헤더 설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            'referer': f'https://finance.daum.net/quotes/{ticker}',
        }

        # 데이터 요청
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 요청이 실패하면 예외 발생

        # 데이터 파싱
        raw_data = response.json()['data']

        # 데이터 변환
        transformed_data = [
            {
                "stock_id": stock_id,
                "ticker": ticker,
                "date": record["date"],
                "open_price": int(record["openingPrice"]),
                "high_price": int(record["highPrice"]),
                "low_price": int(record["lowPrice"]),
                "close_price": int(record["tradePrice"]),
                "volume": int(record["candleAccTradeVolume"]),
                "change_rate": float(record["changeRate"]) if record["changeRate"] is not None else None
            }
            for record in raw_data
        ]

        return transformed_data
    except requests.exceptions.RequestException as e:
        print(f'데이터를 가져오는 중 오류가 발생했습니다: {e}')
        return []
    except KeyError as e:
        print(f'필수 데이터가 응답에 없습니다: {e}')
        return []


def stock_price_crawler(df, interval, n=1000):
    """
    Crawl stock prices for multiple stocks for a specific interval (day, week, month).

    Args:
        df (pd.DataFrame): 대상 종목 정보 (stock_id, ticker 포함).
        interval (str): "days", "weeks", "months" 중 하나.
        n (int): 가져올 데이터 제한 개수 (기본값: 1000).

    Returns:
        pd.DataFrame: 수집된 데이터를 포함한 DataFrame.
    """
    # 데이터 저장용 리스트
    unified_data = []

    # 각 종목에 대해 크롤링 수행
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing stocks ({interval})"):
        stock_id = row["stock_id"]
        ticker = row["ticker"]
        # 데이터 크롤링
        data = fetch_stock_data(interval, stock_id, ticker, limit=n)
        unified_data.extend(data)
        df = pd.DataFrame(unified_data).drop(columns=['ticker'])
    # 데이터프레임으로 반환
    return df


# mc = mysql_connector
# stockInfo_df = mc.read_table_to_dataframe("stock")
# days_price_df = stock_price_crawler(stockInfo_df,'days',1)

# weeks_price_df = stock_price_crawler(stockInfo_df,'weeks',3)
# print(weeks_price_df)
# months_price_df = stock_price_crawler(stockInfo_df,'months',500)
#
# # MySQL에 업로드
# mc.upload_dataframe_to_mysql(weeks_price_df.drop(columns=['ticker']), "stock_price_week", "append")
# mc.upload_dataframe_to_mysql(months_price_df.drop(columns=['ticker']), "stock_price_month", "append")