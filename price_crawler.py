import time

import numpy as np
import requests
import pandas as pd
from tqdm import tqdm
import mysql_connector
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


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
        time.sleep(0.5)
        stock_id = row["stock_id"]
        ticker = row["ticker"]
        # 데이터 크롤링
        data = fetch_stock_data(interval, stock_id, ticker, limit=n)
        unified_data.extend(data)
        df = pd.DataFrame(unified_data).drop(columns=['ticker'])
    # 데이터프레임으로 반환
    return df

# 코스피 코스닥 지수 크롤러
def get_naver_stock_index(market_code, market_name, days=500):
    base_url = f"https://finance.naver.com/sise/sise_index_day.nhn?code={market_code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    df_list = []

    for page in range(1, 50):  # 최대 50페이지까지 크롤링
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="type_1")

        rows = table.find_all("tr")[2:]  # 첫 번째 행(헤더) 제외
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:  # 데이터가 없는 경우 스킵
                continue

            date_str = cols[0].text.strip()
            close_price = cols[1].text.strip().replace(",", "")

            try:
                date = datetime.strptime(date_str, "%Y.%m.%d")
                close_price = float(close_price)
                df_list.append([market_name, date, close_price])
            except ValueError:
                continue

        if df_list and df_list[-1][1] < datetime.today() - timedelta(days=days):
            break  # 1년 치 데이터가 넘으면 종료

    df = pd.DataFrame(df_list, columns=["market", "date", "price"])
    df["date"] = pd.to_datetime(df["date"])  # 날짜 타입 변환
    df["price"] = df["price"].astype(float)  # float
    df = df.drop_duplicates(subset=['market', 'date'], keep='first')
    df = df[df["date"] >= datetime.today() - timedelta(days=days)]
    return df


# 정렬 기반 1~20 범위 균등 분배 함수
def sorted_binning(df):
    df2 = df.copy()
    columns = [
        "market_cap", "thtr_ntin", "bsop_prti", "per", "eps", "bps", "pbr",
        "dividend_yield", "foreigner_ratio", "sps", "sale_account", "crnt_rate",
        "lblt_rate", "ntin_inrt", "bsop_prfi_inrt", "grs", "roe_val"
    ]
    index_max_value = []

    for col in columns:
        if col not in df.columns:  # 존재하지 않는 컬럼 스킵
            continue
    # NaN 및 -inf, inf 값 처리
        df[col] = df[col].replace([np.inf], np.nan)  # inf 제거
        df[col] = df[col].replace(-np.inf, df[col].min() - 1)  # -inf를 최소값보다 작은 값으로 설정
        df[col] = df[col].fillna(df[col].min() - 1)  # NaN이면 1로 설정

        # 데이터 개수가 20개 미만이면 중앙값(10) 부여
        if df[col].nunique() < 20:
            df[col] = 10
            continue

        # 정렬 후 1~20 할당
        df[col] = df[col].rank(method="first")  # 순위 부여
        df[col] = pd.qcut(df[col], q=20, labels=range(1, 21), duplicates='drop')  # 균등 분배
        for value in range(1, 21):  # 1~20까지 반복
            value_rows = df[df[col] == value]
            max_value = df2.loc[value_rows.index, col].max() if not value_rows.empty else None
            index_max_value.append([col, value, max_value])

    result_df = pd.DataFrame(index_max_value, columns=["Column", "Value", "max_value"])

    return df, result_df


def 일주일일년변동률구하는함수():
    # 오늘 날짜
    today_date = pd.to_datetime("today").strftime("%Y-%m-%d")
    # 1년 전
    one_year_ago = (pd.to_datetime("today") - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    # 7일 전
    seven_days_ago = (pd.to_datetime("today") - pd.DateOffset(days=7)).strftime("%Y-%m-%d")

    # today_date = (pd.to_datetime("today") - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    # one_year_ago = (pd.to_datetime("today") - pd.Timedelta(days=1) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    # seven_days_ago = (pd.to_datetime("today") - pd.Timedelta(days=1) - pd.DateOffset(days=7)).strftime("%Y-%m-%d")

    # stock 테이블 데이터 불러오기

    query = f"""
    SELECT stock_id, date, close_price 
    FROM stock_price_day 
    WHERE date IN ('{today_date}', '{one_year_ago}', '{seven_days_ago}')
    """

    # 데이터 가져오기 (mc는 사용자의 DB 연결 객체)
    stockInfo_df = mysql_connector.execute_query(query)

    stockInfo_df["date"] = pd.to_datetime(stockInfo_df["date"])

    # 오늘, 1년 전, 7일 전 데이터 각각 필터링
    today_df = stockInfo_df[stockInfo_df["date"] == today_date][["stock_id", "close_price"]].rename(
        columns={"close_price": "today"})
    one_year_ago_df = stockInfo_df[stockInfo_df["date"] == one_year_ago][["stock_id", "close_price"]].rename(
        columns={"close_price": "one_year_ago"})
    seven_days_ago_df = stockInfo_df[stockInfo_df["date"] == seven_days_ago][["stock_id", "close_price"]].rename(
        columns={"close_price": "seven_days_ago"})

    # stock_id 기준으로 병합
    result_df = today_df.merge(one_year_ago_df, on="stock_id", how="left")
    result_df = result_df.merge(seven_days_ago_df, on="stock_id", how="left")

    result_df["week_rate_change"] = round(result_df["today"] / result_df["seven_days_ago"] - 1, 2)
    result_df["year_rate_change"] = round(result_df["today"] / result_df["one_year_ago"] - 1, 2)
    result_df.fillna(0, inplace=True)
    result_df = result_df[["stock_id", "week_rate_change", "year_rate_change"]]
    return result_df

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