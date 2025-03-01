from datetime import datetime
import mysql_connector as mc
import pandas as pd
import redis_connector
from currentPrice import fetch_sector_data
from price_crawler import stock_price_crawler,get_naver_stock_index

# 월~금 하루에 한번 오후 4시
def update_day():
    try:
        current_time = datetime.now()
        print("start_day",current_time)
        stockInfo_df = mc.read_table_to_dataframe("stock")

        today_date = pd.to_datetime("today").strftime("%Y-%m-%d")

        # stock_price_crawler 실행 후 데이터프레임 필터링
        days_price_df = stock_price_crawler(stockInfo_df, "days", 1)

        # 오늘 날짜와 다른 행 제거
        days_price_df = days_price_df[days_price_df['date'] == today_date]
        mc.upload_dataframe_to_mysql(days_price_df, "stock_price_day", "append")

        kospi_df = get_naver_stock_index("KOSPI", "KOSPI",1)

        # 코스닥 (KOSDAQ) 지수 크롤링
        kosdaq_df = get_naver_stock_index("KOSDAQ", "KOSDAQ",1)

        df_market = pd.concat([kospi_df, kosdaq_df], ignore_index=True)
        # MySQL에 업로드 (시장지표 가격 정보)
        mc.upload_dataframe_to_mysql(df_market, "market_indicator_price", "append")
    except Exception as e:
        print(e)
    print("실시간 순위 데이터 저장 완료.")

update_day()

