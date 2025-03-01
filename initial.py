import mysql_connector as mc
import stock_info
import stock_info as si
import pandas as pd
import price_crawler as pc

# 데이터 타입 매핑
from hantwo_api_info import fetch_all_stocks

dtype_mapping = {
    "ticker": "string", "company_name": "string", "company_overview": "string",
    "market_cap": "Int64", "market_type": "string", "sector": "string",
    "dividend_yield": "float32", "sale_account": "Int64", "bsop_prti": "Int64",
    "thtr_ntin": "Int64", "roe_val": "float32", "eps": "float32", "per": "float32",
    "grs": "float32", "bsop_prfi_inrt": "float32", "ntin_inrt": "float32",
    "lblt_rate": "float32", "bram_depn": "float32", "crnt_rate": "float32",
    "sps": "float32", "bps": "float32", "foreigner_ratio": "float32"
}
# 종목명 크롤링
si = stock_info
krx_df = si.KRX_주식_종목명_티커_크롤러()

# 종목 정보 네이버 크롤링 13 분 소요
기업정보_df = si.네이버_기업정보_크롤링(krx_df)
기업정보_df.to_csv("data/stock_info.csv", index=False, encoding="utf-8-sig")

# 한투 API 종목 세부 정보 요청
df = pd.read_csv("./data/stock_info.csv", dtype={"ticker": str})
df = fetch_all_stocks(df)
df.to_csv("data/stock_info_detail.csv", index=False, encoding="utf-8-sig")

# MySQL에 업로드 (기업 정보)
file_path = "data/stock_info_detail.csv"
df = pd.read_csv(file_path, dtype=dtype_mapping)
mc.upload_dataframe_to_mysql(df, "stock", "append")

# MySQL에서 데이터 읽기
stock_info_df = mc.read_table_to_dataframe("stock")

# 주식 가격 크롤링 (일봉 데이터, 최근 500일)
days_price_df = pc.stock_price_crawler(stock_info_df, 'days', 500)

# MySQL에 업로드 (주식 가격 정보)
mc.upload_dataframe_to_mysql(days_price_df, "stock_price_day", "append")

# # 코스피 (KOSPI) 지수 크롤링
kospi_df = pc.get_naver_stock_index("KOSPI", "KOSPI")

# 코스닥 (KOSDAQ) 지수 크롤링
kosdaq_df = pc.get_naver_stock_index("KOSDAQ", "KOSDAQ")

df_market = pd.concat([kospi_df, kosdaq_df], ignore_index=True)
# MySQL에 업로드 (시장지표 가격 정보)
mc.upload_dataframe_to_mysql(df_market, "market_indicator_price", "append")

stockInfo_df = mc.read_table_to_dataframe("stock")

df_normalized, df_max_value = pc.sorted_binning(stockInfo_df)

stockInfo_df = mc.read_table_to_dataframe("stock_stat")
stockInfo_df = mc.read_table_to_dataframe("stock_indicator_thresholds")
