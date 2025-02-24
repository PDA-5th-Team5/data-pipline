import pandas as pd
import mysql_connector
# 데이터 타입 매핑
dtype_mapping = {
    "ticker": "string",               # 종목 코드 (문자열)
    "company_name": "string",          # 회사명 (문자열)
    "company_overview": "string",      # 회사 개요 (문자열)
    "market_cap": "Int64",             # 시가총액 (큰 정수)
    "market_type": "string",           # 시장 타입 (코스피/코스닥) (문자열)
    "sector": "string",                # 섹터 정보 (문자열)
    "dividend_yield": "float32",       # 배당수익률 (% 단위)
    "sale_account": "Int64",           # 매출액 (큰 정수)
    "bsop_prti": "Int64",              # 영업이익 (큰 정수)
    "thtr_ntin": "Int64",              # 당기순이익 (큰 정수)
    "roe_val": "float32",              # ROE (자기자본이익률) (% 단위)
    "eps": "float32",                  # EPS (주당순이익)
    "per": "float32",                  # PER (주가수익비율)
    "grs": "float32",                  # 매출액 증가율 (% 단위)
    "bsop_prfi_inrt": "float32",       # 영업이익 증가율 (% 단위)
    "ntin_inrt": "float32",            # 순이익 증가율 (% 단위)
    "lblt_rate": "float32",            # 부채 비율 (% 단위)
    "bram_depn": "float32",            # 감가상각비 (단위 % 또는 원 단위)
    "crnt_rate": "float32",            # 유동 비율 (% 단위)
    "sps": "float32",                  # 주당매출액
    "bps": "float32",                  # 주당순자산
    "foreigner_ratio": "float32"       # 외국인 보유율 (% 단위)
}

# 데이터 로드
file_path = "data/stock_info_detail.csv"  # 실제 경로로 변경 필요
df = pd.read_csv(file_path, dtype=dtype_mapping)

df = df[~df["market_type"].isin([None, "", " ", "정보 없음"])]  # 코넥스 종목 제거
df = df.dropna(subset=["market_type"])


df.to_csv("data/stock_info_detail.csv", index=False, encoding="utf-8-sig")
print(df.iloc[-1]["market_type"])

# mc = mysql_connector
# # # MySQL에 업로드
# mc.upload_dataframe_to_mysql(df, "stock", "append")
