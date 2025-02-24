import os
from datetime import datetime
import requests
import pandas as pd
import time
import hantwo_api_token
from dotenv import load_dotenv
from tqdm import tqdm

# .env 파일 로드
load_dotenv()
disable_tqdm = os.getenv("DISABLE_TQDM", "false").lower() == "true"

# API 키 및 토큰 설정
APP_KEY = hantwo_api_token.APP_KEY
APP_SECRET = hantwo_api_token.APP_SECRET
token = hantwo_api_token.get_access_token()

# API 요청
BASE_URL = "https://openapi.koreainvestment.com:9443"


def get_headers(tr_id):
    """공통 헤더 반환"""
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id
    }


def request_api(url, headers, params=None):
    """ API 요청 및 예외 처리 """
    try:
        response = requests.get(url, headers=headers, params=params)
        response_json = response.json()
        if response.status_code == 200 and response_json.get("rt_cd") == "0":
            return response_json.get("output", {})
        elif response_json.get("msg_cd") == "EGW00121":
            print("Token expired. Refreshing...")
            global token
            token = hantwo_api_token.get_access_token()
            return request_api(url, headers, params)
        elif response_json.get("msg_cd") == "EGW00132":
            print("Rate limit exceeded. Retrying...")
            time.sleep(1)
            return request_api(url, headers, params)
        else:
            print(f"API Error: {response_json}")
            return None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None


def fetch_price_data(ticker):
    """ 주식 시세 조회 """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    params = {"FID_DIV_CLS_CODE": "0", "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}
    return request_api(url, get_headers("FHKST01010100"), params)


def fetch_finance_data(ticker):
    """ 재무 비율 조회 """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/finance/financial-ratio"
    params = {"FID_DIV_CLS_CODE": "0", "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}
    return request_api(url, get_headers("FHKST66430300"), params)


def fetch_income_data(ticker):
    """ 손익계산서 조회 """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/finance/income-statement"
    params = {"FID_DIV_CLS_CODE": "0", "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}
    return request_api(url, get_headers("FHKST66430200"), params)


def fetch_stability_data(ticker):
    """ 안정성 비율 조회 """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/finance/stability-ratio"
    params = {"FID_DIV_CLS_CODE": "0", "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}
    return request_api(url, get_headers("FHKST66430600"), params)


def fetch_stock_data(ticker):
    """ 개별 종목 데이터 수집 """
    price_data = fetch_price_data(ticker) or {}
    finance_data = fetch_finance_data(ticker) or [{}]
    income_data = fetch_income_data(ticker) or [{}]
    stability_data = fetch_stability_data(ticker) or [{}]

    return {
        "ticker": ticker,
        "sale_account": income_data[0].get("sale_account"),
        "bsop_prti": income_data[0].get("bsop_prti"),
        "thtr_ntin": income_data[0].get("thtr_ntin"),
        "roe_val": finance_data[0].get("roe_val"),
        "eps": price_data.get("eps"),
        "per": price_data.get("per"),
        "pbr": price_data.get("pbr"),
        "grs": finance_data[0].get("grs"),
        "bsop_prfi_inrt": finance_data[0].get("bsop_prfi_inrt"),
        "ntin_inrt": finance_data[0].get("ntin_inrt"),
        "lblt_rate": stability_data[0].get("lblt_rate"),
        "crnt_rate": stability_data[0].get("crnt_rate"),
        "sps": finance_data[0].get("sps"),
        "bps": price_data.get("bps"),
        "foreigner_ratio": price_data.get("hts_frgn_ehrt"),
    }


def fetch_all_stocks(stock_info_df):
    """ 여러 종목 데이터 수집 """
    stock_data = []
    for _, row in tqdm(stock_info_df.iterrows(), total=stock_info_df.shape[0], desc="Fetching Stock Data",
                       disable=disable_tqdm):
        stock_data.append(fetch_stock_data(row["ticker"]))
        time.sleep(0.3)
    new_data_df = pd.DataFrame(stock_data)
    merged_df = stock_info_df.merge(new_data_df, on="ticker", how="left")
    return merged_df


# # CSV 파일에서 종목 리스트 가져오기
# df = pd.read_csv("./data/stock_info.csv", dtype={"ticker": str})
# df = fetch_all_stocks(df)
# # 결과 저장
# df.to_csv("data/stock_info_detail.csv", index=False, encoding="utf-8-sig")
#
# print("✅ 데이터 수집 완료: data/stock_info_detail.csv 저장 완료")
