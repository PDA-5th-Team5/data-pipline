import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
disable_tqdm = os.getenv("DISABLE_TQDM", "false").lower() == "true"

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

krx_url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

def 시가총액_숫자변환(market_cap_str):
    market_cap_str = market_cap_str.replace(' ', '').replace('\t', '').replace(',', '')
    billions = 0
    trillions = 0

    if '조' in market_cap_str:
        trillions_part = market_cap_str.split('조')[0]
        trillions = int(trillions_part) * 10000

    billions_part = market_cap_str.split('억원')[0].split('조')[-1]
    if billions_part:
        billions = int(billions_part)

    return trillions + billions

def KRX_주식_종목명_티커_크롤러():
    response = requests.get(krx_url)
    response.encoding = "euc-kr"
    tables = pd.read_html(StringIO(response.text))
    listed_companies_df = tables[0][["종목코드", "회사명"]].copy()
    listed_companies_df["종목코드"] = listed_companies_df["종목코드"].astype(str).str.zfill(6)
    return listed_companies_df

def 네이버_기업정보_크롤링(df):
    result_list = []

    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing tickers", disable=disable_tqdm):
        ticker = row["종목코드"]
        company_name = row["회사명"]
        naver_url = f"http://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(naver_url, headers=headers)
        html = response.content.decode('euc-kr', 'replace')
        soup = BeautifulSoup(html, 'html.parser')

        company_summary_element = soup.select_one('div.summary_info')
        try :
            company_summary_text = ' '.join([p.get_text(strip=True) for p in company_summary_element.find_all('p')])
        except:
            company_summary_text = None

        market_cap_element = soup.select_one('tr:has(th:contains("시가총액")) td em')
        try:
            market_cap_text = market_cap_element.text.strip().replace(',', '')
        except:
            market_cap_text = None


        sector_element = soup.select_one('div.section.trade_compare h4 em a')
        try:
            sector_value = sector_element.text.strip()
        except:
            sector_value = None

        def safe_float(element):
            return float(element.text.strip().replace(',', '')) if element and element.text.strip() else None

        # per_value = safe_float(soup.select_one('#_per'))
        # eps_value = safe_float(soup.select_one('#_eps'))
        # bps_value = safe_float(soup.select_one('#_bps'))
        # pbr_value = safe_float(soup.select_one('#_pbr'))
        dividend_yield = safe_float(soup.select_one('#_dvr'))
        # sector_average_per = safe_float(soup.select_one('tab_con1 div table tbody tr td em'))

        market_type_text = None
        for dd in soup.select('dl.blind dd'):
            if "코스피" in dd.text:
                market_type_text = "KOSPI"
                break
            elif "코스닥" in dd.text:
                market_type_text = "KOSDAQ"
                break

        result_list.append({
            "ticker": ticker,
            "company_name": company_name,
            "company_overview": company_summary_text,
            "market_cap": 시가총액_숫자변환(market_cap_text),
            "market_type": market_type_text,
            "sector": sector_value,
            # "per": per_value,
            # "eps": eps_value,
            # "bps": bps_value,
            # "pbr": pbr_value,
            "dividend_yield": dividend_yield,
            # "sector_average_per": sector_average_per
        })

    df = pd.DataFrame(result_list)
    df = df[~df["market_type"].isin([None, "", " ", "정보 없음"])] # 코넥스 종목 제거
    df = df.dropna(subset=["market_type"])

    return df

# krx_df = KRX_주식_종목명_티커_크롤러()
# # 13 분 소요
# 기업정보_df = 네이버_기업정보_크롤링(krx_df)
#
# 기업정보_df.to_csv("data/stock_info.csv", index=False, encoding="utf-8-sig")
#
# print(기업정보_df)
