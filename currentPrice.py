import requests
import redis
import json

# Redis 연결 설정 (Docker에서 실행 중인 경우)
redis_client = redis.StrictRedis(host='localhost', port=6479, db=0, decode_responses=True)

def fetch_sector_data(market):
    """지정된 시장의 섹터별 주식 데이터를 가져오는 함수."""
    api_url = f"https://finance.daum.net/api/quotes/sectors?fieldName=&order=&perPage=&market={market}&page=&changes=UPPER_LIMIT,RISE,EVEN,FALL,LOWER_LIMIT"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': f'https://finance.daum.net/domestic/all_stocks?market={market}',
        'X-Requested-With': 'XMLHttpRequest',
    }
    
    response = requests.get(api_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()  # JSON 형태로 응답받기
        stocks_data = []

        # 데이터 구조에 맞게 정보 추출
        for sector in data.get('data', []):  # 'data' 키에서 값을 가져옴
            included_stocks = sector.get('includedStocks', [])  # 'includedStocks' 키에서 값을 가져옴
            for stock in included_stocks:
                # 'symbolCode'에서 'A' 제거
                ticker = stock.get('symbolCode', '')
                if ticker.startswith('A'):
                    ticker = ticker[1:]  # 'A' 제거

                stock_info = {
                    "ticker": ticker,  # 수정된 종목 코드
                    "name": stock.get('name'),  # 종목 이름
                    "currentPrice": stock.get('tradePrice'),  # 현재가
                    "changeRate": stock.get('changeRate'),  # 등락율
                    "changeAmount": stock.get('changePrice'),  # 등락금액
                    "marketCap": stock.get('marketCap'),  # 시가총액
                    "accTradeVolume": stock.get('accTradeVolume'),
                    "foreignRatio": stock.get('foreignRatio')  # 외국인 비율

                }
                stocks_data.append(stock_info)

        return stocks_data
    else:
        print(f"API 호출 실패: {response.status_code}")
        print(f"응답 내용: {response.content}")
        return []

