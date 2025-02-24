import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# .env 파일 로드
load_dotenv()

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
TOKEN_FILE = ".env"  # .env 파일에서 관리

# .env 파일에 토큰과 만료 시간을 저장
def save_token_to_env(token, expire_time):
    with open(TOKEN_FILE, "a") as file:
        file.write(f"TOKEN={token}\n")
        file.write(f"TOKEN_EXPIRE_TIME={expire_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

# .env 파일에서 토큰과 만료 시간을 로드
def load_token_from_env():
    token = os.getenv("TOKEN")
    expire_time_str = os.getenv("TOKEN_EXPIRE_TIME")
    if expire_time_str:
        expire_time = datetime.strptime(expire_time_str, "%Y-%m-%d %H:%M:%S")
    else:
        expire_time = None
    return token, expire_time


# 새로운 토큰 요청
def request_new_token():
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    payload = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        token = response_data.get("access_token")
        expire_time_str = response_data.get("access_token_token_expired")

        # 만료 시간을 datetime으로 변환
        expire_time = datetime.strptime(expire_time_str, "%Y-%m-%d %H:%M:%S")

        save_token_to_env(token, expire_time)
        return token
    else:
        print(f"Error {response.status_code}: {response.json()}")
        return None


# 유효한 토큰 반환
def get_access_token():
    token, expire_time = load_token_from_env()
    if token and expire_time and expire_time > datetime.now():
        print("Using cached token.")
        return token
    print("Token expired or not found. Requesting new token...")
    return request_new_token()

# Example usage
if __name__ == "__main__":
    access_token = get_access_token()
    if access_token:
        print("Access token retrieved:", access_token)
    else:
        print("Failed to retrieve access token.")
