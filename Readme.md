# 깃 클론 
git clone https://github.com/PDA-5th-Team5/data-pipline.git
# 가상환경 생성
python3 -m venv myenv
# 가상환경 실행 
source myenv/bin/activate
# 라이브러리 설치 
pip install -r requirements.txt  # 다른 환경에서 설치

# 환경 변수 설정 
nano .env 
```shell
# 한국 투자증권 api 실제₩계좌 key, scretkey
APP_KEY=
APP_SECRET=
# 한국 투자증권 api 실제 계좌 번호
ACC=
ACC_NO=

# mysql 연결 정보
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
DB_NAME=

# redis 연결 정보
REDIS_HOST=
REDIS_PORT=
REDIS_DB=
```

# 스케줄링 설정 
crontab -l

crontab -e

# 매일 월~금 16:00 (오후 4시) 실행 → update_day
0 16 * * 1-5 /home/user/data-pipline/myenv/bin/python /home/user/data-pipline/update_tasks.py >> /home/user/logs/update_day.log 2>&1

# 장중(월~금 09:00~16:00) 1분마다 실행 → update_1m
* 9-16 * * 1-5 /home/user/data-pipline/myenv/bin/python /home/user/data-pipline/update_redis_1m.py >> /home/user/logs/update_redis_1m.log 2>&1

# 매일 월~금 16:10 (오후 4시 10분) 실행 → update_1d
10 16 * * 1-5 /home/user/data-pipline/myenv/bin/python /home/user/data-pipline/update_redis_1d.py >> /home/user/logs/update_redis_1d.log 2>&1


