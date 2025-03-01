# 깃 클론 
git clone https://github.com/PDA-5th-Team5/data-pipline.git
# 가상환경 생성
python3 -m venv myenv
# 가상환경 실행 
source myenv/bin/activate
# 라이브러리 설치 
pip install -r requirements.txt  # 다른 환경에서 설치

# 스케줄링 설정 
crontab -e

# 매일 월~금 16:00 (오후 4시) 실행 → update_day
0 18 * * 1-5 /home/user/data-pipline/myenv/bin/python /home/user/data-pipline/update_tasks.py day >> /home/user/logs/update_day.log 2>&1
# 장중(월~금 09:00~18:00) 1~~분마다 실행 → update_1m
* 9-17 * * 1-5 /home/user/data-pipline/myenv/bin/python /home/user/data-pipline/update_tasks.py 1m >> /home/user/logs/update_1m.log 2>&1


