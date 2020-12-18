"""
Desc:네이버 영화랭킹 평점순(모든영화) 1위 ~ 1000위
"""
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import mysql.connector

ranking = 1000
today = datetime.now().strftime('%Y%m%d')
defaultUrl = 'https://movie.naver.com/movie/sdb/rank/rmovie.nhn?sel=pnt&date=' + today
url = []
val = []

# 1page 당 50개의 ranking 조회. 총 페이지수 = ranking/50
for i in range(1, int(ranking/50) + 1, 1):
    if i == 1:
        url.append(defaultUrl)
    else:
        # 2page 부터 추가 url : &page=2
        url.append(defaultUrl + '&page=' + str(i))

# tbody tag 하위 tr 태그에 1개의 ranking 데이터 존재
i = 0
for pageUrl in url:
    src = requests.get(pageUrl).text
    soup = BeautifulSoup(src, 'html.parser')
    keys = soup.select('tbody tr')

    for key in keys:
        if key.select_one('.tit5'):
            i += 1
            val.append(
                (today,
                 i,
                 key.select_one('.tit5').get_text().replace('\n', ''),
                 float(key.select_one('.point').get_text()))
            )
            # print(val[i-1])
        if i >= ranking:
            break

# connection info : 설정 json 파일의 connection 정보 활용
with open('../config/connector.json', 'r') as f:
    config = json.load(f)

conn = mysql.connector.connect(
    host=config['mysql']['host'],
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database=config['mysql']['database']
)

try:
    with conn.cursor() as cursor:
        sql = 'DELETE FROM NAVER_MOVIE_RANK_BY_RATING WHERE YYYYMMDD = %s'
        cursor.execute(sql, (today,))
    conn.commit()

    with conn.cursor() as cursor:
        sql = '''INSERT INTO NAVER_MOVIE_RANK_BY_RATING (YYYYMMDD, RANKING, MOVIE_NM, RATING, LOAD_DTTM) 
                                                 VALUES (%s, %s, %s, %s, NOW())'''
        cursor.executemany(sql, val)
    conn.commit()
    print(cursor.rowcount, 'record was inserted')

finally:
    conn.close()
