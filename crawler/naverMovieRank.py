'''
pip install bs4
pip install requests

네이버 영화랭킹 평점순(모든영화) 1위 ~ 1000위
'''
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

for i in range(1, int(ranking/50) + 1, 1):
    if i == 1:
        url.append(defaultUrl)
    else:
        url.append(defaultUrl + '&page=' + str(i))

i = 0
for pageUrl in url:
    src = requests.get(pageUrl).text
    soup = BeautifulSoup(src, 'html.parser')
    keys = soup.select('tbody tr')

    for key in keys:
        if key.select_one('.tit5'):
            i += 1
            val.append((today, i, key.select_one('.tit5').get_text().replace('\n', ''), float(key.select_one('.point').get_text())))
            # print(val[i-1])
        if i >= ranking:
            break

# connection info
with open('../config/connector.json', 'r') as f:
    config = json.load(f)
host = config['mysql']['host']
user = config['mysql']['user']
password = config['mysql']['password']
database = config['mysql']['database']

conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

sql = 'INSERT INTO NAVER_MOVIE_RANK_BY_RATING (YYYYMMDD, RANKING, MOVIE_NM, RATING) VALUES (%s, %s, %s, %s)'

cursor.executemany(sql, val)
conn.commit()
conn.close()

print(cursor.rowcount, ' record was inserted')
