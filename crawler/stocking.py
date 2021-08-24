import sys
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
# from tabulate import tabulate as tb

export_path = "D:\project\workspace\cdp\etl\stocking\excel"
yyyymmdd = datetime.today().strftime("%Y%m%d")

# Web Crawling Data Gathering
def getCrawlingData(url):
    req = requests.get(url, verify=False)
    soup = bs(req.content, "lxml")

    # print(soup)

    # table 데이터 추출
    table = str(soup.find("table", {"class":"type_2"}))

    # 그리드 정보 저장
    df = pd.read_html(table)[0]

    # 데이터프레임 garbage 행 삭제 & 형 변환
    df = df.dropna(subset=["N"])
    df["N"] = pd.to_numeric(df["N"], downcast="signed")

    # 추가 속성 추출
    html = bs(table, "html.parser")
    link_html = html.find_all("a", {"class":"tltle"})
    codes = []

    # 데이터 파싱
    for link in link_html:
        codes.append(str(link["href"]).replace("/item/main.nhn?code=", ""))

    # 데이터프레임에 add
    df.insert(1, "종목코드", codes)

    return df

# Quant 알고리즘 적용
def getQuantStock(df: pd.DataFrame()) -> pd.DataFrame():
    quant_condition = {
        "현재가": "1000+",
        "보통주배당금": "0+",
        "영업이익증가율": "5+",
        "PER": "25-",
        "ROE": "5+",
        "PBR": "10-",
        "매출액증가율": "5+",
        "유보율": "10+",
        "부채비율": "100-",
        "시총대비거래율": "5+",
        "전일대비거래율": "0+",
        "변환등락율": "0.5+"
    }

    for col in df:
        if quant_condition.get(col) is not None:
            upDown = quant_condition.get(col)[-1]
            upDownValue = float(quant_condition.get(col)[0:len(quant_condition.get(col))-1])

            if upDown == "+":
                df = df[df[col] >= upDownValue]
            else:
                df = df[df[col] <= upDownValue]

    return df

# 문자열 치환
def getStrTokenizer(df, token):
    return_df = []

    for raw in df:
        for token_char in token:
            raw = raw.replace(token_char, "")
        return_df.append(raw)
    return return_df

# pd.DataFrame().to_excel()

# excel export
def exportToExcel(fileName, sheetNames, dfs):

    fullFilePath = f"{export_path}\{fileName}"

    with pd.ExcelWriter(fullFilePath) as writer:
        cnt = 0
        for sheetName in sheetNames:
            dfs[cnt].to_excel(writer,
                              sheet_name=sheetName, 
                              index=False,
                              startcol=1,
                              startrow=1
                              )
            cnt += 1

# 예외사항 : 은행/금융/증권 업종은 부채비율이 높으므로 강제 변환 (99)
def func(row):

    except_stock_names = ["은행", "증권", "보험", "금융", "종금", "캐피탈"]
    stock_name = row["종목명"]
    for except_stock_name in except_stock_names:
        if except_stock_name in stock_name:
            return 99
    return row["부채비율"]

def main() -> int:
    # crawling
    # 코스피 거래량 상위 100선 (1회 조회 팩트 제한 7개 -> 2회 호출)
    urls = ["https://finance.naver.com/sise/field_submit.nhn?menu=quant&returnUrl=http%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_quant.nhn&fieldIds=amount&fieldIds=per&fieldIds=operating_profit_increasing_rate&fieldIds=roe&fieldIds=debt_total&fieldIds=pbr&fieldIds=dividend"
           ,"https://finance.naver.com/sise/field_submit.nhn?menu=quant&returnUrl=http%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_quant.nhn&fieldIds=quant&fieldIds=market_sum&fieldIds=prev_quant&fieldIds=property_total&fieldIds=listed_stock_cnt&fieldIds=sales_increasing_rate&fieldIds=reserve_ratio"
    ]

    df = pd.DataFrame()

    for url in urls:
        temp_df = getCrawlingData(url)

        for col in temp_df.columns:
            try:
                df.insert(len(df.columns), col, temp_df[col])
            except Exception as e:
                continue

    # 계산 필드 추가 & 파싱
    df.insert(len(df.columns), "부채비율", df["부채총계"]/df["자산총계"]*100.0)
    df.insert(len(df.columns), "시총대비거래율", df["거래대금"]/df["시가총액"]*100.0)
    df.insert(len(df.columns), "전일대비거래율", df["거래량"]/df["전일거래량"]*100.0)

    df["부채비율"] = df.apply(func, axis=1)

    # 문자열 치환 후 추가
    temp_df = getStrTokenizer(df["등락률"], "{\+-%}")
    df.insert(len(df.columns), "변환등락율", pd.to_numeric(temp_df))

    initial_df = df

    # 불필요 필드 삭제
    del_cols = ["전일비", "등락률", "부채총계", "자산총계", "거래대금", "시가총액", "거래량", "전일거래량", "상장주식수"]
    df = df.drop(columns=del_cols, axis=1)

    # 데이터 정리 : type, Nan to 0, 비우량주 삭제, float to int
    df["현재가"] = df["현재가"].astype(int)
    df["보통주배당금"] = df["보통주배당금"].fillna(0)
    df = df.dropna(subset=["PER", "PBR", "ROE"])

    for col in df.columns:
        if df[col].dtype == "float64" and col != "변환등락율":
            df[col] = df[col].round(0).astype(int)

    final_df = getQuantStock(df)

    # excel export
    exportToExcel(f"{yyyymmdd}_stock top 100_by trade amount.xlsx", ["stock_list", "recommend"], [initial_df, final_df])

    # print(tb(final_df, headers="keys", tablefmt="pretty"))

    # print(final_df["종목코드"])

    # api 연동

    # 사전준비 : API 서비스 사용신청 -> OpenAPI+ 모듈 다운로드 및 설치

    # login

    # balance check

    # 호가 check

    # 기대수익률 & 안정성 등 투자성향을 고려한 매수/매도 주문

if __name__ == "__main__":

    sys.exit(main())
