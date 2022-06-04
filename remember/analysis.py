import pandas as pd
from datetime import timedelta

def merge_data():
    # success load : 44295
    df_success = pd.read_csv('./data/event_login_success.csv', names=['event_time', 'os', 'app_version', 'source', 'device_id', 'user_id'])
    df_success['success_yn'] = 'Y'

    # fail load : 145203
    df_fail = pd.read_csv('./data/event_login_fail.csv', names=['event_time', 'os', 'app_version', 'source', 'device_id'])
    df_fail['success_yn'] = 'N'

    # user info merge (device 정보 + success 정보 => distinct)
    df_success_device = pd.DataFrame(df_success[['user_id', 'device_id']]) # 44295
    df_device = pd.read_csv('./data/user_device.csv', names=['user_id', 'device_id', 'os']) # 125341
    df_total_device = pd.concat([df_success_device, df_device[['user_id', 'device_id']]], ignore_index=True).drop_duplicates(subset='device_id') # 125694
    
    # fail 사용자 device_info 기준으로 user_id 매핑 (user_id 비식별시, device_id로 대체)
    df_fail = pd.merge(df_fail, df_total_device, how='left', on='device_id')
    df_fail['user_id'] = df_fail['user_id'].fillna(0)
    df_fail['user_id'] = df_fail['user_id'].astype(int)
    df_fail.loc[df_fail['user_id'] == 0, 'user_id'] = df_fail['device_id']

    # data 통합
    df_total_login = pd.concat([df_success, df_fail], ignore_index=True)

    # 가입일 추가, KST 변환
    df_join = pd.read_csv('./data/user_join.csv', names=['user_id', 'joined_at']) # 41619
    df_join['joined_at'] = df_join['joined_at'].astype(str)
    df_join['joined_at'] = pd.to_datetime(df_join['joined_at'], format='%Y-%m-%d %H:%M:%S')
    df_join['joined_at'] = df_join['joined_at'] + timedelta(hours=9)

    df_total_login = pd.merge(df_total_login, df_join, how='left', on='user_id')
    
    # 날짜 비교 형변환 : YYYY-MM-DD
    df_total_login['joined_at'] = df_total_login['joined_at'].astype(str)
    df_total_login['joined_at'] = df_total_login['joined_at'].str.split(' ').str[0]
    df_total_login['event_time'] = df_total_login['event_time'].str.split(' ').str[0]
    df_total_login['event_time'] = df_total_login['event_time'].astype(str)
    df_total_login.loc[df_total_login['event_time'].str.find('.') == 4, 'event_time'] = df_total_login['event_time'].str[:4] + '-' + df_total_login['event_time'].str.split('.').str[1].str.zfill(2) + '-' + df_total_login['event_time'].str.split('.').str[2].str.zfill(2)

    # 가입일 로그 삭제
    idx = df_total_login[df_total_login['event_time'] == df_total_login['joined_at']].index
    df_total_login.drop(idx, inplace=True)
    
    # 업데이트 전후 표기
    df_total_login.loc[((df_total_login['os'] == 'android') & (df_total_login['app_version'] >= 147) & (df_total_login['event_time'] >= '2018-07-02'))
                      |((df_total_login['os'] == 'ios') & (df_total_login['app_version'] >= 158) & (df_total_login['event_time'] >= '2018-07-02')), 'update'] = 'after'
    df_total_login['update'] = df_total_login['update'].fillna('before')

    return df_total_login

def extract_data(df: pd.DataFrame()) -> pd.DataFrame():
    """
    [요청 항목]
    아래 데이터를 일별, 업데이트 전과 후, OS를 구분하여 데이터 추출 요청합니다.
    - 로그인 시도한 유저 수 중 로그인 성공한 유저 수
    - 로그인 시도 횟수 대비 로그인 성공 수
    - 로그인 방법별(네이버, 구글, 페이스북, 이메일) 로그인 시도 횟수 대비 로그인 성공 건 수
    """
    group = df.groupby(['event_time','update','os'])
    # 로그인 시도한 유저 수 중 로그인 성공한 유저 수 : 1_login_user_cnt.csv
    try_user = group.agg({'user_id':'nunique'}).reset_index()
    try_user.rename(columns={'user_id':'try_cnt'}, inplace=True)
    success_user = df.loc[df['success_yn'] == 'Y'].groupby(['event_time','update','os']).agg({'user_id':'nunique'}).reset_index()
    success_user.rename(columns={'user_id':'success_cnt'}, inplace=True)
    result = pd.merge(try_user, success_user, how='left', on=['event_time','update','os'])
    result.to_csv('./output/1_login_user_cnt.csv', sep=',', na_rep=0)

    # 로그인 시도 횟수 대비 로그인 성공 수 : 2_login_cnt.csv
    try_user = pd.DataFrame(group.size())
    try_user.rename(columns={0:'try_cnt'}, inplace=True)
    success_user = pd.DataFrame(df.loc[df['success_yn'] == 'Y'].groupby(['event_time','update','os']).size())
    success_user.rename(columns={0:'success_cnt'}, inplace=True)
    result = pd.merge(try_user, success_user, how='left', on=['event_time','update','os'])
    result.to_csv('./output/2_login_cnt.csv', sep=',', na_rep=0)

    # 로그인 방법별(네이버, 구글, 페이스북, 이메일) 로그인 시도 횟수 대비 로그인 성공 건 수 : 3_login_source_cnt.csv
    group = df.groupby(['event_time','update','os','source'])
    try_user = pd.DataFrame(group.size())
    try_user.rename(columns={0:'try_cnt'}, inplace=True)
    success_user = pd.DataFrame(df.loc[df['success_yn'] == 'Y'].groupby(['event_time','update','os','source']).size())
    success_user.rename(columns={0:'success_cnt'}, inplace=True)
    result = pd.merge(try_user, success_user, how='left', on=['event_time','update','os','source'])
    result.to_csv('./output/3_login_source_cnt.csv', sep=',', na_rep=0)

def main():
    df = merge_data()
    extract_data(df)

if __name__ == '__main__':
    main()