import http.client
import json
import urllib
from datetime import datetime

import pandas as pd

# 文件位置
file_basic_path = 'holidays/'

# 接口
key = '1aa19366cbf636168e5337ba68f8f060'
data_type = 1


# 接口外网获取指定年份的节假日
def get_holiday(year):
    conn = http.client.HTTPSConnection('apis.tianapi.com')
    params = urllib.parse.urlencode({'key': key, 'date': str(year), 'type': data_type})
    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    conn.request('POST', '/jiejiari/index', params, headers)
    tianapi = conn.getresponse()
    result = tianapi.read()
    data = result.decode('utf-8')
    dict_data = json.loads(data)
    if dict_data['code'] == 200:
        data = dict_data['result']['list']
        df = pd.DataFrame(data)
        holiday_df = df.iloc[:, :4]
        df_name = str(year) + '_holiday.csv'
        holiday_df.to_csv(file_basic_path + df_name)
        return holiday_df
    else:
        return None


# 获取年份的节假日set
def get_year_holiday_set(year):
    holiday_set = set()
    df = pd.read_csv(file_basic_path + str(year) + '_holiday.csv')
    holiday_list = df['vacation'].tolist()
    for holiday_str in holiday_list:
        if not pd.isna(holiday_str):
            holidays = holiday_str.split('|')
            holiday_set.update(holidays)
    return holiday_set


# 获取年份的调休日set
def get_year_remark_set(year):
    remark_set = set()
    df = pd.read_csv(file_basic_path + str(year) + '_holiday.csv')
    remark_list = df['remark'].tolist()
    for remark_str in remark_list:
        if not pd.isna(remark_str):
            remarks = remark_str.split('|')
            remark_set.update(remarks)
    return remark_set


def get_holiday_info(date):
    holiday_dict = {}
    if isinstance(date, str):
        date_object = datetime.strptime(date, '%Y-%m-%d')
    else:
        date_object = date
    year = date_object.year
    df = pd.read_csv(file_basic_path + str(year) + '_holiday.csv')
    for index, row in df.iterrows():
        date_list = row['vacation'].split('|')
        if date in date_list:
            holiday_dict['name'] = row['name']
            holiday_dict['year'] = year
            holiday_dict['order'] = date_list.index(date)
            return holiday_dict

    next_df = pd.read_csv(file_basic_path + str(year+1) + '_holiday.csv')
    for index, row in next_df.iterrows():
        date_list = row['vacation'].split('|')
        if date in date_list:
            holiday_dict['name'] = row['name']
            holiday_dict['year'] = year+1
            holiday_dict['order'] = date_list.index(date)
            return holiday_dict
    return None


def get_holiday_date_list(year, holiday_name):
    df = pd.read_csv(file_basic_path + str(year) + '_holiday.csv')
    filter_df = df[df['name'] == holiday_name]
    date_list = filter_df.iloc[0]['vacation']
    return date_list.split('|')
