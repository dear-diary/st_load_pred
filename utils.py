import pandas as pd
from datetime import datetime, timedelta, date


# df数据行转列
def df_trans(df):
    df_melted = df.melt(
        id_vars=['read_date'],
        value_vars=[f'hour{i}' for i in range(1, 25)],
        var_name='timeslot',
        value_name='load'
    )
    df_melted['hour'] = df_melted['timeslot'].str.extract(r'hour(\d+)').astype(int) - 1
    df_melted['time'] = pd.to_datetime(df_melted['read_date']) + pd.to_timedelta(df_melted['hour'], unit='h')
    df_melted = df_melted[['time', 'load']].sort_values('time').reset_index(drop=True)
    return df_melted


# 数据列转行
def df_restore(df):
    df['date'] = df['time'].dt.strftime('%Y-%m-%d')
    df['hour'] = df['time'].dt.hour + 1
    df_restored = (
        df.pivot(index='date', columns='hour', values='load')
        .rename(columns=lambda x: f'hour{x}')
        .reset_index()
        .rename(columns={'date': 'read_date'})
    )
    df_restored = df_restored[['read_date'] + [f'hour{i}' for i in range(1, 25)]]
    df_restored = df_restored.sort_values('read_date').reset_index(drop=True)
    return df_restored


# 获取给定起始、结束时间字符串之间的日期字符串列表
def get_date_list(startDate, endDate):
    # 将字符串转换为datetime对象
    start_time = datetime.strptime(startDate, "%Y-%m-%d")
    end_time = datetime.strptime(endDate, "%Y-%m-%d")
    # 生成时间列表
    time_list = []
    current_time = start_time
    while current_time <= end_time:
        time_list.append(current_time.strftime("%Y-%m-%d"))
        current_time += timedelta(days=1)
    return time_list


# 获取小列表中不存在，但在大列表中存的值
def get_unique_elements(big_list, small_list):
    if len(small_list) == 0:
        return []
    small_set = set(small_list)
    return [x for x in big_list if x not in small_set]


# 获取两个日期的间隔
def get_date_interval(before_date, after_date):
    if isinstance(before_date, str):
        before_date_object = datetime.strptime(before_date, '%Y-%m-%d')
    else:
        before_date_object = before_date
    if isinstance(after_date, str):
        after_date_object = datetime.strptime(after_date, '%Y-%m-%d')
    else:
        after_date_object = after_date
    return (after_date_object - before_date_object).days


# 获取前k个的最大score的index
def get_top_K_index(lst, n):
    sorted_indices = sorted(range(len(lst)), key=lambda i: lst[i], reverse=True)
    return sorted_indices[:n]


# 获取指定日期间隔的日期
def get_delta_date(delta_day):
    today = date.today()
    current_date = today + timedelta(days=delta_day)
    current_date = datetime.strftime(current_date, '%Y-%m-%d')
    return current_date
