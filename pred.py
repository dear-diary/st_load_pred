import pandas as pd
from datetime import datetime
from holiday import get_year_holiday_set, get_year_remark_set, get_holiday_info, get_holiday_date_list
from utils import get_delta_date, get_date_interval, get_top_K_index, df_trans
from sqlite_operation import get_sqlite_conn, close_sqlite_conn, query_load, save_pred_data


# 日类型
WORKDAY = 0  # 工作日
WEEKEND = 1  # 周末
HOLIDAY = 3  # 节假日

# 相似日配置参数
delta_day = 0  # 当前天
history_days = 365  # 历史天数
similar_days = 3  # 相似日天数


# 获取日期的日类型
def get_date_type(date):
    date_type = WORKDAY
    if isinstance(date, str):
        date_object = datetime.strptime(date, '%Y-%m-%d')
    else:
        date_object = date
    day_of_week = date_object.weekday()

    # 判断是否是周末
    if day_of_week == 5 or day_of_week == 6:
        date_type = WEEKEND

    # 判断是否是节假日
    year = date_object.year
    holiday_set = get_year_holiday_set(year)
    remark_set = get_year_remark_set(year)

    month = date_object.month
    day = date_object.day
    if month == 12 and (day == 30 or day == 31):  # 考虑到元旦跨年
        next_holiday_set = get_year_holiday_set(year + 1)
        next_remark_set = get_year_remark_set(year + 1)
        holiday_set = holiday_set.update(next_holiday_set)
        remark_set = remark_set.update(next_remark_set)

    date_str = datetime.strftime(date_object, '%Y-%m-%d')
    if date_str in holiday_set:
        date_type = HOLIDAY
    if date_str in remark_set:
        date_type = WORKDAY
    return date_type


# 获取日期的周类型
def get_date_week(date):
    if isinstance(date, str):
        date_object = datetime.strptime(date, '%Y-%m-%d')
    else:
        date_object = date
    day_of_week = date_object.weekday()
    return day_of_week


def similar_day_forecast(custom_name, forecast_days):
    forecasted_days = 0
    day_index = 1

    conn = get_sqlite_conn()
    while forecasted_days < forecast_days:  # while遍历预测天数
        forecast_date = get_delta_date(delta_day + day_index)
        date_type = get_date_type(forecast_date)
        if date_type == WORKDAY:  # 如果不是工作日那就跳过，也就是要预测forecast_days天数的工作日
            forecasted_days += 1
        if date_type != HOLIDAY:  # 工作日的预测逻辑
            forecast_df = normal_forecast(forecast_date, custom_name)
        else:  # TODO 节假日的预测逻辑有问题
            forecast_df = holiday_forecast(forecast_date, custom_name)
        if forecast_df is not None:
            forecast_df['custom_name'] = custom_name
            forecast_df['pred_value'] = forecast_df['pred_value'].round(2)
            save_pred_data(forecast_df, conn)  # 数据插入数据库
        day_index += 1
    close_sqlite_conn(conn)


# 获取历史数据
def get_history_data(custom_name, his_start, his_end):
    conn = get_sqlite_conn()
    his_df = query_load(his_start, his_end, custom_name, conn, 'fit_load')
    his_df = df_trans(his_df)
    his_df = his_df.rename(columns={'time': 'date_time', 'load': 'energy'})
    his_df['date_time'] = his_df['date_time'].astype(str)
    return his_df


def holiday_forecast(forecast_date, custom_name):
    holiday_dict = get_holiday_info(forecast_date)
    his_years = 2
    if holiday_dict is not None:
        order = holiday_dict['order']
        forecast_df = pd.DataFrame(data={'pred_date': [forecast_date] * 24, 'pred_time': common_time_list()})
        energy_lists = []
        # 以历史两年数据为基础
        for i in range(1, his_years+1):
            before_year = holiday_dict['year'] - i
            holiday_date_list = get_holiday_date_list(before_year, holiday_dict['name'])
            if len(holiday_date_list)-1 > order:
                order_date = holiday_date_list[order]
            else:
                order_date = holiday_date_list[-1]
            his_df = get_history_data(custom_name, order_date, order_date)
            if his_df is not None:
                energy_lists.append(his_df['energy'])
        mean_list = [sum(elements) / len(energy_lists) for elements in zip(*energy_lists)]
        forecast_df['pred_value'] = mean_list
        return forecast_df
    return None


# 一般天的预测
def normal_forecast(forecast_date, custom_name):
    # 获取历史数据
    his_start = get_delta_date(-history_days + delta_day)
    his_end = get_delta_date(delta_day - 1)
    his_df = get_history_data(custom_name, his_start, his_end)
    if his_df.empty:
        return None
    his_df[['date', 'time']] = his_df['date_time'].str.split(' ', expand=True)
    his_df = his_df.drop(columns=['date_time'])

    # 获取日类型
    date_list = his_df['date'].unique().tolist()
    forecast_date_type = get_date_type(forecast_date)
    forecast_date_week = get_date_week(forecast_date)

    # 获得分数list
    score_list = []
    for date in date_list:
        his_date_type = get_date_type(date)
        his_date_week = get_date_week(date)
        date_interval = get_date_interval(date, forecast_date)  # 日期间隔
        score = count_score(forecast_date_type, his_date_type, date_interval) + count_week_score(forecast_date_week, his_date_week)
        score_list.append(score)
    index_list = get_top_K_index(score_list, similar_days)  # 获取前几个分数index

    # 预测
    forecast_df = pd.DataFrame(data={'pred_date': [forecast_date]*24, 'pred_time': common_time_list()})
    energy_lists = []
    for index in index_list:
        part_df = his_df[his_df['date'] == date_list[index]]
        part_value = part_df['energy'].tolist()
        energy_lists.append(part_value)
    mean_list = [sum(elements) / len(energy_lists) for elements in zip(*energy_lists)]
    forecast_df['pred_value'] = mean_list
    return forecast_df


# 计算分数
def count_score(forecast_date_type, his_date_type, date_interval):
    base = 0.98
    interval_score = base ** date_interval
    day_type_score = 3 - abs(forecast_date_type - his_date_type)
    return interval_score + day_type_score


def count_week_score(forecast_date_week, his_date_week):
    base = 1
    if forecast_date_week == his_date_week:
        return base
    else:
        return 0


# 获取通用time_list
def common_time_list():
    time_list = []
    for i in range(0, 24):
        time = str(i).zfill(2) + ':00'
        time_list.append(time)
    return time_list
