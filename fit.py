import numpy as np
import pandas as pd
import copy
from datetime import datetime, timedelta
from utils import df_trans, df_restore, get_date_list, get_unique_elements


def fill(df, start_date_str, end_date_str):
    date_column = 'read_date'
    all_date_list = get_date_list(start_date_str, end_date_str)
    df_date_list = df[date_column].unique().tolist()
    missing_date_list = get_unique_elements(all_date_list, df_date_list)
    df[date_column] = pd.to_datetime(df[date_column], format='%Y-%m-%d')
    if len(missing_date_list) > 0:  # 处理丢失日期的数据
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        for missing_date_str in missing_date_list:
            missing_date = datetime.strptime(missing_date_str, '%Y-%m-%d')
            if missing_date == start_date:
                filling_date = missing_date + timedelta(days=1)  # 计算下一天的 datetime 对象
            else:
                filling_date = missing_date - timedelta(days=1)  # 计算上一天的 datetime 对象
            filling_date_data = df[df[date_column] == filling_date]
            if not filling_date_data.empty:  # 如果该日期存在数据
                new_row = filling_date_data.iloc[0].copy()
                new_row[date_column] = missing_date
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                df = df.sort_values(by=date_column, key=lambda x: pd.to_datetime(x))
                df = df.reset_index(drop=True)
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')  # 转回字符串
    return df


def data_fitting(his_load_df):
    his_load_df_copy = copy.deepcopy(his_load_df)
    date_list = his_load_df['read_date'].unique().tolist()
    if date_list is not None and len(date_list) > 0:
        start_date_str = date_list[0]
        end_date_str = date_list[-1]
        dict_of_dfs = {key: group for key, group in his_load_df.groupby('meter_code')}  # 按列分组，存储为字典
        trans_df_dict = {}

        for index, df in dict_of_dfs.items():
            df = df.drop(columns=['meter_code'])
            df = fill(df, start_date_str, end_date_str)  # 缺失日期填补
            df = df_trans(df)  # 行数据转列数据
            df = outlier_handling(df, 'load')  # 异常值处理
            trans_df_dict[index] = df

        # 重组合并
        handled_df = None
        for index, df in trans_df_dict.items():
            df = df_restore(df)
            df['meter_code'] = index
            if handled_df is None:
                handled_df = df
            else:
                handled_df = pd.concat([handled_df, df])
        handled_df = handled_df.reset_index(drop=True)

        his_load_df_copy = his_load_df_copy[['custom_name', 'meter_name', 'account_no', 'meter_code']]
        his_load_df_copy = his_load_df_copy.drop_duplicates()
        merge_df = pd.merge(handled_df, his_load_df_copy, how='left', on='meter_code')
        return merge_df
    return None


# 异常值处理
def outlier_handling(df, column):
    z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
    df.loc[z_scores > 4, column] = np.nan
    df[column] = df[column].interpolate(method='linear')
    df[column].fillna(method='ffill', inplace=True)
    df[column].fillna(method='bfill', inplace=True)
    return df
