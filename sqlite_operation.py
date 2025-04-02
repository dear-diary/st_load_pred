import sqlite3
import traceback

import pandas as pd


# 获取sqlite连接
def get_sqlite_conn():
    conn = sqlite3.connect('load.db')
    cursor = conn.cursor()

    # 客户名称表
    create_custom_sql = '''
    CREATE TABLE IF NOT EXISTS `custom` (
        `custom_name` varchar(50) DEFAULT NULL
    );
    '''
    cursor.execute(create_custom_sql)

    # 历史数据表
    create_his_load_sql = '''
     CREATE TABLE IF NOT EXISTS `his_load` (
       `custom_name` varchar(50) DEFAULT NULL,
       `meter_name` varchar(50) DEFAULT NULL,
       `account_no` varchar(20) DEFAULT NULL,
       `meter_code` varchar(20) DEFAULT NULL,
       `read_date` varchar(20) DEFAULT NULL,
       `authorization` varchar(20) DEFAULT NULL,
       `daily_total` decimal(12,4) DEFAULT NULL,
       `hour1` decimal(12,4) DEFAULT NULL,
       `hour2` decimal(12,4) DEFAULT NULL,
       `hour3` decimal(12,4) DEFAULT NULL,
       `hour4` decimal(12,4) DEFAULT NULL,
       `hour5` decimal(12,4) DEFAULT NULL,
       `hour6` decimal(12,4) DEFAULT NULL,
       `hour7` decimal(12,4) DEFAULT NULL,
       `hour8` decimal(12,4) DEFAULT NULL,
       `hour9` decimal(12,4) DEFAULT NULL,
       `hour10` decimal(12,4) DEFAULT NULL,
       `hour11` decimal(12,4) DEFAULT NULL,
       `hour12` decimal(12,4) DEFAULT NULL,
       `hour13` decimal(12,4) DEFAULT NULL,
       `hour14` decimal(12,4) DEFAULT NULL,
       `hour15` decimal(12,4) DEFAULT NULL,
       `hour16` decimal(12,4) DEFAULT NULL,
       `hour17` decimal(12,4) DEFAULT NULL,
       `hour18` decimal(12,4) DEFAULT NULL,
       `hour19` decimal(12,4) DEFAULT NULL,
       `hour20` decimal(12,4) DEFAULT NULL,
       `hour21` decimal(12,4) DEFAULT NULL,
       `hour22` decimal(12,4) DEFAULT NULL,
       `hour23` decimal(12,4) DEFAULT NULL,
       `hour24` decimal(12,4) DEFAULT NULL
     );
     '''
    cursor.execute(create_his_load_sql)

    # 拟合数据表
    create_fit_load_sql = '''
     CREATE TABLE IF NOT EXISTS `fit_load` (
       `custom_name` varchar(50) DEFAULT NULL,
       `meter_name` varchar(50) DEFAULT NULL,
       `account_no` varchar(20) DEFAULT NULL,
       `meter_code` varchar(20) DEFAULT NULL,
       `read_date` varchar(20) DEFAULT NULL,
       `authorization` varchar(20) DEFAULT NULL,
       `daily_total` decimal(12,4) DEFAULT NULL,
       `hour1` decimal(12,4) DEFAULT NULL,
       `hour2` decimal(12,4) DEFAULT NULL,
       `hour3` decimal(12,4) DEFAULT NULL,
       `hour4` decimal(12,4) DEFAULT NULL,
       `hour5` decimal(12,4) DEFAULT NULL,
       `hour6` decimal(12,4) DEFAULT NULL,
       `hour7` decimal(12,4) DEFAULT NULL,
       `hour8` decimal(12,4) DEFAULT NULL,
       `hour9` decimal(12,4) DEFAULT NULL,
       `hour10` decimal(12,4) DEFAULT NULL,
       `hour11` decimal(12,4) DEFAULT NULL,
       `hour12` decimal(12,4) DEFAULT NULL,
       `hour13` decimal(12,4) DEFAULT NULL,
       `hour14` decimal(12,4) DEFAULT NULL,
       `hour15` decimal(12,4) DEFAULT NULL,
       `hour16` decimal(12,4) DEFAULT NULL,
       `hour17` decimal(12,4) DEFAULT NULL,
       `hour18` decimal(12,4) DEFAULT NULL,
       `hour19` decimal(12,4) DEFAULT NULL,
       `hour20` decimal(12,4) DEFAULT NULL,
       `hour21` decimal(12,4) DEFAULT NULL,
       `hour22` decimal(12,4) DEFAULT NULL,
       `hour23` decimal(12,4) DEFAULT NULL,
       `hour24` decimal(12,4) DEFAULT NULL
     );
     '''
    cursor.execute(create_fit_load_sql)

    # 预测数据表
    create_load_pred_sql = '''
    CREATE TABLE IF NOT EXISTS `load_pred` (
       `custom_name` varchar(50) DEFAULT NULL,
       `pred_date` varchar(20) DEFAULT NULL,
       `pred_time` varchar(20) DEFAULT NULL,
       `pred_value` decimal(12,4) DEFAULT NULL
    );
    '''
    cursor.execute(create_load_pred_sql)

    return conn


def save_load(his_load_df, conn, table_name):
    if not his_load_df.empty and conn is not None:
        cursor = conn.cursor()
        date_list = his_load_df['read_date'].unique().tolist()
        if date_list is not None and len(date_list) > 0:
            start_date = date_list[0]
            end_date = date_list[-1]
            cursor.execute(f'DELETE FROM {table_name} WHERE read_date between "{start_date}" and "{end_date}"')
            his_load_df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            conn.commit()
        return "导入成功"
    return '导入失败，数据不存在'


def save_custom(his_load_df, conn):
    if not his_load_df.empty and conn is not None:
        custom_list = his_load_df['custom_name'].unique().tolist()
        df = get_custom_df(conn)
        insert_df = pd.DataFrame(columns=['custom_name'])
        if not df.empty:  # 取并集插入
            origin_custom_list = df['custom_name'].unique().tolist()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM custom where 1=1")
            union_list = list(dict.fromkeys(custom_list + origin_custom_list))
            insert_df['custom_name'] = union_list
        else:  # 新增
            insert_df['custom_name'] = custom_list
        insert_df.to_sql(name='custom', con=conn, if_exists='append', index=False)
        conn.commit()


def get_custom_df(conn):
    query = "SELECT * FROM custom"
    df = pd.read_sql_query(query, conn)
    return df


def query_load(start_str, end_str, custom_name, conn, table_name):
    if custom_name == '全部客户':
        query = f'SELECT read_date, SUM(hour1) as hour1, SUM(hour2) as hour2, SUM(hour3) as hour3, SUM(hour4) as hour4, ' \
                f'SUM(hour5) as hour5,SUM(hour6) as hour6,SUM(hour7) as hour7,SUM(hour8) as hour8,' \
                f'SUM(hour9) as hour9,SUM(hour10) as hour10,SUM(hour11) as hour11,SUM(hour12) as hour12,' \
                f'SUM(hour13) as hour13,SUM(hour14) as hour14,SUM(hour15) as hour15,SUM(hour16) as hour16,' \
                f'SUM(hour17) as hour17,SUM(hour18) as hour18,SUM(hour19) as hour19,SUM(hour20) as hour20,' \
                f'SUM(hour21) as hour21,SUM(hour22) as hour22,SUM(hour23) as hour23,SUM(hour24) as hour24 ' \
                f'FROM {table_name} WHERE read_date between "{start_str}" and "{end_str}" group by read_date '
    else:
        query = f'SELECT read_date, SUM(hour1) as hour1, SUM(hour2) as hour2, SUM(hour3) as hour3, SUM(hour4) as hour4, ' \
                f'SUM(hour5) as hour5,SUM(hour6) as hour6,SUM(hour7) as hour7,SUM(hour8) as hour8,' \
                f'SUM(hour9) as hour9,SUM(hour10) as hour10,SUM(hour11) as hour11,SUM(hour12) as hour12,' \
                f'SUM(hour13) as hour13,SUM(hour14) as hour14,SUM(hour15) as hour15,SUM(hour16) as hour16,' \
                f'SUM(hour17) as hour17,SUM(hour18) as hour18,SUM(hour19) as hour19,SUM(hour20) as hour20,' \
                f'SUM(hour21) as hour21,SUM(hour22) as hour22,SUM(hour23) as hour23,SUM(hour24) as hour24 ' \
                f'FROM {table_name} WHERE custom_name = "{custom_name}" and read_date between "{start_str}" and "{end_str}" group by read_date '
    df = pd.read_sql_query(query, conn)
    return df


def save_pred_data(pred_df, conn):
    custom_name = pred_df['custom_name'].unique()[0]
    pred_date = pred_df['pred_date'].unique()[0]
    if not pred_df.empty and conn is not None:
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM load_pred WHERE pred_date = "{pred_date}" and custom_name = "{custom_name}"')
        pred_df.to_sql(name='load_pred', con=conn, if_exists='append', index=False)
        conn.commit()


def query_pred(start_str, end_str, custom_name, conn):
    query = f'SELECT * FROM load_pred WHERE custom_name = "{custom_name}" and pred_date between "{start_str}" and "{end_str}"'
    pred_df = pd.read_sql_query(query, conn)
    pred_df['time'] = pred_df['pred_date'] + ' ' + pred_df['pred_time'] + ':00'
    pred_df = pred_df.drop(columns=['pred_date', 'pred_time', 'custom_name'])
    return pred_df


def get_fit_load(start_str, end_str, conn):
    query = f'SELECT * FROM fit_load WHERE read_date between "{start_str}" and "{end_str}"'
    fit_df = pd.read_sql_query(query, conn)
    return fit_df


# 关闭连接
def close_sqlite_conn(conn):
    if conn is not None:
        conn.close()
