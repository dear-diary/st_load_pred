import streamlit as st
import pandas as pd
from sqlite_operation import save_load, get_sqlite_conn, close_sqlite_conn, save_custom
from fit import data_fitting


uploaded_file = st.file_uploader("选择负荷文件", key=1)
if uploaded_file is not None:
    his_load_df = pd.read_excel(uploaded_file)
    his_load_df = his_load_df.iloc[:-1]
    his_load_df = his_load_df.drop(columns=['Unnamed: 0'])
    # 列名称重命名
    his_load_df = his_load_df.rename(columns={
        '电力用户名称': 'custom_name',
        '计量点名称': 'meter_name',
        '用户号': 'account_no',
        '计量点ID': 'meter_code',
        '读数日期': 'read_date',
        '授权状态': 'authorization',
        '日电量合计': 'daily_total',
        '时段1电量': 'hour1',
        '时段2电量': 'hour2',
        '时段3电量': 'hour3',
        '时段4电量': 'hour4',
        '时段5电量': 'hour5',
        '时段6电量': 'hour6',
        '时段7电量': 'hour7',
        '时段8电量': 'hour8',
        '时段9电量': 'hour9',
        '时段10电量': 'hour10',
        '时段11电量': 'hour11',
        '时段12电量': 'hour12',
        '时段13电量': 'hour13',
        '时段14电量': 'hour14',
        '时段15电量': 'hour15',
        '时段16电量': 'hour16',
        '时段17电量': 'hour17',
        '时段18电量': 'hour18',
        '时段19电量': 'hour19',
        '时段20电量': 'hour20',
        '时段21电量': 'hour21',
        '时段22电量': 'hour22',
        '时段23电量': 'hour23',
        '时段24电量': 'hour24',
    })
    # 去除多余小数部分
    his_load_df['account_no'] = his_load_df['account_no'].astype(str).str.replace(r'\.0$', '', regex=True)
    his_load_df['meter_code'] = his_load_df['meter_code'].astype(str).str.replace(r'\.0$', '', regex=True)

    # 创建数据连接
    conn = get_sqlite_conn()
    message1 = save_load(his_load_df, conn, 'his_load')
    save_custom(his_load_df, conn)
    if message1 == '导入成功':
        st.markdown('历史电量导入成功!')
        # 数据拟合和异常值处理
        fitting_load_df = data_fitting(his_load_df)
        if not fitting_load_df.empty:
            message2 = save_load(fitting_load_df, conn, 'fit_load')
            if message2 == '导入成功':
                st.markdown('历史电量拟合成功!')
            else:
                st.markdown(message2)
    else:
        st.markdown(message1)
    close_sqlite_conn(conn)
