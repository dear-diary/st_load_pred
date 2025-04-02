import pandas as pd
import streamlit as st
from datetime import date
from sqlite_operation import get_sqlite_conn, close_sqlite_conn, get_fit_load
from io import BytesIO

selected_range = st.date_input(
    "选择数据日期范围",
    value=(date(2025, 1, 1), date(2025, 1, 1)),
    format="YYYY-MM-DD",
)
st.text('选择完日期后，需要一些时间处理数据')
# 获取起始和结束时间
start_str = None
end_str = None
if len(selected_range) == 2:
    start_date, end_date = selected_range
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

fit_df = pd.DataFrame()
excel_data = None
if start_str is not None and end_str is not None:
    conn = get_sqlite_conn()
    fit_df = get_fit_load(start_str, end_str, conn)
    close_sqlite_conn(conn)
    # 数据处理
    fit_df = fit_df.rename(columns={
        'custom_name': '电力用户名称',
        'meter_name': '计量点名称',
        'account_no': '用户号',
        'meter_code': '计量点ID',
        'read_date': '读数日期',
        'authorization': '授权状态',
        'daily_total': '日电量合计',
        'hour1': '时段1电量',
        'hour2': '时段2电量',
        'hour3': '时段3电量',
        'hour4': '时段4电量',
        'hour5': '时段5电量',
        'hour6': '时段6电量',
        'hour7': '时段7电量',
        'hour8': '时段8电量',
        'hour9': '时段9电量',
        'hour10': '时段10电量',
        'hour11': '时段11电量',
        'hour12': '时段12电量',
        'hour13': '时段13电量',
        'hour14': '时段14电量',
        'hour15': '时段15电量',
        'hour16': '时段16电量',
        'hour17': '时段17电量',
        'hour18': '时段18电量',
        'hour19': '时段19电量',
        'hour20': '时段20电量',
        'hour21': '时段21电量',
        'hour22': '时段22电量',
        'hour23': '时段23电量',
        'hour24': '时段24电量',
    })
    fit_df['授权状态'] = '已授权'
    col_idx = fit_df.columns.get_loc('日电量合计')
    fit_df['日电量合计'] = fit_df.iloc[:, col_idx+1:].sum(axis=1)  # 计算合计
    st.dataframe(fit_df)

if not fit_df.empty:
    # 创建内存中的二进制流
    output = BytesIO()
    # 使用 ExcelWriter 将数据写入二进制流
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        fit_df.to_excel(writer, index=False, sheet_name='sheet1')
    # 将指针重置到流的起始位置
    output.seek(0)
    # 创建下载按钮
    st.download_button(
        label="📥 导出 Excel 文件",
        data=output,
        file_name=f'{start_str}_{end_str} 拟合数据.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
else:
    st.markdown("该时间段内的数据为空")
