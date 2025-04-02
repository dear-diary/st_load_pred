from datetime import date
import pandas as pd
import streamlit as st
from sqlite_operation import get_sqlite_conn, close_sqlite_conn, get_custom_df, query_load, query_pred
from utils import df_trans
from pred import similar_day_forecast

selected_range = st.date_input(
    "选择查看数据的日期范围",
    value=(date(2025, 1, 1), date(2025, 1, 1)),
    format="YYYY-MM-DD",
)
# 获取起始和结束时间
start_str = None
end_str = None
if len(selected_range) == 2:
    start_date, end_date = selected_range
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

# 获取用户列表
conn = get_sqlite_conn()
custom_df = get_custom_df(conn)
close_sqlite_conn(conn)
custom_list = custom_df['custom_name'].tolist()
custom_list.insert(0, '全部客户')
custom_name = st.selectbox(
    "选择客户名称",
    custom_list,
    index=None,
    key=0
)

tab1, tab2, tab3 = st.tabs(["历史电量", "预测电量", "查看预测电量"])
with tab1:
    # 查询历史电量数据和拟合电量数据
    if start_str is not None and end_str is not None and custom_name is not None:
        get_his_df = st.button('查询', key=1)
        if get_his_df:
            conn = get_sqlite_conn()
            his_df = query_load(start_str, end_str, custom_name, conn, 'his_load')
            fit_df = query_load(start_str, end_str, custom_name, conn, 'fit_load')
            close_sqlite_conn(conn)
            trans_his_df = df_trans(his_df)
            trans_fit_df = df_trans(fit_df)
            # 拟合数据和实际数据结合
            combined_df = pd.merge(trans_his_df, trans_fit_df, how='left', on='time')
            combined_df = combined_df.rename(columns={'load_y': '拟合电量', 'load_x': '实际电量', 'time': '时间'})
            combined_df = combined_df.set_index("时间")
            graph_tab, table_tab = st.tabs(["图", "表"])
            with graph_tab:
                st.line_chart(combined_df)
            with table_tab:
                st.dataframe(combined_df, width=800)

with tab2:
    pred_days = st.selectbox(
        "选择预测天数（默认所有客户全部预测）",
        [1, 2],
        index=None,
        key=2
    )
    pred = st.button('执行今日预测', key=3)

    if pred:
        if pred_days is not None:
            placeholder = st.empty()
            placeholder2 = st.empty()
            for index, pred_custom_name in enumerate(custom_list):
                similar_day_forecast(pred_custom_name, pred_days)
                placeholder.text(f"当前进度{index+1}/{len(custom_list)}, 预测中请勿执行其他操作")
                placeholder2.text(f"{pred_custom_name}预测完成")
            st.markdown('全部预测完成！')
        else:
            st.markdown('请先选择预测天数')

with tab3:
    if start_str is not None and end_str is not None and custom_name is not None:
        # 查询预测电量数据
        add_real_load = st.checkbox("是否包含实际电量")
        add_fit_load = st.checkbox("是否包含拟合电量")
        get_pred_df = st.button('查询', key=4)
        if get_pred_df:
            conn = get_sqlite_conn()
            pred_df = query_pred(start_str, end_str, custom_name, conn)
            if not pred_df.empty:
                pred_df = pred_df.rename(columns={'time': '时间', 'pred_value': '预测电量'})
                if add_real_load:
                    real_load_df = query_load(start_str, end_str, custom_name, conn, 'his_load')
                    if not real_load_df.empty:
                        real_load_df = df_trans(real_load_df)
                        real_load_df = real_load_df.rename(columns={'time': '时间', 'load': '实际电量'})
                        pred_df = pd.merge(pred_df, real_load_df, on='时间', how='outer')
                    else:
                        pred_df['实际电量'] = None
                if add_fit_load:
                    fit_load_df = query_load(start_str, end_str, custom_name, conn, 'fit_load')
                    if not fit_load_df.empty:
                        fit_load_df = df_trans(fit_load_df)
                        fit_load_df = fit_load_df.rename(columns={'time': '时间', 'load': '拟合电量'})
                        pred_df = pd.merge(pred_df, fit_load_df, on='时间', how='outer')
                    else:
                        pred_df['拟合电量'] = None
                pred_df = pred_df.set_index("时间")
            close_sqlite_conn(conn)
            graph_tab2, table_tab2 = st.tabs(["图", "表"])
            with graph_tab2:
                st.line_chart(pred_df)
            with table_tab2:
                st.dataframe(pred_df, width=800)
