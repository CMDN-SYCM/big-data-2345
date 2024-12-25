import streamlit as st
import pymysql
from pyecharts import options as opts
from pyspark.sql import SparkSession
from pyecharts.charts import Map
from pyecharts.charts import Timeline
from pyecharts.charts import Line, Pie, Bar
import pandas as pd
import os
from pyecharts.charts import Parallel

# 连接MySQL数据库(获取数据)
def data_avg():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path mysql-connector-j-8.0.33.jar pyspark-shell'
    # 查看有哪些数据库
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://192.168.253.139:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://192.168.253.139:9083"). \
        enableHiveSupport(). \
        getOrCreate()

    hive_sql = "select district,year_mon_day,max_t,min_t,AVG(max_t+min_t) as average_t,weather,wind,aqi,aqi_type from bjweather.bj_dwd"
    resDF = spark.sql(hive_sql)
    resDF.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://192.168.253.139:3306/weather") \
        .option("dbtable", "bj_dwd") \
        .option("user", "root") \
        .option("password", "root") \
        .option("mode", "overwrite") \
        .save()

    # 4.停止spark
    spark.stop()




def get_data(sql):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='123456', db='weather')
    st.write('连接数据库成功')
    cursor = connection.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    # print(data)
    data_list = list(data)
    return data, data_list

# 可视化函数
# 时间轴函数
def get_time_list(sql):
    data, data_list = get_data(sql)
    time_list = []
    for row in data:
        if row[1] not in time_list:
            time_list.append(row[1])
    return time_list, data

# 月折线图
def zhe_month(place, year, month):
    sql = f"SELECT substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}';"
    data, data_list = get_data(sql)
    title1 = '{}{}年{}月的温度趋势图'.format(place,year,month)
    e = (
        Line()
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title1),
            tooltip_opts=opts.TooltipOpts(is_show=False),
            xaxis_opts=opts.AxisOpts(type_="category"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        .add_xaxis(xaxis_data=[row[0] for row in data])
        .add_yaxis("最高温度", y_axis=[float(row[1]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="red"))
        .add_yaxis("最低温度", y_axis=[float(row[2]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="blue"))
        .add_yaxis("平均温度", y_axis=[(float(row[1])+float(row[2]))/2 for row in data], itemstyle_opts=opts.ItemStyleOpts(color="green"))
    )
    return e

# 年折线图
def zhe_year(place, year):
    sql = f"SELECT substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 4) = '{year}';"
    data, data_list = get_data(sql)
    title1 = '{}{}年的温度趋势图'.format(place,year)
    e = (
        Line()
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title1),
            tooltip_opts=opts.TooltipOpts(is_show=False),
            xaxis_opts=opts.AxisOpts(type_="category"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        .add_xaxis(xaxis_data=[row[0] for row in data])
        .add_yaxis("最高温度", y_axis=[float(row[1]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="red"))
        .add_yaxis("最低温度", y_axis=[float(row[2]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="blue"))
        .add_yaxis("平均温度", y_axis=[(float(row[1])+float(row[2]))/2 for row in data], itemstyle_opts=opts.ItemStyleOpts(color="green"))
    )
    return e

# 总折线图
def zhe_all(place):
    sql = f"SELECT substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE district = '{place_name}';"
    data, data_list = get_data(sql)
    title1 = '{}的温度趋势图'.format(place)
    e = (
        Line()
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title1),
            tooltip_opts=opts.TooltipOpts(is_show=False),
            xaxis_opts=opts.AxisOpts(type_="category"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        .add_xaxis(xaxis_data=[row[0] for row in data])
        .add_yaxis("最高温度", y_axis=[float(row[1]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="red"))
        .add_yaxis("最低温度", y_axis=[float(row[2]) for row in data], itemstyle_opts=opts.ItemStyleOpts(color="blue"))
        .add_yaxis("平均温度", y_axis=[(float(row[1])+float(row[2]))/2 for row in data], itemstyle_opts=opts.ItemStyleOpts(color="green"))
    )
    return e


# 北京每月各区地图可视化（北京年气温变化）
def map_time(time, data):
    title1 = '北京各区的温度趋势图'
    d = (
        Map()# data_pair=([(row[0], row[2]) for row in text_data if row[2] not in new])
        .add(series_name="", data_pair=[(row[0]+'区', ((float(row[2])+float(row[3]))/2)) for row in data if row[1] == time], maptype="北京")
        # 使用 options 配置项，在 pyecharts 中，一切皆 Options
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title1),
            visualmap_opts=opts.VisualMapOpts(max_=100000, is_piecewise=True,
                                              pieces=[{"max": 40, "min": 31, "label": "30~40℃", "color": "#B40404"},
                                                      {"max": 30, "min": 21, "label": "20~30℃", "color": "#DF0101"},
                                                      {"max": 20, "min": 11, "label": "10~20℃", "color": "#F78181"},
                                                      {"max": 10, "min": 1, "label": "0~10℃", "color": "#F5A9A9"},
                                                      {"max": 0, "min": -10, "label": "-10~0℃", "color": "#FFFFCC"}]
                                              )
        ))
    return d
def create_tu():
    time_list, data = get_time_list(sql)
    for time in time_list:
        map_all = map_time(time, data)
        timeline.add(map_all, time)
    html = timeline.render_embed()
    return html


# 各天气出现占比(饼状图)
def get_weather_data(data): # 地区/时间/天气
    weather_list = []
    for row in data:
        if row[2] not in weather_list:
            weather_list.append(row[2])
    weather_data = []
    for w_type in weather_list:
        count = 0
        for row in data:
            if row[2] == w_type:
                count = count + 1
        weather_data.append((w_type, count))
    return weather_list, weather_data

def bing(weather_data):
    f = (
        Pie()
        .add("", weather_data, radius=[50, 70], center=["30%", "50%"], rosetype="radius")
        .set_global_opts(title_opts=opts.TitleOpts(title="天气占比情况"),
                        legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="60%"))
    )
    return f

def data_type_get(data_options):
    data_type = ','.join(data_options)
    return data_type

def data_table(data, data_options):
    data = pd.DataFrame(data, columns=data_options)
    st.table(data.style.hide_index())

def max_min_tmp(data):
    #sql = f"SELECT 最高温,最低温,天气,风向,空气质量指数,空气质量 FROM beijing WHERE 地区 = '{place_name}';"
    max_t = [float(row[0]) for row in data]
    min_t = [float(row[1]) for row in data]
    api_list = [float(row[4]) for row in data]
    print(api_list)
    high_max_tmp = max(max_t)
    high_min_tmp = min(max_t)
    short_max_tmp = max(min_t)
    short_min_tmp = min(min_t)
    max_api = max(api_list)
    min_api = min(api_list)
    print(type(max_api))
    print(min_api)
    return high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api

def pingxing(high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api, data):
    you_list = []
    liang_list = []
    small_list = []
    mid_list = []
    big_list = []
    yan_list = []

    weather_list = []
    for row in data:
        if row[2] not in weather_list:
            weather_list.append(row[2])

    for row in data:
        if row[5] == '优':
            you_list.append(row)
        if row[5] == '良':
            liang_list.append(row)
        if row[5] == '轻度污染':
            small_list.append(row)
        if row[5] == '中度污染':
            mid_list.append(row)
        if row[5] == '重度污染':
            big_list.append(row)
        if row[5] == '严重污染':
            yan_list.append(row)

    d = (
        Parallel(init_opts=opts.InitOpts(width="1000px", height="700px"))
        .add_schema(
            [
                opts.ParallelAxisOpts(dim=0, name="max_temp", max_=high_max_tmp, min_=high_min_tmp),
                opts.ParallelAxisOpts(dim=1, name="min_temp", max_=short_max_tmp, min_=short_min_tmp),
                opts.ParallelAxisOpts(dim=2, name="天气", type_="category", data=weather_list),
                opts.ParallelAxisOpts(dim=3, name="风向", type_="category", data=[
                    '西南风', '西北风', '东风', '西风', '南风', '北风', '东北风', '东南风', '西南风', '西北风', '无']),
                opts.ParallelAxisOpts(dim=4, name="空气质量指数", max_=max_api, min_=min_api),
                opts.ParallelAxisOpts(dim=5, name="空气质量", type_="category", data=["优", "良", "轻度污染", "中度污染", "重度污染", "严重污染"])
            ]
        )
        .add("优", you_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="blue"))
        .add("良", liang_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="red"))
        .add("轻度污染", small_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="orange"))
        .add("中度污染", mid_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="green"))
        .add("重度污染", big_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="purple"))
        .add("严重污染", yan_list, linestyle_opts=opts.LineStyleOpts(opacity=1, color="yellow"))

        .set_global_opts(
            title_opts=opts.TitleOpts(title="天气间因素图")
        )
        .set_series_opts(
            linestyle_opts=opts.LineStyleOpts(width=1, opacity=1),
        )
    )
    return d

def get_bar_list(data):
    weather_type = []
    for row in data:
        tianqi = row[0]
        if tianqi not in weather_type:
            weather_type.append(tianqi)
    # weather,aqi

    bar_list = []
    for weather in weather_type:
        s = 0
        c = 0
        for row in data:
            aqi = float(row[1])
            w = row[0]
            if w == weather:
                s += aqi
                c += 1
        if c != 0:
            avg_aqi = s / c
        else:
            avg_aqi = 0  # 处理除零情况，如果 c 为零则平均值为0
        bar_list.append([weather, avg_aqi])

    x_list = []
    y_list = []
    for row in bar_list:
        a = round(row[1], 2)
        x_list.append(row[0])
        y_list.append(a)

    return x_list, y_list

def creat_bar(x_list, y_list):
    b = (
        Bar()
        .add_xaxis(x_list)
        .add_yaxis("AQI指数", y_list, stack="total",label_opts=opts.LabelOpts(is_show=True))
        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"空气质量指数堆叠柱状图"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            yaxis_opts=opts.AxisOpts(type_="value"),
            xaxis_opts=opts.AxisOpts(type_="category"),
        )
    )

    return b



# streamlit实现大屏
places = ["朝阳区", "昌平区", "丰台区", "石景山区", "东城区",
           "西城区", '海淀区', "门头沟区", "房山区", "通州区",
           "顺义区", "大兴区", "怀柔区", "平谷区", "密云区", "延庆区"]

# 主界面
st.title("基于大数据平台的天气数据")

# 侧边栏
st.sidebar.title('天气数据查询')
st.sidebar.markdown('---')

# 查询地区选择
place = st.sidebar.selectbox("*选择你要查询的地区：", places)
place_name = place.replace('区', '')
# 查询数据选择
data_options = st.sidebar.multiselect(
        label='地区数据选项',
        options=['最高温', '最低温', '天气', '风向', '空气质量指数', '空气质量'],
        default=None,  # 默认选中最高温
        format_func=str,
        help='选择您将查询的地区数据'
    )

# 日期输入框
time = st.sidebar.text_input(label='请输入您想查询的日期(格式2016-01-01)', value="", max_chars=None, key=None,
                type="default", help=None, autocomplete=None,
                on_change=None, placeholder=None)

# 查询数据 ，表格显示
data_type_try = data_type_get(data_options)
data_type = data_type_try.replace('最高温', 'max_t').replace('最低温', 'min_t').replace('天气', 'weather').replace('风向', 'wind').replace('空气质量指数', 'aqi').replace('空气质量', 'aqi_type')
if st.sidebar.button('数据查询'):
    if len(time) == 0:
        sql = f"SELECT {data_type} FROM 北京_清洗后 WHERE district = '{place_name}';"
        data, data_list = get_data(sql)
        st.write('{}区的数据如下：'.format(place_name))
        data_table(data, data_options)

    if len(time) == 4:
        year = time
        sql = f"SELECT {data_type} FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 4) = '{year}';"
        print(sql)
        data, data_list = get_data(sql)
        st.write('{}区{}年的数据如下：'.format(place_name, year))
        data_table(data, data_options)

    if len(time) == 7:   # (2016-01)
        year, month = time.split('-')
        sql = f"SELECT {data_type} FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}';"
        print(sql)
        data, data_list = get_data(sql)
        st.write('{}区{}年{}月的数据如下：'.format(place_name, year, month))
        data_table(data, data_options)

    if len(time) == 10:   # (2016-01-01)
        year, month, day = time.split('-')
        sql = f"SELECT {data_type} FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 12) = '{year}-{month}-{day}';"
        print(sql)
        data, data_list = get_data(sql)
        st.write('{}区{}年{}月{}日的数据如下：'.format(place_name, year, month, day))
        data_table(data, data_options)

# 可视化选择
keshihua_list = ['气温变化趋势可视化(折线图)', '北京年气温变化(地图)', '各天气出现占比(饼状图)', '天气间因素(平行坐标轴)','空气指数与天气(柱状图)']
keshihua_option = st.sidebar.selectbox(
        label='地区数据选项',
        options=keshihua_list,  # 添加一个空选项作为默认
        index=0,  # 默认选择第一个（空选项）
        format_func=str,
        help='选择您想了解的数据情况'
    )


# 可视化显示
if st.sidebar.button('数据分析'):
    # 折线图
    if keshihua_option == '气温变化趋势可视化(折线图)':
        # 总
        if len(time) == 0:
            year = time
            chart = zhe_all(place_name)
            html = chart.render_embed()
            st.components.v1.html(html, width=1000, height=800)
        # 年
        if len(time) == 4:
            year = time
            chart = zhe_year(place_name, year)
            html = chart.render_embed()
            st.components.v1.html(html, width=1000, height=800)
        # 月
        if len(time) == 7:
            year, month = time.split('-')
            chart = zhe_month(place_name, year, month)
            html = chart.render_embed()
            st.components.v1.html(html, width=1000, height=800)
        # 日
        if len(time) == 10:
            st.write("每日天气每月无趋势变化!")
    # 地图
    if keshihua_option == '北京年气温变化(地图)':
        # 时间轴
        timeline = Timeline(  # 创建时间轴对象
            init_opts=opts.InitOpts(width='800px', height='600px'),
        )
        # 调整时间轴位置
        timeline.add_schema(
            orient="vertical",  # 垂直展示
            is_auto_play=True,
            is_inverse=True,
            play_interval=200,  # 播放时间间隔，毫秒
            pos_left="null",
            pos_right="2%",
            pos_top="20",
            pos_bottom="20",
            width="100",  # 组件宽度
            label_opts=opts.LabelOpts(is_show=True, color="#fff", position='left'),
        )
        # 总数据
        if len(time) == 0:
            sql = f"SELECT district,substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后;"
            html = create_tu()
            st.components.v1.html(html, width=1000, height=800)

        # 年数据
        if len(time) == 4:
            year = time
            sql = f"SELECT district,substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE substring(year_mon_day, 1, 4) = '{year}';"
            html = create_tu()
            st.components.v1.html(html, width=1000, height=800)

        # 月数据
        if len(time) == 7:
            year, month = time.split('-')
            sql = f"SELECT district,substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE substring(year_mon_day, 1, 7) = '{year}-{month}';"
            html = create_tu()
            st.components.v1.html(html, width=1000, height=800)

        # 日数据
        if len(time) == 10:
            year, month, day = time.split('-')
            sql = f"SELECT district,substring(year_mon_day, 1, 12),max_t,min_t FROM 北京_清洗后 WHERE substring(year_mon_day, 1, 12) = '{year}-{month}-{day}';"
            data, data_list = get_data(sql)

            map = map_time(time, data)
            html = map.render_embed()
            st.write("每日气温无趋势变化!")
            st.components.v1.html(html, width=1000, height=800)
    # 饼状图
    if keshihua_option == '各天气出现占比(饼状图)':
        if len(time) == 0:
            sql = f"SELECT district,substring(year_mon_day, 1, 12),weather FROM 北京_清洗后 WHERE district = '{place_name}';"
            data, data_list = get_data(sql)
            weather_list, weather_data = get_weather_data(data)

            bing_tu = bing(weather_data)
            html = bing_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 4:
            year = time
            sql = f"SELECT district,substring(year_mon_day, 1, 12),weather FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 4) = '{year}';"
            data, data_list = get_data(sql)
            weather_list, weather_data = get_weather_data(data)

            bing_tu = bing(weather_data)
            html = bing_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 7:
            year, month = time.split('-')
            sql = f"SELECT district,substring(year_mon_day, 1, 12),weather FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}';"
            data, data_list = get_data(sql)
            weather_list, weather_data = get_weather_data(data)

            bing_tu = bing(weather_data)
            html = bing_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 10:
            year, month, day = time.split('-')
            sql = f"SELECT district,substring(year_mon_day, 1, 12),weather FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}-{day}';"
            data, data_list = get_data(sql)
            st.write("该日天气为:{}",data[2])
            weather_list, weather_data = get_weather_data(data)

            bing_tu = bing(weather_data)
            html = bing_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)
    # 平行坐标轴
    if keshihua_option =='天气间因素(平行坐标轴)':
        if len(time) == 0:
            sql = f"SELECT max_t,min_t,weather,wind,aqi,aqi_type FROM 北京_清洗后 WHERE district = '{place_name}';"
            data, data_list = get_data(sql)
            high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api = max_min_tmp(data)
            ping = pingxing(high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api, data)
            html = ping.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 4:
            year = time
            sql = f"SELECT max_t,min_t,weather,wind,aqi,aqi_type FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 4) = '{year}';"
            data, data_list = get_data(sql)
            high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api = max_min_tmp(data)
            ping = pingxing(high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api, data)
            html = ping.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 7:
            year, month = time.split('-')
            sql = f"SELECT max_t,min_t,weather,wind,aqi,aqi_type FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}';"
            data, data_list = get_data(sql)
            high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api = max_min_tmp(data)
            ping = pingxing(high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api, data)
            html = ping.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 10:
            year, month, day = time.split('-')
            sql = f"SELECT max_t,min_t,weather,wind,aqi,aqi_type FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 12) = '{year}-{month}-{day}';"
            data, data_list = get_data(sql)
            high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api = max_min_tmp(data)
            ping = pingxing(high_max_tmp, high_min_tmp, short_max_tmp, short_min_tmp, max_api, min_api, data)
            html = ping.render_embed()
            st.components.v1.html(html, width=1000, height=800)

    if keshihua_option =='空气指数与天气(柱状图)':
        if len(time) == 0:
            sql = f"SELECT weather,aqi FROM 北京_清洗后 WHERE district = '{place_name}';"
            data, data_list = get_data(sql)
            x_list, y_list = get_bar_list(data)

            bar_tu = creat_bar(x_list, y_list)
            html = bar_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 4:
            year = time
            sql = f"SELECT weather,aqi FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 4) = '{year}';"
            data, data_list = get_data(sql)
            x_list, y_list = get_bar_list(data)

            bar_tu = creat_bar(x_list, y_list)
            html = bar_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 7:
            year, month = time.split('-')
            sql = f"SELECT weather,aqi FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 7) = '{year}-{month}';"
            data, data_list = get_data(sql)
            x_list, y_list = get_bar_list(data)

            bar_tu = creat_bar(x_list, y_list)
            html = bar_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

        if len(time) == 10:
            year, month, day = time.split('-')
            sql = f"SELECT weather,aqi FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 12) = '{year}-{month}-{day}';"
            data, data_list = get_data(sql)
            x_list, y_list = get_bar_list(data)

            bar_tu = creat_bar(x_list, y_list)
            html = bar_tu.render_embed()
            st.components.v1.html(html, width=1000, height=800)

# 数据分析
def cut_data(data):
    sql = f"SELECT district,max_t,min_t,aqi FROM 北京_清洗后 WHERE district = '{place_name}' AND substring(year_mon_day, 1, 12) = '{year}-{month}-{day}';"