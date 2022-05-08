README-0.1  2022-04-30
使用方法：修改test.py下数据库名、表名，执行test.py。当目录下拥有dates.pkl则根据dates.pkl计算日期；
    否则读取数据库日期信息，构建日期列表，并创建dates.pkl。

已实现的函数
MA(ticker, check_date, period=5, unit='d') 计算5日收盘价平均值
LB(ticker, cur_time, days=5) 计算5日量比
MACD(self, ticker, check_date, short=12, long=26, mid=9, include_now=True)
    冷启动：返回DIF，DEA，以及2(DIF-DEA)的DATAFRAME
    已计算过昨日的：返回DIF、DEA、2(DIF-DEA)的数值 #需对接Redis部分，还未实现
KDJ(ticker, check_date, days=9, m1=3, m2=3)
    冷启动：返回K，D，J的DATAFRAME
    已计算过昨日的：返回K、D、J的数值 #需对接Redis部分，还未实现

添加的一些方法
datetime_manager.py 提供计算日期方法
tech_indicator.py
--select_n_terms_data() 查找时间范围数据
--select_n_time_data() 查找每天时间点数据
  --get_close() 可以得到几天的收盘数据
