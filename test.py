# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 14:16:19 2022
Modified Structure and Queries on Sun Apr  3 2022
@author: BHuang
"""

import pandas as pd
import numpy as np
import csv
from datetime import datetime
from database_pipeline import DatabaseConnector, Database
from tech_indicator import TechnicalIndicators
from settings import db_name, table_name
# 确认数据库名称和表格名称，并生成连接数据库所用的connector
connector_engine = DatabaseConnector(db_name).engine
database = Database(connector_engine, table_name)

tech_ind = TechnicalIndicators(database)
# database.create_index(table_name, 'code')
# 想要计算indicator的股票代码list
check_time = datetime.fromisoformat('2020-04-23 09:31:00')
code = '601789.XSHG'
lb = tech_ind.LB(code, check_time)
ma1w = tech_ind.MA(code, check_time, period=5)
ma1m = tech_ind.MA(code, check_time, period=5, unit='m')
k, d, j = tech_ind.KDJ(code, check_time)
mb, ub, _ = tech_ind.BOLL(code, check_time)
dif1d, dea1d, macd1d = tech_ind.MACD(code, check_time)
dif1m, dea1m, macd1m = tech_ind.MACD(code, check_time, unit='1m')
dif5m, dea5m, macd5m = tech_ind.MACD(code, check_time, unit='5m')

#hsl1d = tech_ind.HSL(code, check_time)
# mahsl1d = tech_ind.HSL(code, )
# hsl1m = tech_ind.HSL(code, check_time)
# mahsl1m = tech_ind.HSL(code, )

mfi1d = tech_ind.MFI(code, check_time)
rsi1d = tech_ind.RSI(code, check_time)
accer5m = tech_ind.ACCER(code, check_time, unit='5m')
accer15m = tech_ind.ACCER(code, check_time, unit='15m')
ar, br = tech_ind.BRAR(code, check_time)
pcnt1d, mapcnt1d = tech_ind.PCNT(code, check_time)
cci1d = tech_ind.CCI(code, check_time)
cci1m = tech_ind.CCI(code, check_time, unit='1m')
cci5m = tech_ind.CCI(code, check_time, unit='5m')
print("LB:", lb)
#print("MA1w/MA1m:", ma1w, ma1m)
print("K/D/J:", k, d, j)
print("DIF1d/DEA1d/MACD1d:", dif1d, dea1d, macd1d)
print("DIF1m/DEA1m/MACD1m:", dif1m, dea1m, macd1m)
print("DIF5m/DEA5m/MACD5m:", dif5m, dea5m, macd5m)
print("UB/MB:", ub, mb)
print("MFI:", mfi1d)
print("RSI:", rsi1d)
print("ACCER5m/15m:", accer5m, accer15m)
print("AR/BR:", ar, br)
print("PCNT/MAPCNT:", pcnt1d, mapcnt1d)
print("CCI1d/1m/5m:", cci1d, cci1m, cci5m)
with open("out.csv", 'w', newline='') as f:
    csv_wt = csv.writer(f)
    header = ['lb','k','d','j','dif1d','dea1d','macd1d','dif1m','dea1m','macd1m',
        'dif5m','dea5m','macd5m','ub','mb','mfi1d','rsi1d',
        'accer5m','accer15m','ar','br','pcnt1d','mapcnt1d','cci1d','cci1m','cci5m']
    make_list = [lb, k, d, j, dif1d, dea1d, macd1d, dif1m, dea1m, macd1m,
                 dif5m, dea5m, macd5m, ub, mb, mfi1d, rsi1d,
                 accer5m, accer15m, ar, br, pcnt1d, mapcnt1d, cci1d, cci1m, cci5m]
    csv_wt.writerow(header)
    csv_wt.writerow(make_list)

