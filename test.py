# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 14:16:19 2022
Modified Structure and Queries on Sun Apr  3 2022
@author: BHuang
"""

import pandas as pd
import numpy as np
import csv
from datetime import datetime, timedelta
from database_pipeline import DatabaseConnector, Database
from tech_indicator import TechnicalIndicators, get_chg, get_chg_rate
from settings import db_name, table_name, tushare_token
#from collect_indicator import collect_indicator
# 确认数据库名称和表格名称，并生成连接数据库所用的connector
connector_engine = DatabaseConnector(db_name).engine
database = Database(connector_engine, table_name)

tech_ind = TechnicalIndicators(database, tushare_token)
# database.create_index(table_name, 'code')
# 想要计算indicator的股票代码list

#tu_handle = TuData(tushare_token)
#adjdf = tu_handle.get_adj_factor('601789.SH', '20200227', '20200423')
#sharedf = tu_handle.get_float_share('601789.SH', '20200227', '20200423')
#with open('adj.csv', 'w', newline='') as f:
#    adjdf.to_csv(f)
#with open('share.csv', 'w', newline='') as f:
#    sharedf.to_csv(f)

check_time = datetime.fromisoformat('2020-03-02 13:23:00')
code = '601789.XSHG'
lb = tech_ind.LB(code, check_time)
ma1w = tech_ind.MA(code, check_time, period=5)
ma1m = tech_ind.MA(code, check_time, period=5, unit='m')
k, d, j = tech_ind.KDJ(code, check_time)
ub, mb, _ = tech_ind.BOLL(code, check_time)
dif1d, dea1d, macd1d = tech_ind.MACD(code, check_time)
dif1m, dea1m, macd1m = tech_ind.MACD(code, check_time, unit='1m')
dif5m, dea5m, macd5m = tech_ind.MACD(code, check_time, unit='5m')

hsl1d, mahsl1d = tech_ind.HSL(code, check_time)
hsl1m, mahsl1m = tech_ind.HSL(code, check_time, unit='1m')

mfi1d = tech_ind.MFI(code, check_time)
rsi1d = tech_ind.RSI(code, check_time)
accer5m = tech_ind.ACCER(code, check_time, unit='5m')
accer15m = tech_ind.ACCER(code, check_time, unit='15m')
ar, br = tech_ind.BRAR(code, check_time)
pcnt1d, mapcnt1d = tech_ind.PCNT(code, check_time)
cci1d = tech_ind.CCI(code, check_time)
cci1m = tech_ind.CCI(code, check_time, unit='1m')
cci5m = tech_ind.CCI(code, check_time, unit='5m')
lb_chg = get_chg(tech_ind.LB, code, check_time, timedelta(minutes=1))
ma1w_chg = get_chg_rate(tech_ind.MA, code, check_time, timedelta(days=1))
ma1m_chg = get_chg_rate(tech_ind.MA, code, check_time, timedelta(minutes=1), unit='m')
k_chg, d_chg, j_chg = get_chg(tech_ind.KDJ, code, check_time, timedelta(days=1))
dif1d_chg, dea1d_chg, macd1d_chg = get_chg(tech_ind.MACD, code, check_time, timedelta(days=1))
dif1m_chg, dea1m_chg, macd1m_chg = get_chg(tech_ind.MACD, code, check_time, timedelta(minutes=1), unit='1m')
dif5m_chg, dea5m_chg, macd5m_chg = get_chg(tech_ind.MACD, code, check_time, timedelta(minutes=1), unit='5m')

ub_chg, mb_chg, _ = get_chg_rate(tech_ind.BOLL, code, check_time, timedelta(days=1))

hsl1d_chg, mahsl1d_chg = get_chg(tech_ind.HSL, code, check_time, timedelta(days=1))
hsl1m_chg, mahsl1m_chg = get_chg(tech_ind.HSL, code, check_time, timedelta(minutes=1))
mfi1d_chg = get_chg(tech_ind.MFI, code, check_time, timedelta(days=1))
rsi1d_chg = get_chg(tech_ind.RSI, code, check_time, timedelta(days=1))
accer15m_chg = get_chg(tech_ind.ACCER, code, check_time, timedelta(minutes=1), unit='15m')
accer5m_chg = get_chg(tech_ind.ACCER, code, check_time, timedelta(minutes=1), unit='5m')
ar_chg, br_chg = get_chg(tech_ind.BRAR, code, check_time, timedelta(days=1))
pcnt1d_chg, mapcnt1d_chg = get_chg(tech_ind.PCNT, code, check_time, timedelta(days=1))
cci1d_chg = get_chg(tech_ind.CCI, code, check_time, timedelta(days=1))
cci1m_chg = get_chg(tech_ind.CCI, code, check_time, timedelta(minutes=1), unit='1m')
cci5m_chg = get_chg(tech_ind.CCI, code, check_time, timedelta(minutes=1), unit='5m')

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
        'accer5m','accer15m','ar','br','pcnt1d','mapcnt1d','cci1d','cci1m','cci5m',
              'lb_chg', 'k_chg', 'd_chg', 'j_chg', 'dif1d_chg', 'dea1d_chg', 'macd1d_chg', 'dif1m_chg', 'dea1m_chg', 'macd1m_chg',
              'dif5m_chg', 'dea5m_chg', 'macd5m_chg', 'ub_chg', 'mb_chg', 'mfi1d_chg', 'rsi1d_chg',
              'accer5m_chg', 'accer15m_chg', 'ar_chg', 'br_chg', 'pcnt1d_chg', 'mapcnt1d_chg', 'cci1d_chg', 'cci1m_chg', 'cci5m_chg',
              ]
    make_list = [lb, k, d, j, dif1d, dea1d, macd1d, dif1m, dea1m, macd1m,
                 dif5m, dea5m, macd5m, ub, mb, mfi1d, rsi1d,
                 accer5m, accer15m, ar, br, pcnt1d, mapcnt1d, cci1d, cci1m, cci5m,
                 lb_chg, k_chg, d_chg, j_chg, dif1d_chg, dea1d_chg, macd1d_chg, dif1m_chg, dea1m_chg, macd1m_chg,
                 dif5m_chg, dea5m_chg, macd5m_chg, ub_chg, mb_chg, mfi1d_chg, rsi1d_chg,
                 accer5m_chg, accer15m_chg, ar_chg, br_chg, pcnt1d_chg, mapcnt1d_chg, cci1d_chg, cci1m_chg, cci5m_chg,
                 ]
    csv_wt.writerow(header)
    csv_wt.writerow(make_list)

