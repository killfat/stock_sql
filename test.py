# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 14:16:19 2022
Modified Structure and Queries on Sun Apr  3 2022
@author: BHuang
"""

import pandas as pd
import numpy as np
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

code_list=['000687.XSHE','000514.XSHE','000630.XSHE']
for code in code_list:
    five_day_average = tech_ind.MA(code, datetime.fromisoformat('2020-04-23'))
    five_day_lb = tech_ind.LB(code, datetime.fromisoformat('2020-04-23 09:31:00'))
    df_macd = tech_ind.MACD(code, datetime.fromisoformat('2020-04-23'))
    df_kdj = tech_ind.KDJ(code, datetime.fromisoformat('2020-04-23'))
    print('MA/LB', five_day_average, five_day_lb)
    print('MACD')
    print(df_macd)
    print('KDJ')
    print(df_kdj)
