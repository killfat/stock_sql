# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 13:58:39 2021

@author: BHuang
"""

from sqlalchemy import create_engine 


user_name = 'root'
password = 'root'
db_name = 'price_info'
table_name = 'stock_prices'
tushare_token = "afa152c521c091df3cd7d92bf9661884f0e9c71e1859a0dee2c21c38"


def connector(db_name):
    MySQL_CONFIG = {
        'user': user_name,
        'host': 'localhost',
        'database': db_name,
        'password':password}
    return create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (MySQL_CONFIG['user'], MySQL_CONFIG['password'], MySQL_CONFIG['host'], MySQL_CONFIG['database']))
