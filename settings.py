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

def connector(db_name):
    MySQL_CONFIG = {
        'user': user_name,
        'host': 'localhost',
        'database': db_name,
        'password':password}
    return create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (MySQL_CONFIG['user'], MySQL_CONFIG['password'], MySQL_CONFIG['host'], MySQL_CONFIG['database']))
