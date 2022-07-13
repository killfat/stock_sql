# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 13:56:24 2021

@author: BHuang
"""
from pandas import read_sql
from settings import connector
from itertools import chain

class DatabaseConnector:
    def __init__(self, db_name):
        self.db_name = db_name
        self.engine = connector(db_name)


class Database:
    def __init__(self, engine, table_name):
        """

        Parameters
        ----------
        engine : connector from DatabaseConnector


        """
        self.engine = engine
        self.table_name = table_name

    def __del__(self):
        self.engine.dispose()

    def operate_by_query(self, query_sentence):
        return self.engine.execute(query_sentence)

    def download_data(self, table_name):
        sql = 'select * from ' + table_name + ';'
        return read_sql(sql, self.engine)

    def upload_data(self, data, data_name, if_exists=None, index=None, index_label=None, dtype=None):
        # if_exists = 'append','replace'
        if if_exists is None:
            if_exists = 'append'
        if index is None:
            index = False

        data.to_sql(data_name, self.engine, if_exists=if_exists, index=index,
                    index_label=index_label, dtype=dtype, chunksize=10000)  # ,method='multi')
        return

    def create_database(self, db_name):
        sql = 'CREATE DATABASE if not exists ' + db_name + ';'
        self.engine.execute(sql)
        return

    def create_index(self, table_name, col_name):
        create_idx = 'CREATE INDEX %s ON %s(%s);' % ('idx_' + col_name, table_name, col_name)
        self.engine.execute(create_idx)

    def delete_database(self, db_name):
        sql = 'DROP DATABASE ' + db_name + ';'
        self.engine.execute(sql)
        return

    def change_column_name(self, original_col_name, new_col_name):

        sql = "alter table %s change `%s` `%s` varchar(100);" % (self.table_name, original_col_name, new_col_name)
        self.engine.execute(sql)

    def select_data(self, start_time, end_time, code=None, price_type_list='*'):
        """

        Parameters
        ----------

        start_time : Datetime(year,month,day,hour,minute)
            time >= start_time
            
        end_time : Datetime(year,month,day,hour,minute)
            time < end_date
            
        code :  String, optional
            stock code, when None return all stocks.
            The default is None.

        Raises
        ------
        TypeError
            when price type isn't list.

        Returns
        -------
        TYPE
            dataframe of the specified data.

        """

        # define part relative to stock code in sql command

        # Nested Query
        if code is None:
            print("WARNING: no code provided")
            t1 = "(SELECT * FROM %s) as t1" % self.table_name

        else:
            t1 = "(SELECT * FROM %s WHERE code = '%s') as t1" % (self.table_name, code)

        # define part relative to price type in sql command
        if price_type_list == '*':

            ...

        else:

            if type(price_type_list) != list:
                price_type_list = [price_type_list]

            if "time" not in price_type_list:
                price_type_list.append("time")
            # time and code is mandatory
            #command = 'time, code'

            price_type_list = ",".join(price_type_list)


        # combine sql command
        sql = "select %s from %s where time >= '%s' and time <= '%s';" \
              % (price_type_list, t1, start_time, end_time)

        return read_sql(sql, self.engine, index_col="time")

    def select_time(self, start_time, end_time, time, code=None, price_type_list='*'):
        if code is None:
            print("WARNING: no code provided")
            t1 = "(SELECT * FROM %s) as t1" % self.table_name

        else:
            t1 = "(SELECT * FROM %s WHERE code = '%s') as t1" % (self.table_name, code)

        # define part relative to price type in sql command
        if price_type_list == '*':

            ...

        else:

            if type(price_type_list) != list:
                price_type_list = [price_type_list]

            if "time" not in price_type_list:
                price_type_list.append("time")
            price_type_list = ",".join(price_type_list)

        # combine sql command
        sql = "SELECT * FROM (SELECT %s from %s where time>='%s' and time<'%s') AS t2 WHERE HOUR(time)='%d' and MINUTE(time)='%d' ORDER BY " \
              "time ASC;" % (price_type_list, t1, start_time, end_time, time.hour, time.minute)

        return read_sql(sql, self.engine, index_col="time")

    def get_all_date(self):
        sql = "SELECT DISTINCT DATE(time) FROM %s;" % self.table_name
        return list(chain(*self.operate_by_query(sql)))

    def select_by_date(self, start_time, end_time, code=None, price_type_list='*'):
        if code is None:
            print("WARNING: no code provided")
            t1 = "(SELECT * FROM %s WHERE time >= '%s' and time < '%s') t1"\
                 % (self.table_name, start_time, end_time)
        else:
            t1 = "(SELECT * FROM %s WHERE code='%s' and time >= '%s' and time < '%s') t1"\
                 % (self.table_name, code, start_time, end_time)
        # define part relative to price type in sql command
        if price_type_list == '*':

            ...

        else:

            if type(price_type_list) != list:
                price_type_list = [price_type_list]

            if "time" not in price_type_list:
                price_type_list.append("time")
            price_type_list = ",".join(price_type_list)
        sql = "SELECT t1.time, t2.open, t3.close, t1.high, t1.low, sum(t1.volume) FROM %s t1 " \
            "LEFT JOIN %s t2 ON t1.code='%s' and t2.code=t1.code and t1.time>='%s' and t1.time<'%s' and DATE(t1.time)=DATE(t2.time) and t1.time>t2.time " \
            "LEFT JOIN %s t3 ON t1.code='%s' and t3.code=t1.code and t1.time>='%s' and t1.time<'%s' and DATE(t1.time)=DATE(t3.time) and t1.time<t3.time " \
            "WHERE t2.time is NULL and t3.time is NULL" \
            % (self.table_name, self.table_name, code, start_time, end_time, self.table_name, code, start_time, end_time)
        return read_sql(sql, self.engine, index_col="date")

    def update_data(self):
        pass
