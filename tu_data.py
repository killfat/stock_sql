import tushare as ts
from pandas import to_datetime, concat
from datetime import timedelta
import traceback

class TuData:
    def __init__(self, token):
        ts.set_token(token)
        self.handle = ts.pro_api()
        self.share_buffer = {}
        self.adj_buffer = {}

    def get_float_share(self, code, start_day, end_day):
        df = None
        query = False
        delta = timedelta(days=1)
        day = start_day
        if code in self.share_buffer:
            df = self.share_buffer[code]
            while day <= end_day:
                if day not in df.index:
                    query = True
                    break
                day += delta
        else:
            query = True

        if query:
            try:
                df_q = self.handle.query('daily_basic', ts_code=code,
                                         start_date=(start_day - timedelta(days=60)).strftime('%Y%m%d'),
                                         end_date=end_day.strftime('%Y%m%d'),
                                         fields='ts_code,trade_date,float_share')
                df_q['trade_date'] = to_datetime(df_q['trade_date'], format='%Y%m%d')
                df_q = df_q.set_index('trade_date')
                if df is not None:
                    df = concat([df, df_q])
                    df = df[~df.index.duplicated(keep='first')]
                else:
                    df = df_q
                self.share_buffer[code] = df
            except Exception as e:
                traceback.print_exc()
                print(e)
        return df

    def get_adj_factor(self, code, start_day, end_day):
        df = None
        query = False
        delta = timedelta(days=1)
        day = start_day
        if code in self.adj_buffer:
            df = self.adj_buffer[code]
            while day <= end_day:
                if day not in df.index:
                    query = True
                    break
                day += delta
        else:
            query = True

        if query:
            try:
                df_q = self.handle.query('adj_factor', ts_code=code,
                                         start_date=(start_day - timedelta(days=60)).strftime('%Y%m%d'),
                                         end_date=end_day.strftime('%Y%m%d'),
                                         fields='ts_code,trade_date,adj_factor')
                df_q['trade_date'] = to_datetime(df_q['trade_date'], format='%Y%m%d')
                df_q = df_q.set_index('trade_date')
                if df is not None:
                    df = concat([df, df_q])
                    df = df[~df.index.duplicated(keep='first')]
                else:
                    df = df_q
                self.adj_buffer[code] = df
            except Exception as e:
                traceback.print_exc()
                print(e)
        return df
