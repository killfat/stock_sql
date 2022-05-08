from datetime import time, timedelta
from copy import copy
from datetime_manager import DatetimeManager
from pandas import DataFrame, Timestamp

class TechnicalIndicators:

    def __init__(self, database):

        """
        Connect to SQL database
        Download all the required data over the specified period for all stocks of all price types,
        and store it as data

        Parameters
        ----------

        """
        self.database = database
        self.date = DatetimeManager(database)
        self.buffer = {}

    # def end_time(self, start_time, terms_amount, term_unit):
    #
    #     """
    #     Calculate the end time based on the given start time and terms and unit of terms,
    #     for example, end_time = start_time + 10 days
    #
    #     Parameters
    #     ----------
    #     start_time : Datetime(year,month,day,hour,minute)
    #         select data where time > start_time.
    #
    #     terms_amount : int
    #         the maximum amount of terms we need to calculate the technical indicators of a day.
    #
    #     Raises
    #     ------
    #     ValueError
    #         unit of terms must be either d or day or m or minute.
    #
    #     Returns
    #     -------
    #     end_time : Datetime
    #
    #     """
    #     if term_unit.lower() == 'd' or term_unit.lower() == 'day':
    #         end_time = start_time + timedelta(days=terms_amount)
    #
    #     elif term_unit.lower() == 'm' or term_unit.lower() == 'minute':
    #         end_time = start_time + timedelta(minutes=terms_amount)
    #
    #     else:
    #         raise ValueError("Unknown term unit!!! Has to be either day or minute")
    #
    #     return end_time

    def select_n_terms_data(self, start_time, end_time, code=None, price_type='*'):
        """
        Use the function from previous class to download the required data from database

        Parameters
        ----------
        end_time : same

        code : string, optional
            stock code. The default is None.

        price_type : list, optional
            required price type - high, low, close, open, volume, money. The default is '*'.

        Returns
        -------
        df : dataframe

        """

        # store all data for all stocks over the period
        # this could make the class only call database once each day and save some time from connecting database
        # all the indicator calculation should based on this variable
        df = self.database.select_data(start_time, end_time, code, price_type)

        return df

    def select_n_time_data(self, start_time, end_time, time, code=None, price_type='*'):
        """
        Use the function from previous class to download the required data from database

        Parameters
        ----------
        end_time : same

        code : string, optional
            stock code. The default is None.

        price_type : list, optional
            required price type - high, low, close, open, volume, money. The default is '*'.

        Returns
        -------
        df : dataframe

        """

        # store all data for all stocks over the period
        # this could make the class only call database once each day and save some time from connecting database
        # all the indicator calculation should based on this variable
        df = self.database.select_time(start_time, end_time, time, code, price_type)

        return df


    # def n_terms_average(self, terms_amount, term_unit, code):
    #     """
    #     average over the specified period of the stocks in code
    #
    #     Parameters
    #     ----------
    #     Same as before
    #
    #     Returns
    #     -------
    #     average : dataframe
    #         A dataframe that index is the code list the value is average value of each price type.
    #
    #     """
    #     start_time = self.start_time
    #
    #     end_time = self.end_time(start_time, terms_amount, term_unit)
    #     if code[0] != '\'':
    #         code = '\'' + code + '\''
    #     data = self.select_n_terms_data(start_time, terms_amount, term_unit, code)
    #
    #     data = data[(data['time'] >= start_time) & (data['time'] < end_time)]
    #
    #     average = data.mean(numeric_only=True)
    #
    #     return average

    # def n_terms_min(self, terms_amount, term_unit, code):
    #     """
    #     min of the specified period of the stocks in code
    #
    #     Parameters
    #     ----------
    #     Same as before
    #
    #     Returns
    #     -------
    #     average : dataframe
    #         A dataframe that index is the code list the value is min value of each price type.
    #
    #     """
    #     start_time = self.start_time
    #
    #     end_time = self.end_time(start_time, terms_amount, term_unit)
    #     if code[0] != '\'':
    #         code = '\'' + code + '\''
    #     data = self.select_n_terms_data(start_time, terms_amount, term_unit, code)
    #     data = data[(data['time'] >= start_time) & (data['time'] < end_time)]
    #
    #     min_ = data.min()
    #
    #     return min_
    #
    # def n_terms_max(self, terms_amount, term_unit, code):
    #     """
    #     average over the specified period of the stocks in code_list
    #
    #     Parameters
    #     ----------
    #     Same as before
    #
    #     Returns
    #     -------
    #     average : dataframe
    #         A dataframe that index is the code list the value is min value of each price type.
    #
    #     """
    #     start_time = self.start_time
    #
    #     end_time = self.end_time(start_time, terms_amount, term_unit)
    #     if code[0] != '\'':
    #         code = '\'' + code + '\''
    #     data = self.select_n_terms_data(start_time, terms_amount, term_unit, code)
    #     data = data[(data['time'] >= start_time) & (data['time'] < end_time)]
    #
    #     max_ = data.max()
    #
    #     return max_

    def get_field_data(self, ticker, start_time, end_time, field):
        # Calculate average of 'field'
        # need timedelta(+1) to get today's data
        buffered = None#self.buffer.get(ticker, start_time, end_time + timedelta(1))
        if buffered is not None:
            data = buffered
        else:
            data = self.select_n_terms_data(start_time, end_time, ticker, field)
        return data[field]

    def get_close(self, ticker, start_time, end_time):
        buffered = None#self.buffer.get(ticker, start_time, end_time)
        if buffered is not None:
            data = buffered
        else:
            data = self.select_n_time_data(start_time, end_time, time(hour=15, minute=0), ticker, 'close')
        return data

    def LB(self, ticker, cur_time, days=5):
        """
        计算量比
        :param ticker:
        :param cur_time:
        :param days:
        :return:
        """
        cur_time.replace(second=0)
        if cur_time.hour < 9 or (cur_time.hour == 9 and cur_time.minute <= 30):
            return None
        today_time = copy(cur_time).replace(hour=9, minute=30)
        start_time = self.date.backward(today_time.date(), days)
        if start_time is not None:
            today_avg = self.get_field_data(ticker, today_time, cur_time, 'volume').mean()
            past_avg = self.get_field_data(ticker, start_time, today_time.date(), 'volume').mean()
            return today_avg / past_avg
        else:
            raise Exception

    def MA(self, ticker, check_date, period=5, unit='d'):
        """
        计算均线
        使用收盘价
        :param ticker:
        :param check_date:
        :param period:
        :param unit:
        :return:
        """
        if unit == 'd':
            days = 1
        elif unit == 'w':
            days = 7
        else:
            return None
        start_day = self.date.backward(check_date.date(), period * days)
        if start_day is not None:
            res = self.get_close(ticker, start_day, check_date)['close'].mean()
            return res
        else:
            raise Exception

    def KDJ(self, ticker, check_date, days=9, m1=3, m2=3, unit='d'):
        """

        :param ticker:
        :param check_date:
        :param days:
        :param m1:
        :param m2:
        :param unit:
        :return:
        """
        if self.buffer.get('kdj') is None:
            start_day = self.date.get_first_day()
            daily_low = self.get_field_data(ticker, start_day, check_date, ["time","low"])
            daily_low = daily_low.groupby(daily_low['time'].dt.date).min()["low"].reset_index()
            daily_high = self.get_field_data(ticker, start_day, check_date, ["time","high"])
            daily_high = daily_high.groupby(daily_high['time'].dt.date).max()["high"].reset_index()
            low = daily_low.rolling(window=9).min()
            high = daily_high.rolling(window=9).max()

            date_df = DataFrame(self.date.date_list, columns=['Dates'])
            date_df['close'] = self.get_close(ticker, start_day, check_date)['close']
            RSV = (date_df['close'] - low['low']) \
                        / (high['high'] - low['low']) * 100
            RSV.fillna(50, inplace=True)
            K = RSV.ewm(alpha=1 / m1, adjust=False, min_periods=days).mean()
            D = K.ewm(alpha=1 / m2, adjust=False, min_periods=days).mean()
            J = 3 * K - 2 * D
            date_df['RSV'] = RSV
            date_df['K'] = K
            date_df['D'] = D
            date_df['J'] = J
            date_df = date_df.dropna()
            return date_df.iloc[-1]['K'], date_df.iloc[-1]['D'], date_df.iloc[-1]['J']
        else:
            start = self.date.backward(check_date.date(), days)
            low = self.get_field_data(ticker, start, check_date, 'low').min()
            high = self.get_field_data(ticker, start, check_date, 'high').max()
            close = self.get_close(ticker, start, check_date)
            RSV = (close - low) / (high - low) * 100
            K = (self.buffer.get('K') * (m1 - 1) + RSV) / m1
            D = (self.buffer.get('D') * (m2 - 1) + K) / m2
            J = 3*K - 2*D
            return K, D, J

    def MACD(self, ticker, check_date, short=12, long=26, mid=9, include_now=True):
        if self.buffer.get('macd') is None:
            start_ema = self.date.get_first_day()
            close_df = self.get_close(ticker, start_ema, check_date)
            EMA1 = close_df['close'].ewm(span=short, adjust=False, min_periods=short).mean()
            EMA2 = close_df['close'].ewm(span=long, adjust=False, min_periods=long).mean()

            DIF = EMA1 - EMA2
            DEA = DIF.ewm(span=mid, adjust=False, min_periods=mid).mean()
            DIF_DEA = 2 * (DIF - DEA)
            date_df = DataFrame(self.date.date_list, columns=['DATES'])
            date_df['DIF'] = DIF
            date_df['DEA'] = DEA
            date_df['DIF_DEA'] = DIF_DEA
            date_df = date_df.dropna()
            return date_df.iloc[-1]['DIF'], date_df.iloc[-1]['DEA'], date_df.iloc[-1]['DIF_DEA']

        else:
            close = self.get_close(ticker, check_date, check_date + 1)
            EMA1 = ((short - 1) * self.buffer.get('ema1') + close * 2) / (short + 1)
            EMA2 = ((long - 1) * self.buffer.get('ema2') + close * 2) / (long + 1)
            DIF = EMA1 - EMA2
            DEA = (DIF * 2 + self.buffer.get('dif') * (mid - 1)) / (mid + 1)
            return DIF, DEA, (DIF-DEA) * 2

    def BOLL(self, ticker, check_date, days, up=2, dwn=2, include_now=True):
        start_day = self.date.backward(check_date, days)
        close = self.get_close(ticker, start_day, check_date)['close']
        MB = close.mean()
        UB = MB + up * close.std()
        LB = MB - dwn * close.std()
        return MB, UB, LB

    def HSL(self, ticker, check_date, total, days=5):
        # 换手线
        start_day = self.date.backward(check_date, days)
        vol = self.get_field_data(ticker, start_day, check_date, ['volume'])
        return 10000 * vol.sum() / total

    def MFI(self, ticker, check_date, days):
        start_day = self.date.backward(check_date, days)
        close = self.get_close(ticker, check_date, check_date + 1)['close'].values
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high', 'volume'])
        grouped_data = data.groupby(data['time'].dt.date)
        low = grouped_data['low'].min()
        high = grouped_data['high'].max()
        vol = grouped_data['volume'].sum()
        typ = (high + close + low) / 3
        positive_flow = (typ > typ.rolling(window=1)) * typ * vol
        negative_flow = (typ < typ.rolling(window=1)) * typ * vol
        MFI = 100 - (100 / (1 + positive_flow / negative_flow))
        return MFI

    def RSI(self):
        pass

    def BRAR(self):
        pass

    def CCI(self):
        pass
