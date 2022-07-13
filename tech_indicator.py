from datetime import time, timedelta
from copy import copy
from datetime_manager import DatetimeManager
from pandas import DataFrame, Timestamp, concat, to_datetime, Grouper
from sklearn.linear_model import LinearRegression
from numpy import reshape

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

    def select_n_time_data(self, start_time, end_time, time0, code=None, price_type='*'):
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
        df = self.database.select_time(start_time, end_time, time0, code, price_type)

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
        buffered = None  # self.buffer.get(ticker, start_time, end_time + timedelta(1))
        if buffered is not None:
            data = buffered
        else:
            data = self.select_n_terms_data(start_time, end_time, ticker, field)
        return data

    def get_close(self, ticker, start_time, end_time, unit='d', include_today=True):
        buffered = None  # self.buffer.get(ticker, start_time, end_time)
        if buffered is not None:
            data = buffered
        else:
            if unit == 'd':
                data = self.select_n_time_data(start_time, end_time.date(), time(hour=15, minute=0), ticker,
                                               'close')
                if include_today:
                    data_today = self.select_n_terms_data(end_time - timedelta(minutes=4), end_time, ticker, 'close')
                    if not data_today.empty:
                        data = concat([data, data_today.tail(1)], axis=0)
                if not data.empty:
                    data.index = data.index.normalize()
            elif unit == 'm':
                data = self.select_n_terms_data(start_time, end_time, ticker, 'close')
            else:
                raise Exception
        return data['close']

    def get_open(self, ticker, start_time, end_time, include_today=True):
        buffered = None  # self.buffer.get(ticker, start_time, end_time)
        if buffered is not None:
            data = buffered
        else:
            data = self.select_n_time_data(start_time, end_time, time(hour=9, minute=31), ticker, 'open')

        data.index = data.index.normalize()
        return data['open']

    def LB(self, ticker, cur_time, days=5):
        """
        计算量比
        :param ticker:
        :param cur_time:
        :param days:
        :return:
        need minute?
        """
        cur_time.replace(second=0)
        if cur_time.hour < 9 or (cur_time.hour == 9 and cur_time.minute <= 30):
            return None
        today_time = copy(cur_time).replace(hour=9, minute=30)
        start_time = self.date.backward(today_time.date(), days)
        if start_time is not None:
            today_avg = self.get_field_data(ticker, today_time, cur_time, 'volume').mean()
            past_avg = self.get_field_data(ticker, start_time, today_time.date(), 'volume').mean()
            return (today_avg / past_avg)['volume']
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
        can be day, minute
        """
        if unit == 'd':
            days = 1
            start_day = self.date.backward(check_date.date(), period * days - 1)
        elif unit == 'm':
            start_day = check_date - timedelta(minutes=period-1)
            if (start_day.hour == 12 or (start_day.hour == 11 and start_day.minute > 30)) \
                    or (start_day.hour == 13 and start_day.minute == 0):
                start_day = start_day - timedelta(hours=1, minutes=30)
            if start_day.hour < 9 or (start_day.hour == 9 and start_day.minute <= 30):
                start_day = start_day - timedelta(hours=18, minutes=30)
        else:
            return None
        if start_day is not None:
            res = self.get_close(ticker, start_day, check_date, unit=unit, include_today=True)
            res = res.mean()
            return res

        else:
            raise Exception

    def KDJ(self, ticker, check_date, days=9, m1=3, m2=3, max_day=30):
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
            start_day = self.date.backward(check_date.date(), max_day - 1)
            data = self.get_field_data(ticker, start_day, check_date, ["low", "high"])
            daily_low = data['low']
            daily_low = daily_low.groupby(to_datetime(daily_low.index.date)).min()
            daily_high = data['high']
            daily_high = daily_high.groupby(to_datetime(daily_high.index.date)).max()
            low = daily_low.rolling(window=9).min()
            high = daily_high.rolling(window=9).max()
            date_close = self.get_close(ticker, start_day, check_date, include_today=True)
            RSV = (date_close - low) \
                  / (high - low) * 100
            RSV.fillna(50, inplace=True)
            K = RSV.ewm(alpha=1 / m1, adjust=False, min_periods=days).mean()
            D = K.ewm(alpha=1 / m2, adjust=False, min_periods=days).mean()
            J = 3 * K - 2 * D
            df = DataFrame(date_close)
            df['K'] = K
            df['D'] = D
            df['J'] = J
            df = df.dropna()
            return df.iloc[-1]['K'], df.iloc[-1]['D'], df.iloc[-1]['J']
        else:
            start = self.date.backward(check_date.date(), days)
            low = self.get_field_data(ticker, start, check_date, ['low', 'high']).min()
            high = self.get_field_data(ticker, start, check_date, 'high').max()
            close = self.get_close(ticker, start, check_date, include_today=True)
            RSV = (close - low) / (high - low) * 100
            K = (self.buffer.get('K') * (m1 - 1) + RSV) / m1
            D = (self.buffer.get('D') * (m2 - 1) + K) / m2
            J = 3 * K - 2 * D
            return K, D, J

    def MACD(self, ticker, check_date, short=12, long=26, mid=9, unit='d', include_now=True):
        if self.buffer.get('macd') is None:
            if unit == 'd':
                start_day = self.date.backward(check_date.date(), long + mid - 1)
                close_df = self.get_close(ticker, start_day, check_date, include_today=include_now)
            elif unit[-1] == 'm':
                start_day = check_date - timedelta(minutes=int(unit[0]) * (long + mid - 1) - 1)
                if (start_day.hour == 12 or (start_day.hour == 11 and start_day.minute > 30)) \
                        or (start_day.hour == 13 and start_day.minute == 0):
                    start_day = start_day - timedelta(hours=1, minutes=30)
                if start_day.hour < 9 or (start_day.hour == 9 and start_day.minute <= 30):
                    start_day = start_day - timedelta(hours=18, minutes=30)
                if (start_day.hour == 12 or (start_day.hour == 11 and start_day.minute > 30)) \
                        or (start_day.hour == 13 and start_day.minute == 0):
                    start_day = start_day - timedelta(hours=1, minutes=30)
                close_df = self.get_field_data(ticker, start_day, check_date, ['close'])
                close_df = close_df[(int(unit[0]) - 1)::int(unit[0])].reset_index()['close']
            else:
                return None
            EMA1 = close_df.ewm(span=short, adjust=False, min_periods=short).mean()
            EMA2 = close_df.ewm(span=long, adjust=False, min_periods=long).mean()

            DIF = EMA1 - EMA2
            DEA = DIF.ewm(span=mid, adjust=False, min_periods=mid).mean()
            DIF_DEA = 2 * (DIF - DEA)
            DIF = DIF.to_frame()
            DIF['DEA'] = DEA
            DIF['DIF_DEA'] = DIF_DEA
            DIF = DIF.dropna()
            return DIF.iloc[-1]['close'], DIF.iloc[-1]['DEA'], DIF.iloc[-1]['DIF_DEA']

        else:
            close = self.get_close(ticker, check_date, check_date + 1)
            EMA1 = ((short - 1) * self.buffer.get('ema1') + close * 2) / (short + 1)
            EMA2 = ((long - 1) * self.buffer.get('ema2') + close * 2) / (long + 1)
            DIF = EMA1 - EMA2
            DEA = (DIF * 2 + self.buffer.get('dif') * (mid - 1)) / (mid + 1)
            return DIF, DEA, (DIF - DEA) * 2

    def BOLL(self, ticker, check_date, days, up=2, dwn=2, include_now=True):
        start_day = self.date.backward(check_date, days - 1)
        close = self.get_close(ticker, start_day, check_date)
        MB = close.mean()
        UB = MB + up * close.std()
        LB = MB - dwn * close.std()
        return MB, UB, LB

    def HSL(self, ticker, check_date, total, days=5):
        # 换手线
        start_day = self.date.backward(check_date, days)
        vol = self.get_field_data(ticker, start_day, check_date, ['volume'])
        return 10000 * vol.sum() / total

    def MFI(self, ticker, check_date, days=14):
        start_day = self.date.backward(check_date.date(), days - 1)
        close = self.get_close(ticker, start_day, check_date, include_today=True)
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high', 'volume'])
        low = data['low'].groupby(data.index.date).min()
        high = data['high'].groupby(data.index.date).max()
        vol = data['volume'].groupby(data.index.date).sum()
        typ = (high + close + low) / 3
        positive_flow = ((typ.diff() > 0)[1:] * typ[1:] * vol[1:]).sum()
        negative_flow = ((typ.diff() < 0)[1:] * typ[1:] * vol[1:]).sum()
        MFI = 100 - (100 / (1 + positive_flow / negative_flow))
        return MFI

    def RSI(self, ticker, check_date, days=14, unit='d'):
        start_day = self.date.backward(check_date.date(), days)
        close = self.get_close(ticker, start_day, check_date, unit=unit, include_today=True)
        close_delta = close.diff()
        close_delta = close_delta.dropna()
        return 100 - 100 / (1 + close_delta[close_delta > 0].sum() / -close_delta[close_delta < 0].sum())

    def ACCER(self, ticker, check_date, n=8, unit='d'):
        if unit == 'd':
            start_day = self.date.backward(check_date.date(), n - 1)
        elif unit == 'm':
            start_day = check_date - timedelta(minutes=n-1)
            if (start_day.hour == 12 or (start_day.hour == 11 and start_day.minute > 30)) \
                    or (start_day.hour == 13 and start_day.minute == 0):
                start_day = start_day - timedelta(hours=1, minutes=30)
            if start_day.hour < 9 or (start_day.hour == 9 and start_day.minute <= 30):
                start_day = start_day - timedelta(hours=18, minutes=30)
        else:
            return None
        close = self.get_close(ticker, start_day, check_date, unit=unit, include_today=True)
        model = LinearRegression()
        x = range(n)

        model.fit(reshape(x, (-1, 1)), close.tolist())
        k = model.coef_[0]
        return k / close.iloc[-1]

    def BRAR(self, ticker, check_date, days=26):
        start_day = self.date.backward(check_date.date(), days - 1)
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high'])
        high = data['high'].groupby(data.index.date).max()
        low = data['low'].groupby(data.index.date).min()
        close = self.get_close(ticker, start_day, check_date, include_today=True)
        opn = self.get_open(ticker, start_day, check_date)
        last_close = close.shift()
        x = high - last_close[1:]
        y = last_close - low
        BR = x[x > 0].sum() / y[y > 0].sum() * 100
        AR = (high - opn).sum() / (opn - low).sum() * 100
        return BR, AR

    def PCNT(self, ticker, check_date, days=5):
        start_day = self.date.backward(check_date.date(), days)
        close = self.get_close(ticker, start_day, check_date, include_today=True)
        pcnt_df = (close.diff()[1:]) * 100 / close[1:]
        mapcnt = pcnt_df.ewm(span=days, adjust=False, min_periods=days).mean()
        return pcnt_df.tail(1)[0], mapcnt.tail(1)[0]

    def CCI(self, ticker, check_date, n=14, unit='d'):
        if unit == 'd':
            days = 1
            start_day = self.date.backward(check_date.date(), n * days - 1)
        elif unit[-1] == 'm':
            start_day = check_date - timedelta(minutes=int(unit[0]) * n-1)
            if (start_day.hour == 12 or (start_day.hour == 11 and start_day.minute > 30)) \
                    or (start_day.hour == 13 and start_day.minute == 0):
                start_day = start_day - timedelta(hours=1, minutes=30)
            if start_day.hour < 9 or (start_day.hour == 9 and start_day.minute <= 30):
                start_day = start_day - timedelta(hours=18, minutes=30)
        else:
            return None
        close = self.get_close(ticker, start_day, check_date, unit=unit[-1])
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high'])
        if unit == 'd':
            high = data['high'].groupby(data.index.date).max()
            low = data['low'].groupby(data.index.date).min()
        elif unit[-1] == 'm':
            data = data.reset_index()
            high = data['high'].groupby(data.index // int(unit[0])).max()
            low = data['high'].groupby(data.index // int(unit[0])).min()
            #high = data['high'].groupby(Grouper(freq=unit[0]+'min', closed='left', origin='start')).max().reset_index()['high']
            #low = data['low'].groupby(Grouper(freq=unit[0]+'min', closed='left', origin='start')).min().reset_index()['low']
            close = close[(int(unit[0])-1)::int(unit[0])].reset_index()['close']
        else:
            return None

        typ = (high + close + low) / 3
        ave_dev = typ.mad()
        err = 15 * ave_dev
        return (typ.iloc[-1] - typ.mean()) * 1000 / err
