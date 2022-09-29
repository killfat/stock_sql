from datetime import time, timedelta, datetime
from copy import copy
from datetime_manager import DatetimeManager
from pandas import read_csv, DataFrame, Timedelta, concat, to_datetime, date_range, Series
from sklearn.linear_model import LinearRegression
from numpy import reshape
from tu_data import TuData


class TechnicalIndicators:

    def __init__(self, database, tushare_token):

        """
        Connect to SQL database
        Download all the required data over the specified period for all stocks of all price types,
        and store it as data

        Parameters
        ----------

        """
        self.database = database
        self.date = DatetimeManager(database)
        self.tu_handle = TuData(tushare_token)
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

    def select_ratio(self, start_time, end_time, code):
        parse_date = ['trade_date']
        if code[0] == '6':
            suffix = '.SH'
        else:
            suffix = '.SZ'
        code = code.split('.')[0] + suffix
        # adjust_rate = read_csv('adj.csv', index_col='trade_date', parse_dates=parse_date)
        # adjust_rate['trade_date'] = to_datetime(adjust_rate['trade_date'], format='%Y%m%d')
        if isinstance(start_time, datetime):
            start_time = start_time.date()
        if isinstance(end_time, datetime):
            end_time = end_time.date()
        date_list = [end_time]
        dt = self.date.backward(end_time, 1)
        while dt >= start_time:
            date_list.append(dt)
            last_dt = self.date.backward(dt, 1)
            if last_dt == dt:
                break
            dt = last_dt
        adjust_rate = self.tu_handle.get_adj_factor(code, date_list)
        #adjust_rate = adjust_rate.set_index('trade_date')
        if adjust_rate is None:
            return None
        ratio = adjust_rate['adj_factor'] / max(adjust_rate['adj_factor'])
        return ratio

    def select_share(self, start_time, end_time, code):
        parse_date = ['trade_date']
        if code[0] == '6':
            suffix = '.SH'
        else:
            suffix = '.SZ'
        code = code.split('.')[0] + suffix
        # float_share = read_csv('share.csv', index_col='trade_date', parse_dates=parse_date)
        if isinstance(end_time, datetime):
            end_time = end_time.date()
        if isinstance(start_time, datetime):
            start_time = start_time.date()
        date_list = [end_time]
        dt = self.date.backward(end_time, 1)
        while dt >= start_time:
            date_list.append(dt)
            last_dt = self.date.backward(dt, 1)
            if last_dt == dt:
                break
            dt = last_dt
        float_share = self.tu_handle.get_float_share(code, date_list)
        if float_share is None:
            return None
        return float_share['float_share']

    def select_n_terms_data(self, start_time, end_time, code=None, ratio=None, price_type='*'):
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
        if df.empty:
            print("No data found. Params:", str(start_time), str(end_time), code)
            return df
        if ratio is None:
            ratio = self.select_ratio(start_time, end_time, code)
            if ratio is None:
                return df
            df['trade_date'] = to_datetime(df.index.date)
            df = df.reset_index().merge(ratio, on='trade_date', how='left').set_index('time')
            for col in ['open', 'close', 'high', 'low']:
                if col in df.columns:
                    df[col] *= df['adj_factor']
            df = df.drop(columns=['trade_date', 'adj_factor'])
        else:
            # just ignore ratio
            pass

        return df

    def select_n_time_data(self, start_time, end_time, time0, code=None, ratio=None, price_type='*'):
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
        if df.empty:
            print("No data found. Params:", str(start_time), str(end_time), code)
            return df

        if ratio is None:
            ratio = self.select_ratio(start_time, end_time, code)
            if ratio is None:
                return df
        else:
            # just ignore ratio
            return df
        df['trade_date'] = to_datetime(df.index.date)
        df = df.reset_index().merge(ratio, on='trade_date', how='left').set_index('time')
        for col in ['open', 'close', 'high', 'low']:
            if col in df.columns:
                df[col] *= df['adj_factor']
        df.drop(columns=['trade_date', 'adj_factor'])
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
            data = self.select_n_terms_data(start_time, end_time, ticker, price_type=field)
        return data

    def get_close(self, ticker, start_time, end_time, unit='d', include_today=True):
        buffered = None  # self.buffer.get(ticker, start_time, end_time)
        if buffered is not None:
            data = buffered
        else:
            if unit == 'd':
                data = self.select_n_time_data(start_time, end_time.date(), time(hour=15, minute=0), ticker,
                                               price_type='close')
                if include_today:
                    data_today = self.select_n_terms_data(end_time - timedelta(minutes=4), end_time, ticker,
                                                          price_type='close')
                    if not data_today.empty:
                        data = concat([data, data_today.tail(1)], axis=0)
                if not data.empty:
                    data.index = data.index.normalize()
            elif unit == 'm':
                data = self.select_n_terms_data(start_time, end_time, ticker, price_type='close')
            else:
                print("Invalid unit parameter.")
                return None
        return data['close']

    def get_open(self, ticker, start_time, end_time, include_today=True):
        buffered = None  # self.buffer.get(ticker, start_time, end_time)
        if buffered is not None:
            data = buffered
        else:
            data = self.select_n_time_data(start_time, end_time, time(hour=9, minute=31), ticker, price_type='open')

        data.index = data.index.normalize()
        return data['open']

    def check_available(self, ticker, check_date, days=35):
        start_day = self.date.backward(check_date.date(), days)
        res = self.get_close(ticker, start_day, check_date, include_today=True)
        if len(res) < days:
            return False
        else:
            return True

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
            return 0
        today_time = copy(cur_time).replace(hour=9, minute=30)
        start_time = self.date.backward(today_time.date(), days)
        data = self.get_field_data(ticker, today_time, cur_time, 'volume')
        past_data = self.get_field_data(ticker, start_time, today_time.date(), 'volume')
        if data.empty or past_data.empty:
            print("LB: no data.")
            return None
        else:
            today_avg = data.mean()
            past_avg = past_data.mean()
            return (today_avg / past_avg)['volume']

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
            start_day = self.date.get_previous_trade_time(check_date, timedelta(minutes=period - 1))
        else:
            print("LB: Invalid unit.")
            return None
        res = self.get_close(ticker, start_day, check_date, unit=unit, include_today=True)
        if res.empty:
            print("Not enough close data for LB")
            return None
        res = res.mean()
        return res

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
            if data.empty:
                print("Not enough data for KDJ.")
                return None, None, None
            daily_low = data['low']
            daily_low = daily_low.groupby(to_datetime(daily_low.index.date)).min()
            daily_high = data['high']
            daily_high = daily_high.groupby(to_datetime(daily_high.index.date)).max()
            window = days
            if days > len(daily_low):
                window = len(daily_low)
            low = daily_low.rolling(window=window).min()
            high = daily_high.rolling(window=window).max()
            date_close = self.get_close(ticker, start_day, check_date, include_today=True)
            RSV = (date_close - low) \
                  / (high - low) * 100
            if days > len(daily_low):
                pad_days = 2 * days - len(daily_low)
                dt_range = date_range(start=RSV.index.min() - Timedelta(days=pad_days + 1), end=RSV.index.min() - Timedelta(days=1))
                se_na = Series(index=dt_range)
                RSV = concat([RSV, se_na])
            RSV.fillna(50, inplace=True)
            RSV = RSV.sort_index()
            K = RSV.ewm(alpha=1 / m1, adjust=False, min_periods=days).mean()
            D = K.ewm(alpha=1 / m2, adjust=False, min_periods=days).mean()
            J = 3 * K - 2 * D
            df = concat([K, D, J], axis=1, join='inner')
            #df = df.dropna()
            return (*df.iloc[-1].tolist(), )
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
                if long + mid - 1 > len(close_df):
                    print("Not enough close data for MACD-d")
                    return None, None, None
            elif unit[-1] == 'm':
                start_day = self.date.get_previous_trade_time(check_date,
                                                              timedelta(minutes=int(unit[:-1]) * (long + mid - 1) - 1))
                close_df = self.get_field_data(ticker, start_day, check_date, ['close'])
                if long + mid - 1 > len(close_df):
                    print("Not enough close data for MACD-m")
                    return None, None, None
                close_df = close_df[(int(unit[:-1]) - 1)::int(unit[:-1])].reset_index()['close']
            else:
                print("Invalid unit param.")
                return None, None, None

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

    def BOLL(self, ticker, check_date, days=20, up=2, dwn=2, include_now=True):
        start_day = self.date.backward(check_date.date(), days - 1)
        close = self.get_close(ticker, start_day, check_date)
        if len(close) < 7:
            print("Not enough close data for BOLL")
            return None, None, None
        MB = close.mean()
        UB = MB + up * close.std()
        LB = MB - dwn * close.std()
        return UB, MB, LB

    def HSL(self, ticker, check_date, days=5, unit='d'):
        # 换手线
        if unit == 'd':
            start_day = self.date.backward(check_date.date(), days - 1)
        elif unit[-1] == 'm':
            start_day = self.date.get_previous_trade_time(check_date, timedelta(minutes=days * int(unit[:-1]) - 1))
        else:
            print("Invalid unit param")
            return None, None

        vol_total = self.get_field_data(ticker, start_day, check_date, ['volume'])
        if len(vol_total) < days:
            print("Not enough volume data for HSL")
            return None, None
        shares = self.select_share(start_day, check_date, ticker)
        if shares is None:
            print("No share data available.")
            return None, None
        vol_total['trade_date'] = to_datetime(vol_total.index.date)
        vol_total = vol_total.reset_index().merge(shares, on='trade_date', how='left').set_index('time')
        vol_total = vol_total.drop(columns=['trade_date', 'float_share'])
        if unit == 'd':
            vol_unit = vol_total.groupby(vol_total.index.date).sum()
            vol = vol_unit.tail(1)
        else:
            vol_unit = vol_total
            vol = vol_unit.tail(1)

        if shares is None:
            pass  # get shares
        return vol / shares, (vol_total / shares).mean()

    def MFI(self, ticker, check_date, days=14):
        start_day = self.date.backward(check_date.date(), days - 1)
        close = self.get_close(ticker, start_day, check_date, include_today=True)
        if len(close) < days:
            print("Not enough close data for MFI")
            return None
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
        if len(close) < days:
            print("Not enough close data for RSI")
            return None
        close_delta = close.diff()
        close_delta = close_delta.dropna()
        return 100 - 100 / (1 + close_delta[close_delta > 0].sum() / -close_delta[close_delta < 0].sum())

    def ACCER(self, ticker, check_date, n=8, unit='d'):
        if unit == 'd':
            start_day = self.date.backward(check_date.date(), n - 1)
        elif unit[-1] == 'm':
            start_day = self.date.get_previous_trade_time(check_date, timedelta(minutes=n * int(unit[:-1]) - 1))
        else:
            print("Invalid unit parameter.")
            return None
        close = self.get_close(ticker, start_day, check_date, unit=unit[-1], include_today=True)
        if len(close) < n:
            print("Not enough close data for ACCER.")
            return None
        if unit[-1] == 'm':
            close = close[(int(unit[:-1]) - 1)::int(unit[:-1])].reset_index()['close']
        model = LinearRegression()
        x = range(n)

        model.fit(reshape(x, (-1, 1)), close.tolist())
        k = model.coef_[0]
        return k / close.iloc[-1]

    def BRAR(self, ticker, check_date, days=26):
        start_day = self.date.backward(check_date.date(), days - 1)
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high'])
        if data.empty:
            print("Not enough close data for BRAR")
            return None, None
        high = data['high'].groupby(data.index.date).max()
        low = data['low'].groupby(data.index.date).min()
        close = self.get_close(ticker, start_day, check_date, include_today=True)
        if len(close) < days:
            print("Not enough close data for BRAR")
            return None, None
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
        if len(close) < days:
            print("Not enough close data for PCNT")
            return None, None
        pcnt_df = (close.diff()[1:]) * 100 / close[1:]
        window = days
        if len(pcnt_df) < days:
            window = len(pcnt_df)
        mapcnt = pcnt_df.ewm(span=days, adjust=False, min_periods=window).mean()
        return pcnt_df.tail(1)[0], mapcnt.tail(1)[0]

    def CCI(self, ticker, check_date, n=14, unit='d'):
        if unit == 'd':
            days = 1
            start_day = self.date.backward(check_date.date(), n * days - 1)
        elif unit[-1] == 'm':
            start_day = self.date.get_previous_trade_time(check_date, timedelta(minutes=int(unit[:-1]) * n - 1))
        else:
            print("Invalid unit params for CCI.")
            return None
        close = self.get_close(ticker, start_day, check_date, unit=unit[-1])
        if len(close) < n:
            print("Not enough close data for CCI")
            return None
        data = self.get_field_data(ticker, start_day, check_date, ['low', 'high'])
        if unit == 'd':
            high = data['high'].groupby(data.index.date).max()
            low = data['low'].groupby(data.index.date).min()
        elif unit[-1] == 'm':
            data = data.reset_index()
            high = data['high'].groupby(data.index // int(unit[:-1])).max()
            low = data['high'].groupby(data.index // int(unit[:-1])).min()
            # high = data['high'].groupby(Grouper(freq=unit[0]+'min', closed='left', origin='start')).max().reset_index()['high']
            # low = data['low'].groupby(Grouper(freq=unit[0]+'min', closed='left', origin='start')).min().reset_index()['low']
            close = close[(int(unit[:-1]) - 1)::int(unit[:-1])].reset_index()['close']
        else:
            return None

        typ = (high + close + low) / 3
        ave_dev = typ.mad()
        err = 15 * ave_dev
        return (typ.iloc[-1] - typ.mean()) * 1000 / err


def get_chg(func, code, check_time, t_delta, unit=None):
    param = [code, check_time]
    kwargs = {}
    if unit is not None:
        kwargs['unit'] = unit
    cur = func(*param, **kwargs)
    if cur is None:
        return None
    param[1] -= t_delta
    prev = func(*param, **kwargs)
    if prev is None:
        return None
    if isinstance(cur, tuple):
        if cur[-1] is None:
            return [None] * len(cur)
        return ((a - b) for a, b in zip(cur, prev))
    else:
        return cur - prev


def get_chg_rate(func, code, check_time, t_delta, unit=None):
    param = [code, check_time]
    kwargs = {}
    if unit is not None:
        kwargs['unit'] = unit
    cur = func(*param, **kwargs)
    if cur is None:
        return None
    param[1] -= t_delta
    prev = func(*param, **kwargs)
    if prev is None:
        return None
    if isinstance(cur, tuple):
        if cur[-1] is None:
            return [None] * len(cur)
        return ((a - b) / b for a, b in zip(cur, prev))
    elif prev == 0:
        return None
    else:
        return (cur - prev) / prev
