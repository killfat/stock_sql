import pickle
from datetime import timedelta

class DatetimeManager:
    def __init__(self, database):
        try:
            f = open('dates.pkl', 'rb')
            self.date_list = pickle.load(f)
            self.date_len = len(self.date_list)
            print("Date Parsing Finished.")
        except:
            print("Getting available dates from database...")
            self.date_list = sorted(database.get_all_date())
            self.date_len = len(self.date_list)
            f = open('dates.pkl', 'wb')
            pickle.dump(self.date_list, f)
            print("Date Parsing Finished.")

    def get_first_day(self):
        return self.date_list[0]

    def get_last_day(self):
        return self.date_list[-1]

    def get_previous_trade_time(self, cur_time, t_delta):
        previous_time = cur_time - t_delta
        if (((previous_time.day < cur_time.day) or
                cur_time.hour >= 13) and (previous_time.hour < 13 or (previous_time.hour == 13 and previous_time.minute == 0))):
            previous_time = previous_time - timedelta(hours=1, minutes=30)
        if previous_time.hour < 9 or (previous_time.hour == 9 and previous_time.minute <= 30):
            start_date = self.backward(cur_time.date(), 1)
            previous_time = previous_time - timedelta(hours=18, minutes=30)
            previous_time = previous_time.replace(start_date.year, start_date.month, start_date.day)
        if (previous_time.hour == 12 or (previous_time.hour == 11 and previous_time.minute > 30)) \
                or (previous_time.hour == 13 and previous_time.minute == 0):
            previous_time = previous_time - timedelta(hours=1, minutes=30)
        return previous_time

    def add_new_day(self, day):
        self.date_list.append(day)

    def find(self, today):
        _min = 0
        _max = self.date_len - 1
        while _min <= _max:
            mid = (_min + _max) // 2
            if today == self.date_list[mid]:
                return mid
            elif today < self.date_list[mid]:
                _max = mid - 1
            else:
                _min = mid + 1
        return _max

    def backward(self, today, days):
        # only return start day
        idx = self.find(today)
        if idx < days:
            print("back %s days not available" % days)
            return None

        return self.date_list[idx - days]

    def backward_list(self, today, days):
        # return len-days list, containing available dates
        idx = self.find(today)
        if idx < days:
            print("back %s days not available" % days)
            return None
        return self.date_list[idx - days:idx]

    # def forward(self, today, days):
    #     idx = self.find(today)
    #     if idx + days >= self.date_len:
    #         print("next %s days not available")
    #         return None
    #
    #     return self.date_list[idx + days]
