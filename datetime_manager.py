import pickle


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
