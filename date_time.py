# from datetime import datetime
# from pytz import timezone
#
# cst = timezone('US/Central')
# cst_time = datetime.now(cst)
#
# from datetime import datetime, timedelta
#
# day = '2022-05-23 00:00:00'
# dt = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
# start = dt - timedelta(days=dt.weekday())
# end = start + timedelta(days=6,seconds=-1)
# print(start)
# print(end)
#
# from datetime import date, timedelta
# today = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
# start = today - timedelta(days=today.weekday())
# end = start + timedelta(days=7, seconds=-1)
# print("Today: " + str(today))
# print("Start: " + str(start))
# print("End: " + str(end))


# from datetime import date, datetime, timedelta
#
# import re
#
# week_start = datetime.strptime(str(date.today() - timedelta(days=7)), '%Y-%m-%d')
#
# week_end = datetime.strptime(str(date.today() - timedelta(days=1)), '%Y-%m-%d').replace(hour=23, minute=59, second=59)
#
# month_start = datetime.strptime(str(date.today().replace(day=1) - timedelta(days=1)), '%Y-%m-%d')
#
# month_end = datetime.strptime(str(date.today() - timedelta(days=1)), '%Y-%m-%d').replace(hour=23, minute=59, second=59)
#
# day_start = datetime.strptime(str(date.today() - timedelta(days=1)), '%Y-%m-%d').replace(hour=17, minute=00)
#
# day_end = datetime.strptime(str(date.today()), '%Y-%m-%d').replace(hour=16, minute=59, second=59)
#
# print(week_start)
#
# print(week_end)
#
# print(month_start)
#
# print(month_end)
#
# print(day_start)
#
# print(day_end)
#
# customer_log_sql_file = open('/Users/chittaranjanbhat/PycharmProjects/noblrReport/sql/cust_loss_template.sql','r')
#
# customer_log_sql_lines = customer_log_sql_file.read()
#
# # To replace the week start
#
# result = re.sub(r"\d{4}-\d{2}-\d{2}\s*00:00:00", str(week_start), customer_log_sql_lines).su
#
# print(result)

# import datetime
# today = datetime.date.today()
# idx = (today.weekday() + 1) % 6 # MON = 0, SUN = 6 -> SUN = 0 .. SAT = 6
# print(idx)
# sat = today - datetime.timedelta(7+idx-6)
# print(sat)


# from datetime import datetime, timedelta

def prior_week_end():
    return datetime.now() - timedelta(days=((datetime.now().isoweekday() + 4) % 7))


def prior_week_start():
    return prior_week_end() - timedelta(days=6)


# print(prior_week_end())
# print(prior_week_start())

from datetime import date, datetime, timedelta
import datetime as dt
from dateutil import relativedelta
import pytz

now = dt.datetime.now(pytz.timezone('US/Central'))
today = now.date()

# today = today.replace(month=5, day=14, year=2022)

print(today)

day_start = datetime.strptime(str(today - timedelta(days=1)), '%Y-%m-%d').replace(hour=17, minute=00)

day_end = datetime.strptime(str(today), '%Y-%m-%d').replace(hour=16, minute=59, second=59)

start = today - dt.timedelta((today.weekday() + 1) % 7)
week_start = datetime.strptime(str(start + relativedelta.relativedelta(weekday=relativedelta.SA(-1))), '%Y-%m-%d')
week_end = datetime.strptime(str(start + relativedelta.relativedelta(weekday=relativedelta.FR(0))), '%Y-%m-%d').replace(
    hour=23, minute=59, second=59)

month_start = datetime.strptime(str(today.replace(month=today.month - 1, day=1)), '%Y-%m-%d')

month_end = datetime.strptime(str(today.replace(day=1) - timedelta(days=1)), '%Y-%m-%d').replace(
    hour=23, minute=59, second=59)

print(f'day start   : {day_start}')
print(f'day end     : {day_end}')
print(f'\nweek start  : {week_start}')
print(f'week end    : {week_end}')
print(f'\nmonth start : {month_start}')
print(f'month end   : {month_end}')

