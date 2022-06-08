import os
import sys
import traceback

from utils import noblr_config
from utils import noblr_notify
from utils import noblr_secrets
from utils import noblr_format_xlsx
import psycopg2
import pandas as pd
import time
import argparse
from datetime import datetime, timedelta
import datetime as dt
from dateutil import relativedelta
import pytz


def main():
    sql = ''
    if args.report_type == 'cust_loss':
        with open(f'{HOME}/sql/{report_type}_template.sql') as f:
            sql = f.read()
    elif args.report_type == 'payment':
        with open(f'{HOME}/sql/{report_type}_template.sql') as f:
            sql = f.read()
    else:
        print('Report you looking for does not exist, try cust_loss or payment')
    sql = sql.replace('#start_time#', str(start_time)).replace('#end_time#', str(end_time))

    sheet_name = args.report_type
    excel_file_name = f"Noblr_{args.report_type}_{args.schedule_type}_Report_{now.strftime('%m%d%Y')}.xlsx"
    sql_start = time.perf_counter()
    df = pd.read_sql(sql, con=conn)
    excel_file = f'{config.fs_xls_path()}/{excel_file_name}'
    df.to_excel(excel_file, sheet_name=sheet_name, index=False)
    noblr_format_xlsx.design_xlsx(excel_file, sheet_name)
    sql_finish = time.perf_counter()
    notify.notification(excel_file_name, excel_file, round(sql_finish - sql_start, 2), len(df.index))


def get_start_end_time(jtype):

    today = now.date()

    if jtype == 'daily':
        day_start = datetime.strptime(str(today - timedelta(days=1)), '%Y-%m-%d').replace(hour=17, minute=00)
        day_end = datetime.strptime(str(today), '%Y-%m-%d').replace(hour=16, minute=59, second=59)
        return day_start, day_end
    elif jtype == 'weekly':
        deltatime = today - dt.timedelta((today.weekday() + 1) % 7)
        week_start = datetime.strptime(str(deltatime + relativedelta.relativedelta(weekday=relativedelta.SA(-1))),
                                       '%Y-%m-%d')
        week_end = datetime.strptime(str(deltatime + relativedelta.relativedelta(weekday=relativedelta.FR(0))),
                                     '%Y-%m-%d').replace(
            hour=23, minute=59, second=59)
        return week_start, week_end
    elif jtype == 'monthly':
        month_start = datetime.strptime(str(today.replace(month=today.month - 1, day=1)), '%Y-%m-%d')
        month_end = datetime.strptime(str(today.replace(day=1) - timedelta(days=1)), '%Y-%m-%d').replace(
            hour=23, minute=59, second=59)
        return month_start, month_end


if __name__ == '__main__':
    start = time.perf_counter()

    # initiate the parser
    parser = argparse.ArgumentParser()

    # add long and short argument
    parser.add_argument("--environment", "-e", help="select the environment to execute..!", choices=['dev', 'prod'],
                        default='dev')
    parser.add_argument("--report_type", "-r", required=True, choices=['cust_loss', 'payment'],
                        help="pass report type to be genereated..!")
    parser.add_argument("--schedule_type", "-s", required=True, choices=['daily', 'weekly', 'monthly'],
                        help="schedule types available daily, weekly and monthly..!")
    parser.add_argument("--odate", "-o", required=False,
                        help="Enter a date in YYYY-MM-DD format to run for specific day..!")

    # read arguments from the command line
    args = parser.parse_args()

    # set the default date to use
    now = dt.datetime.now(pytz.timezone('US/Central'))
    if args.odate:
        year, month, day = map(int, args.odate.split('-'))
        now = now.replace(month=month, day=day, year=year)

    report_type = args.report_type
    schedule_type = args.schedule_type

    start_time, end_time = get_start_end_time(schedule_type)

    # Instantiate config class, close the config file as soon as possible
    HOME = os.getcwd()
    configFileName = f'{HOME}/conf/config_{args.environment}.yml'
    configFile = open(configFileName, "r")
    config = noblr_config.NoblrConfig(configFile)
    configFile.close()

    # Set email notification details
    notify = noblr_notify.Notify(report_type, schedule_type, config.notify_email(), now, start_time, end_time)

    # Create Noblr secrets connection
    secrets = noblr_secrets.NoblrSecrets(config.get_secret_name(), config.get_secret_region())
    secret = secrets.get_secret()

    # Create postgres connection
    conn = None
    try:
        conn = psycopg2.connect(
            host=config.get_postgres_jdbcUrl(),
            database=config.get_postgres_jdbcDatabase(),
            user=secret[config.get_postgres_user()],
            password=secret[config.get_postgres_pwd()])
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        error_msg = traceback.format_exc()
        print(error_msg)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
        finish = time.perf_counter()
        print(f'Finished in {round(finish - start, 2)} second(s)')
