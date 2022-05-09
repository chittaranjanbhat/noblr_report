import os
import traceback

from utils import noblr_config
from utils import noblr_notify
from utils import noblr_secrets
import psycopg2
import pandas as pd
import time
import argparse


def main():
    sql_file = f'{HOME}/sql/{args.sql_file}'
    with open(sql_file, 'r') as f:
        sql = f.read()
    sql_start = time.perf_counter()
    df = pd.read_sql(sql, con=conn)
    excel_file = f'{config.fs_xls_path()}/Report.xlsx'
    df.to_excel(excel_file, sheet_name='Noblr report', index=False)
    sql_finish = time.perf_counter()
    noblr_notify.notification(excel_file, config.notify_email(), round(sql_finish - sql_start, 2), len(df.index))


if __name__ == '__main__':
    start = time.perf_counter()

    # initiate the parser
    parser = argparse.ArgumentParser()

    # add long and short argument
    parser.add_argument("--environment", "-e", help="select the environment to execute..!", choices=['dev', 'prod'],
                        default='dev')
    parser.add_argument("--sql_file", "-sql", help="give file name of sql you want to run", default='report.sql')

    # read arguments from the command line
    args = parser.parse_args()

    # Instantiate config class, close the config file as soon as possible
    HOME = os.getcwd()
    configFileName = f'{HOME}/conf/config_{args.environment}.yml'
    configFile = open(configFileName, "r")
    config = noblr_config.NoblrConfig(configFile)
    configFile.close()

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
